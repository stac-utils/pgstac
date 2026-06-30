#![cfg(feature = "cli")]
//! End-to-end tests for the `pgstac` CLI binary: drive the actual built binary (via
//! `CARGO_BIN_EXE_pgstac`) through load -> maintain -> delete against a fresh clone of the ingest
//! template, asserting the database state over a side connection. Loads route through the Rust loader.

use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio_postgres::NoTls;

fn base() -> String {
    std::env::var("PGSTAC_RS_TEST_BASE")
        .unwrap_or_else(|_| "postgresql://username:password@localhost:5439".to_string())
}

fn template() -> String {
    std::env::var("PGSTAC_RS_INGEST_TEMPLATE")
        .unwrap_or_else(|_| "pgstac_rs_ingest_template".to_string())
}

/// A disposable database cloned from the ingest template, dropped on `Drop`.
struct CloneDb {
    name: String,
}

impl CloneDb {
    async fn create() -> CloneDb {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let name = format!(
            "pgstac_rs_cli_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let (client, connection) = tokio_postgres::connect(&format!("{}/postgres", base()), NoTls)
            .await
            .unwrap();
        let handle = tokio::spawn(connection);
        client
            .execute(
                &format!("CREATE DATABASE {name} TEMPLATE {}", template()),
                &[],
            )
            .await
            .unwrap();
        handle.abort();
        CloneDb { name }
    }

    fn dsn(&self) -> String {
        format!("{}/{}", base(), self.name)
    }

    async fn count_items(&self) -> i64 {
        let (client, connection) = tokio_postgres::connect(&self.dsn(), NoTls).await.unwrap();
        let handle = tokio::spawn(connection);
        client
            .batch_execute("SET search_path TO pgstac, public")
            .await
            .unwrap();
        let n: i64 = client
            .query_one("SELECT count(*) FROM items", &[])
            .await
            .unwrap()
            .get(0);
        handle.abort();
        n
    }
}

impl Drop for CloneDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        std::thread::scope(|s| {
            s.spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let (client, connection) =
                            tokio_postgres::connect(&format!("{}/postgres", base()), NoTls).await.unwrap();
                        let handle = tokio::spawn(connection);
                        let _ = client
                            .execute(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                                &[&name],
                            )
                            .await;
                        let _ = client.execute(&format!("DROP DATABASE IF EXISTS {name} WITH (force)"), &[]).await;
                        handle.abort();
                    });
            });
        });
    }
}

fn pgstac(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pgstac"))
        .args(args)
        .output()
        .expect("run pgstac binary")
}

const COLLECTION: &str = r#"{"id":"clitest","type":"Collection","stac_version":"1.0.0","description":"cli test","license":"proprietary","extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[[null,null]]}},"links":[]}"#;

fn item(id: &str, datetime: &str) -> String {
    format!(
        r#"{{"type":"Feature","stac_version":"1.0.0","id":"{id}","collection":"clitest","geometry":{{"type":"Point","coordinates":[-105.1,40.1]}},"bbox":[-105.1,40.1,-105.1,40.1],"properties":{{"datetime":"{datetime}"}},"assets":{{}},"links":[]}}"#
    )
}

/// load (collection + NDJSON items via the Rust loader) -> maintain (tighten) -> delete item -> delete
/// collection, asserting the database state at each step over a side connection.
#[tokio::test]
async fn cli_load_maintain_delete() {
    let db = CloneDb::create().await;
    let dsn = db.dsn();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("collection.json"), COLLECTION).unwrap();
    std::fs::write(
        dir.path().join("items.ndjson"),
        format!(
            "{}\n{}\n",
            item("i1", "2023-01-05T00:00:00Z"),
            item("i2", "2023-02-05T00:00:00Z")
        ),
    )
    .unwrap();

    let out = pgstac(&["load", "--dsn", &dsn, dir.path().to_str().unwrap()]);
    assert!(
        out.status.success(),
        "load failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(db.count_items().await, 2, "both items loaded via the CLI");

    let out = pgstac(&["maintain", "--dsn", &dsn]);
    assert!(
        out.status.success(),
        "maintain failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let out = pgstac(&[
        "delete",
        "--dsn",
        &dsn,
        "--collection",
        "clitest",
        "--item",
        "i1",
    ]);
    assert!(
        out.status.success(),
        "delete item failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(db.count_items().await, 1, "one item left after deleting i1");

    let out = pgstac(&["delete", "--dsn", &dsn, "--collection", "clitest"]);
    assert!(
        out.status.success(),
        "delete collection failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        db.count_items().await,
        0,
        "collection delete cascaded its items"
    );
}

/// `load --policy error` rejects a duplicate id (the loader's Error conflict policy), and the CLI exits
/// non-zero.
#[tokio::test]
async fn cli_load_error_policy_rejects_duplicate() {
    let db = CloneDb::create().await;
    let dsn = db.dsn();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("collection.json"), COLLECTION).unwrap();
    std::fs::write(
        dir.path().join("items.ndjson"),
        format!("{}\n", item("dup", "2023-01-05T00:00:00Z")),
    )
    .unwrap();

    let out = pgstac(&["load", "--dsn", &dsn, dir.path().to_str().unwrap()]);
    assert!(
        out.status.success(),
        "first load: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Re-load the same id with the error policy (items only) -> must fail.
    let items = dir.path().join("items.ndjson");
    let out = pgstac(&[
        "load",
        "--dsn",
        &dsn,
        "--policy",
        "error",
        items.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "duplicate load with --policy error should fail"
    );
    assert_eq!(db.count_items().await, 1, "duplicate not added");
}
