#![cfg(feature = "pool")]
//! Gate for `PgstacPool::load_extensions` against a clone of the ingest template: step-1 URL collection
//! from collections' `stac_extensions` (with `#fragment` stripping + de-duplication), step-2 population of
//! `content` from a local file (bare path and `file://` URL), and the skip-with-warning path for an
//! unreachable URL. (Ports pypgstac's `loadextensions`; http + local fetch is a superset of pypgstac's
//! local-only loader — the http path is exercised by the CLI, not asserted here to keep the tests offline.)

use pgstac::{ConnectConfig, PgstacPool};
use serde_json::{Value, json};
use std::path::PathBuf;
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
            "pgstac_rs_extensions_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let (client, connection) = tokio_postgres::connect(&format!("{}/postgres", base()), NoTls)
            .await
            .unwrap();
        let handle = tokio::spawn(connection);
        let _ = client
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
}

impl Drop for CloneDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        std::thread::scope(|scope| {
            let _ = scope.spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let (client, connection) =
                        tokio_postgres::connect(&format!("{}/postgres", base()), NoTls)
                            .await
                            .unwrap();
                    let handle = tokio::spawn(connection);
                    let _ = client
                        .execute(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                            &[&name],
                        )
                        .await;
                    let _ = client
                        .execute(&format!("DROP DATABASE IF EXISTS {name}"), &[])
                        .await;
                    handle.abort();
                });
            });
        });
    }
}

async fn pool(db: &CloneDb) -> PgstacPool {
    PgstacPool::connect(ConnectConfig {
        dsn: Some(db.dsn()),
        ..Default::default()
    })
    .await
    .unwrap()
}

/// A short-lived raw connection to the clone (for setup + assertions); the caller aborts the returned
/// connection handle when done. Raw connections have no `pgstac` search_path, so callers schema-qualify.
async fn raw(
    db: &CloneDb,
) -> (
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
) {
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    let handle = tokio::spawn(connection);
    (client, handle)
}

/// Inserts a `stac_extensions` row with the given URL and NULL content.
async fn insert_extension_url(db: &CloneDb, url: &str) {
    let (client, handle) = raw(db).await;
    let _ = client
        .execute(
            "INSERT INTO pgstac.stac_extensions (url) VALUES ($1)",
            &[&url],
        )
        .await
        .unwrap();
    handle.abort();
}

/// The stored `content` (jsonb) for a `stac_extensions` URL; `None` when NULL.
async fn content_of(db: &CloneDb, url: &str) -> Option<Value> {
    let (client, handle) = raw(db).await;
    let row = client
        .query_one(
            "SELECT content FROM pgstac.stac_extensions WHERE url = $1",
            &[&url],
        )
        .await
        .unwrap();
    let content = row.get::<_, Option<Value>>(0);
    handle.abort();
    content
}

/// The `stac_extensions` URLs matching a `LIKE` pattern, ordered.
async fn ext_urls_like(db: &CloneDb, pattern: &str) -> Vec<String> {
    let (client, handle) = raw(db).await;
    let rows = client
        .query(
            "SELECT url FROM pgstac.stac_extensions WHERE url LIKE $1 ORDER BY url",
            &[&pattern],
        )
        .await
        .unwrap();
    handle.abort();
    rows.into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

/// A JSON file written under the temp dir, removed on drop.
struct TempJson {
    path: PathBuf,
}

impl TempJson {
    fn new(name: &str, value: &Value) -> TempJson {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "pgstac_rs_ext_{}_{}_{name}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        TempJson { path }
    }

    fn path_str(&self) -> String {
        self.path.to_str().unwrap().to_string()
    }
}

impl Drop for TempJson {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Step 1: a collection's `stac_extensions` are collected into `stac_extensions` with any `#fragment`
/// stripped and duplicates collapsed. Bare (nonexistent) local paths keep step 2 offline: each fetch fails
/// and is skipped, so nothing is populated but the distinct URL set is what we assert.
#[tokio::test]
async fn collections_extensions_collected_distinct_and_fragment_stripped() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&json!({
        "id": "c-ext", "type": "Collection", "stac_version": "1.0.0", "description": "t",
        "license": "proprietary",
        "stac_extensions": [
            "/pgstac-rs-ext-test/a.json",
            "/pgstac-rs-ext-test/a.json#/definitions/thing",
            "/pgstac-rs-ext-test/b.json"
        ],
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[null, null]]}},
        "links": []
    }))
    .await
    .unwrap();

    let loaded = pool.load_extensions().await.unwrap();
    assert_eq!(
        loaded, 0,
        "the bare paths do not exist, so no content is populated"
    );

    let urls = ext_urls_like(&db, "/pgstac-rs-ext-test/%").await;
    assert_eq!(
        urls,
        vec![
            "/pgstac-rs-ext-test/a.json".to_string(),
            "/pgstac-rs-ext-test/b.json".to_string()
        ],
        "the #fragment is stripped and the duplicate collapses to one distinct row"
    );
}

/// Step 2: an extension whose content is NULL is fetched from a local file and stored, given either as a
/// bare path or a `file://` URL.
#[tokio::test]
async fn local_file_populates_content() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;

    let bare = TempJson::new("bare.json", &json!({"title": "bare-ext"}));
    let via_url = TempJson::new("url.json", &json!({"title": "file-url-ext"}));
    insert_extension_url(&db, &bare.path_str()).await;
    let file_url = format!("file://{}", via_url.path_str());
    insert_extension_url(&db, &file_url).await;

    let loaded = pool.load_extensions().await.unwrap();
    assert_eq!(loaded, 2, "both local extensions were fetched and stored");
    assert_eq!(
        content_of(&db, &bare.path_str()).await,
        Some(json!({"title": "bare-ext"}))
    );
    assert_eq!(
        content_of(&db, &file_url).await,
        Some(json!({"title": "file-url-ext"}))
    );
}

/// The skip-with-warning path: an unreachable URL is skipped (its content stays NULL) and the command
/// still succeeds, populating the reachable extensions.
#[tokio::test]
async fn bad_url_is_skipped_and_good_still_loads() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;

    let good = TempJson::new("good.json", &json!({"title": "good-ext"}));
    let missing = "/pgstac-rs-ext-test/does-not-exist.json";
    insert_extension_url(&db, &good.path_str()).await;
    insert_extension_url(&db, missing).await;

    let loaded = pool.load_extensions().await.unwrap();
    assert_eq!(loaded, 1, "only the reachable extension is populated");
    assert_eq!(
        content_of(&db, &good.path_str()).await,
        Some(json!({"title": "good-ext"}))
    );
    assert_eq!(
        content_of(&db, missing).await,
        None,
        "the bad URL's content stays NULL and the load still succeeds"
    );
}
