//! End-to-end gate for the Rust ingest pipeline: `load_items` (dehydrate -> check_partition ->
//! make_binary_staging -> binary COPY -> flush_items_staging_binary) stores items that equal SQL
//! `content_dehydrate`, and a `pgstac_ingest` role can ingest only through the SECURITY DEFINER path
//! (direct item writes are denied).
//!
//! Needs a database with THIS branch's schema (the SECURITY DEFINER functions + revoked direct writes), so it clones a dedicated
//! template `pgstac_rs_ingest_template` (build with:
//! `psql ... -c 'CREATE DATABASE pgstac_rs_ingest_template'; psql ... pgstac_rs_ingest_template -f src/pgstac/pgstac.sql`).
//! Override the template/base via `PGSTAC_RS_INGEST_TEMPLATE` / `PGSTAC_RS_TEST_BASE`.

use pgstac::dehydrate::DehydrateSchema;
use pgstac::ingest::{ConflictPolicy, load_items};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio_postgres::{Client, NoTls};

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
            "pgstac_rs_ingest_test_{}_{}",
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

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
    client
}

fn collection() -> Value {
    json!({
        "id": "c1",
        "type": "Collection",
        "stac_version": "1.0.0",
        "description": "test",
        "license": "proprietary",
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[null, null]]}},
        "links": []
    })
}

fn item(id: &str, datetime: &str) -> Value {
    json!({
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": id,
        "collection": "c1",
        "geometry": {"type": "Point", "coordinates": [-105.1019, 40.1672]},
        "bbox": [-105.1019, 40.1672, -105.1019, 40.1672],
        "properties": {"datetime": datetime, "platform": "landsat-8", "eo:cloud_cover": 12.5},
        "assets": {"thumbnail": {"href": "https://x/t.png", "type": "image/png"}}
    })
}

/// Each stored item must equal content_dehydrate of the same item (ignoring the load-time timestamp).
async fn assert_stored_equals_content_dehydrate(client: &Client, items: &[Value]) {
    for it in items {
        let id = it["id"].as_str().unwrap();
        let coll = it["collection"].as_str().unwrap();
        let matches: bool = client
            .query_one(
                "SELECT (to_jsonb(i) - 'pgstac_updated_at') = (to_jsonb(d) - 'pgstac_updated_at') \
                 FROM items i, content_dehydrate($1::jsonb) d WHERE i.id = $2 AND i.collection = $3",
                &[it, &id, &coll],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            matches,
            "stored item {id} must equal content_dehydrate output"
        );
    }
}

#[tokio::test]
async fn load_items_stores_items_matching_content_dehydrate() {
    let db = CloneDb::create().await;
    let mut client = connect(&db.dsn()).await;
    let schema = DehydrateSchema::load(&client).await.unwrap();

    let _ = client
        .execute("SELECT create_collection($1::jsonb)", &[&collection()])
        .await
        .unwrap();

    let items = vec![
        item("a", "2023-01-07T00:00:00Z"),
        item("b", "2023-02-15T00:00:00Z"),
    ];
    let n = load_items(&mut client, items.clone(), &schema, ConflictPolicy::Ignore)
        .await
        .unwrap();
    assert_eq!(n, 2, "two items flushed");

    let count: i64 = client
        .query_one("SELECT count(*) FROM items WHERE collection = 'c1'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2, "two items stored");

    assert_stored_equals_content_dehydrate(&client, &items).await;
}

#[tokio::test]
async fn load_items_works_as_pgstac_ingest_under_the_wall() {
    let db = CloneDb::create().await;
    let mut client = connect(&db.dsn()).await;
    let schema = DehydrateSchema::load(&client).await.unwrap();
    let _ = client
        .execute("SELECT create_collection($1::jsonb)", &[&collection()])
        .await
        .unwrap();

    // Act as a role inheriting pgstac_ingest: direct item writes must be denied...
    client
        .batch_execute("SET ROLE pgstac_ingest;")
        .await
        .unwrap();
    let direct = client
        .execute(
            "INSERT INTO items (id, collection, geometry, datetime, end_datetime, datetime_is_range, item_hash) \
             VALUES ('x', 'c1', ST_SetSRID(ST_MakePoint(0,0),4326), now(), now(), false, '\\x00')",
            &[],
        )
        .await;
    assert!(
        direct.is_err(),
        "direct INSERT INTO items must be denied for pgstac_ingest"
    );

    // ...but the Rust pipeline (via the SECURITY DEFINER flush) ingests fine.
    let items = vec![item("a", "2023-01-07T00:00:00Z")];
    let n = load_items(&mut client, items.clone(), &schema, ConflictPolicy::Ignore)
        .await
        .unwrap();
    assert_eq!(n, 1, "one item flushed via the SECURITY DEFINER path");

    client.batch_execute("RESET ROLE;").await.unwrap();
    assert_stored_equals_content_dehydrate(&client, &items).await;
}
