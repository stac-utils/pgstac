#![cfg(feature = "pool")]
//! Gate for `PgstacPool::load_queryables` against a clone of the ingest template: `property_wrapper`
//! mapping, `index_fields` → `property_index_type`, core-field skipping, `delete_missing`, and the
//! no-`properties` error. (Ports the pypgstac `test_queryables` cases.)

use pgstac::{ConnectConfig, PgstacPool};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio_postgres::NoTls;

fn base() -> String {
    std::env::var("PGSTAC_RS_TEST_BASE")
        .unwrap_or_else(|_| "postgresql://username:password@localhost:5439".to_string())
}

fn template() -> String {
    std::env::var("PGSTAC_RS_INGEST_TEMPLATE").unwrap_or_else(|_| "pgstac_rs_ingest_template".to_string())
}

/// A disposable database cloned from the ingest template, dropped on `Drop`.
struct CloneDb {
    name: String,
}

impl CloneDb {
    async fn create() -> CloneDb {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let name = format!(
            "pgstac_rs_queryables_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let (client, connection) = tokio_postgres::connect(&format!("{}/postgres", base()), NoTls)
            .await
            .unwrap();
        let handle = tokio::spawn(connection);
        let _ = client
            .execute(&format!("CREATE DATABASE {name} TEMPLATE {}", template()), &[])
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

/// A queryables schema doc: four core fields (skipped) + five `test:*` properties covering each wrapper.
fn queryables_doc() -> Value {
    json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "collection": {"type": "string"},
            "datetime": {"type": "string", "format": "date-time"},
            "geometry": {"type": "object"},
            "test:string_prop": {"type": "string"},
            "test:number_prop": {"type": "number"},
            "test:integer_prop": {"type": "integer"},
            "test:datetime_prop": {"type": "string", "format": "date-time"},
            "test:array_prop": {"type": "array", "items": {"type": "string"}}
        }
    })
}

/// `name -> (property_wrapper, property_index_type)` for the loaded `test:*` queryables.
async fn loaded(db: &CloneDb) -> HashMap<String, (String, Option<String>)> {
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    let handle = tokio::spawn(connection);
    let rows = client
        .query(
            "SELECT name, property_wrapper, property_index_type FROM pgstac.queryables \
             WHERE name LIKE 'test:%' ORDER BY name",
            &[],
        )
        .await
        .unwrap();
    handle.abort();
    rows.into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                (row.get::<_, String>(1), row.get::<_, Option<String>>(2)),
            )
        })
        .collect()
}

fn all_index_fields() -> Vec<String> {
    ["test:string_prop", "test:number_prop", "test:integer_prop", "test:datetime_prop", "test:array_prop"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

#[tokio::test]
async fn load_queryables_maps_wrappers_and_indexes() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    let n = pool
        .load_queryables(queryables_doc(), None, false, Some(all_index_fields()))
        .await
        .unwrap();
    assert_eq!(n, 5, "5 non-core queryables loaded (core fields skipped)");
    let q = loaded(&db).await;
    assert_eq!(q.len(), 5);
    assert_eq!(q["test:string_prop"], ("to_text".into(), Some("BTREE".into())));
    assert_eq!(q["test:number_prop"], ("to_float".into(), Some("BTREE".into())));
    assert_eq!(q["test:integer_prop"], ("to_int".into(), Some("BTREE".into())));
    assert_eq!(q["test:datetime_prop"], ("to_tstz".into(), Some("BTREE".into())));
    assert_eq!(q["test:array_prop"], ("to_text_array".into(), Some("BTREE".into())));
}

#[tokio::test]
async fn load_queryables_without_index_fields_has_no_index() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    let n = pool.load_queryables(queryables_doc(), None, false, None).await.unwrap();
    assert_eq!(n, 5);
    let q = loaded(&db).await;
    assert_eq!(q["test:number_prop"].0, "to_float");
    for (name, (_wrapper, index)) in &q {
        assert!(index.is_none(), "{name} should have no index without index_fields");
    }
}

#[tokio::test]
async fn load_queryables_specific_index_fields() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    let indexed = vec!["test:string_prop".to_string(), "test:datetime_prop".to_string()];
    pool.load_queryables(queryables_doc(), None, false, Some(indexed))
        .await
        .unwrap();
    let q = loaded(&db).await;
    assert_eq!(q["test:string_prop"].1, Some("BTREE".into()));
    assert_eq!(q["test:datetime_prop"].1, Some("BTREE".into()));
    assert!(q["test:number_prop"].1.is_none());
    assert!(q["test:integer_prop"].1.is_none());
    assert!(q["test:array_prop"].1.is_none());
}

#[tokio::test]
async fn load_queryables_delete_missing_removes_absent() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.load_queryables(queryables_doc(), None, false, None).await.unwrap();
    let partial = json!({"properties": {
        "test:string_prop": {"type": "string"},
        "test:number_prop": {"type": "number"}
    }});
    pool.load_queryables(partial, None, true, None).await.unwrap();
    let q = loaded(&db).await;
    assert_eq!(q.len(), 2, "delete_missing removed the 3 absent test:* queryables");
    assert!(q.contains_key("test:string_prop"));
    assert!(q.contains_key("test:number_prop"));
}

#[tokio::test]
async fn load_queryables_no_properties_errors() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    let result = pool.load_queryables(json!({"type": "object"}), None, false, None).await;
    assert!(result.is_err(), "a document with no `properties` must error");
}

/// The `collection_ids` scope of every queryables row for `name` (NULL = shared), ordered.
async fn scopes_for(db: &CloneDb, name: &str) -> Vec<Option<Vec<String>>> {
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    let handle = tokio::spawn(connection);
    let rows = client
        .query(
            "SELECT collection_ids FROM pgstac.queryables WHERE name = $1 \
             ORDER BY collection_ids NULLS FIRST",
            &[&name],
        )
        .await
        .unwrap();
    handle.abort();
    rows.into_iter().map(|r| r.get::<_, Option<Vec<String>>>(0)).collect()
}

/// A collection-scoped load replaces the shared (NULL-scope) row of the same name: the shared row must be
/// removed first, or the name-uniqueness (overlapping-scope) trigger would reject the scoped insert. Guards
/// the DELETE-shared path (untested by the None-scope cases above).
#[tokio::test]
async fn load_queryables_scoped_replaces_shared() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.load_queryables(queryables_doc(), None, false, None).await.unwrap();
    assert_eq!(scopes_for(&db, "test:string_prop").await, vec![None], "starts shared");

    // A scoped queryable requires the collection to exist — the constraint trigger validates collection_ids.
    pool.create_collection(&json!({
        "id": "c1", "type": "Collection", "stac_version": "1.0.0", "description": "t",
        "license": "proprietary",
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[null, null]]}},
        "links": []
    }))
    .await
    .unwrap();
    pool.load_queryables(queryables_doc(), Some(vec!["c1".to_string()]), false, None)
        .await
        .unwrap();
    assert_eq!(
        scopes_for(&db, "test:string_prop").await,
        vec![Some(vec!["c1".to_string()])],
        "the scoped load replaced the shared row (and did not trip the overlap trigger)"
    );
}
