#![cfg(feature = "pool")]
//! End-to-end gate for [`PgstacPool`] write methods: create_collection -> create_items -> get_item
//! round-trip, the `Error` conflict policy rejects a duplicate id, and delete_item removes an item.
//! Runs against a clone of this branch's ingest template (see tests/ingest_load.rs for how to build it).

use pgstac::ingest::ConflictPolicy;
use pgstac::{ConnectConfig, PgstacPool};
use serde_json::{Value, json};
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
            "pgstac_rs_pool_ingest_test_{}_{}",
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

async fn pool(db: &CloneDb) -> PgstacPool {
    PgstacPool::connect(ConnectConfig {
        dsn: Some(db.dsn()),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn pool_create_items_then_get_item() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();

    let n = pool
        .create_items(
            vec![
                item("a", "2023-01-07T00:00:00Z"),
                item("b", "2023-02-15T00:00:00Z"),
            ],
            ConflictPolicy::Ignore,
        )
        .await
        .unwrap();
    assert_eq!(n, 2, "two items created");

    let got = pool.get_item("c1", "a").await.unwrap();
    assert_eq!(
        got.as_ref().map(|i| &i["id"]),
        Some(&json!("a")),
        "get_item round-trips the created item"
    );

    pool.close();
}

#[tokio::test]
async fn pool_search_finds_sparse_items_across_a_long_span() {
    // Regression: 3 items years apart in one (NULL-trunc) partition. partition_stats.n is tiny relative
    // to the month span, so partition_bounds prorates every month's count to 0. The keyset search must
    // still return all 3 — bands must cover the whole range, never be dropped by (estimated) count.
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();
    let items = vec![
        item("s2010", "2010-06-01T00:00:00Z"),
        item("s2015", "2015-06-01T00:00:00Z"),
        item("s2023", "2023-06-01T00:00:00Z"),
    ];
    let n = pool
        .create_items(items, ConflictPolicy::Ignore)
        .await
        .unwrap();
    assert_eq!(n, 3, "three items created");

    let page = pool
        .search_collect(&json!({"collections": ["c1"], "limit": 100}), None)
        .await
        .unwrap();
    assert_eq!(
        page.features.len(),
        3,
        "search must return all sparse items, none dropped"
    );
    pool.close();
}

#[tokio::test]
async fn pool_create_item_rejects_duplicate_then_deletes() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();

    assert_eq!(
        pool.create_item(item("a", "2023-01-07T00:00:00Z"))
            .await
            .unwrap(),
        1
    );

    // The Error policy must reject a second item with the same id.
    let duplicate = pool.create_item(item("a", "2023-01-07T00:00:00Z")).await;
    assert!(
        duplicate.is_err(),
        "create_item must error on an existing id"
    );

    // delete_item removes it.
    pool.delete_item("c1", "a").await.unwrap();
    assert!(
        pool.get_item("c1", "a").await.unwrap().is_none(),
        "item deleted"
    );

    pool.close();
}

/// A collection with `item_assets` so `create_collection` derives a non-empty `fragment_config`.
fn frag_collection(id: &str) -> Value {
    json!({
        "id": id,
        "type": "Collection",
        "stac_version": "1.0.0",
        "description": "fragment test",
        "license": "proprietary",
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[null, null]]}},
        "links": [],
        "item_assets": {
            "B1": {"type": "image/tiff; application=geotiff", "title": "Coastal", "roles": ["data"], "eo:bands": [{"name": "B1", "common_name": "coastal"}]},
            "thumbnail": {"type": "image/png", "roles": ["thumbnail"]}
        }
    })
}

fn frag_item(collection: &str, id: &str) -> Value {
    json!({
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": id,
        "collection": collection,
        "geometry": {"type": "Point", "coordinates": [-105.1, 40.1]},
        "bbox": [-105.1, 40.1, -105.1, 40.1],
        "properties": {"datetime": "2023-01-07T00:00:00Z", "platform": "landsat-8"},
        "assets": {
            "B1": {"href": format!("https://x/{id}/B1.tif"), "type": "image/tiff; application=geotiff", "title": "Coastal", "roles": ["data"], "eo:bands": [{"name": "B1", "common_name": "coastal"}]},
            "thumbnail": {"href": format!("https://x/{id}/t.png"), "type": "image/png", "roles": ["thumbnail"]}
        },
        "links": [{"rel": "self", "href": format!("https://x/{id}")}]
    })
}

/// The Rust loader's fragment split must produce the SAME stored rows + fragments as the SQL
/// items_staging path. Load identical items (modulo collection) via both paths into two fragment-config
/// collections and assert the per-item columns + the fragment they point to are byte-identical.
#[tokio::test]
async fn fragment_load_matches_sql_items_staging() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&frag_collection("c_rust"))
        .await
        .unwrap();
    pool.create_collection(&frag_collection("c_sql"))
        .await
        .unwrap();

    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();

    // Both collections must actually fragment (else the test is vacuous).
    let with_config: i64 = client
        .query_one(
            "SELECT count(*) FROM collections WHERE id IN ('c_rust','c_sql') \
             AND fragment_config IS NOT NULL AND cardinality(fragment_config) > 0",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        with_config, 2,
        "both collections need a non-empty fragment_config"
    );

    // Rust loader -> c_rust.
    let rust_items: Vec<Value> = ["a", "b", "c"]
        .iter()
        .map(|id| frag_item("c_rust", id))
        .collect();
    pool.create_items(rust_items, ConflictPolicy::Ignore)
        .await
        .unwrap();

    // SQL items_staging -> c_sql.
    for id in ["a", "b", "c"] {
        client
            .execute(
                "INSERT INTO items_staging (content) VALUES ($1::jsonb)",
                &[&frag_item("c_sql", id)],
            )
            .await
            .unwrap();
    }

    // Rust must have actually created fragments + stamped fragment_id.
    let frags: i64 = client
        .query_one(
            "SELECT count(*) FROM item_fragments WHERE collection='c_rust'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(frags >= 1, "Rust load must create fragments");
    let stamped: i64 = client
        .query_one(
            "SELECT count(*) FROM items WHERE collection='c_rust' AND fragment_id IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stamped, 3, "all Rust items must reference a fragment");

    // Per-item columns + the fragment they point to must match between the two paths (symmetric diff = 0).
    let diff: i64 = client
        .query_one(
            "WITH cols AS (\
               SELECT i.collection, i.id, i.assets, i.properties, i.stac_version, i.stac_extensions, \
                      i.links, i.link_hrefs, f.content AS frag_content, f.links_template \
               FROM items i LEFT JOIN item_fragments f ON i.fragment_id = f.id \
               WHERE i.collection IN ('c_rust','c_sql')), \
             r AS (SELECT id, assets, properties, stac_version, stac_extensions, links, link_hrefs, frag_content, links_template FROM cols WHERE collection='c_rust'), \
             s AS (SELECT id, assets, properties, stac_version, stac_extensions, links, link_hrefs, frag_content, links_template FROM cols WHERE collection='c_sql') \
             SELECT count(*) FROM ((SELECT * FROM r EXCEPT SELECT * FROM s) UNION ALL (SELECT * FROM s EXCEPT SELECT * FROM r)) d",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        diff, 0,
        "Rust fragment split must match SQL items_staging row-for-row"
    );
    pool.close();
}

/// The Rust loader must NOT silently drop an item whose collection doesn't exist (the SQL items_staging
/// path does — see the ignored lib test tests::item_without_collection). ensure_partitions ->
/// check_partition raises for an unknown collection.
#[tokio::test]
async fn create_items_errors_on_absent_collection() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    // No collection created.
    let result = pool
        .create_item(item("orphan", "2023-01-07T00:00:00Z"))
        .await;
    assert!(
        result.is_err(),
        "loading an item for a nonexistent collection must error, not silently drop"
    );
    pool.close();
}

/// The Upsert policy must rewrite an item only when its content changed (flush's content-aware
/// DELETE-if-distinct + INSERT), leaving an unchanged re-upsert untouched.
#[tokio::test]
async fn upsert_only_rewrites_changed_items() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();

    let v1 = item("u", "2023-01-07T00:00:00Z");
    pool.upsert_item(v1.clone()).await.unwrap();
    let t1: chrono::DateTime<chrono::Utc> = client
        .query_one("SELECT pgstac_updated_at FROM items WHERE id='u'", &[])
        .await
        .unwrap()
        .get(0);

    // Re-upsert identical content -> not rewritten (pgstac_updated_at unchanged).
    pool.upsert_item(v1.clone()).await.unwrap();
    let t2: chrono::DateTime<chrono::Utc> = client
        .query_one("SELECT pgstac_updated_at FROM items WHERE id='u'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(t1, t2, "unchanged upsert must not rewrite the row");

    // Upsert changed content -> rewritten with the new value.
    let mut v2 = v1;
    v2["properties"]["eo:cloud_cover"] = json!(99.0);
    pool.upsert_item(v2).await.unwrap();
    let (cc, t3): (f64, chrono::DateTime<chrono::Utc>) = {
        let row = client
            .query_one(
                "SELECT eo_cloud_cover, pgstac_updated_at FROM items WHERE id='u'",
                &[],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(cc, 99.0, "changed upsert must store the new content");
    assert!(
        t3 > t1,
        "changed upsert must rewrite (newer pgstac_updated_at)"
    );
    pool.close();
}

/// After a load, partition_stats covers the data (row count raised, dtrange generous + dirty);
/// tighten_partition_stats then narrows to the EXACT extent and clears dirty.
#[tokio::test]
async fn tighten_narrows_widen_now_stats_to_exact() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();
    pool.create_items(
        vec![
            item("a", "2023-01-10T00:00:00Z"),
            item("b", "2023-01-20T00:00:00Z"),
        ],
        ConflictPolicy::Ignore,
    )
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();

    // Stats now cover the load: row count >= the 2 rows, dtrange COVERS the data span, dirty set.
    let row = client
        .query_one(
            "SELECT n, dtrange @> tstzrange('2023-01-10','2023-01-20','[]') AS covers, dirty \
             FROM partition_stats WHERE collection='c1'",
            &[],
        )
        .await
        .unwrap();
    let n: i64 = row.get("n");
    let covers: bool = row.get("covers");
    let dirty: bool = row.get("dirty");
    assert!(n >= 2, "row count covers the loaded rows");
    assert!(covers, "dtrange must cover the data span");
    assert!(
        dirty,
        "freshly-loaded partition is dirty (awaiting tighten)"
    );

    // Tighten -> exact extent + exact count + dirty cleared.
    client
        .execute(
            "SELECT tighten_partition_stats(partition) FROM partition_stats WHERE collection='c1'",
            &[],
        )
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT n, dtrange = tstzrange('2023-01-10','2023-01-20','[]') AS exact, dirty \
             FROM partition_stats WHERE collection='c1'",
            &[],
        )
        .await
        .unwrap();
    let n: i64 = row.get("n");
    let exact: bool = row.get("exact");
    let dirty: bool = row.get("dirty");
    assert_eq!(n, 2, "tighten sets the exact row count");
    assert!(exact, "tighten narrows dtrange to the exact data extent");
    assert!(!dirty, "tighten clears dirty");
    pool.close();
}

/// High-concurrency single-item ingest: many concurrent create_item
/// calls to the same collection/partition must all succeed (idempotent check_partition + covered-guard
/// widen + ON CONFLICT) and every item must land.
#[tokio::test]
async fn concurrent_single_item_creates_all_land() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();

    const N: usize = 24;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            pool.create_item(item(&format!("c{i:03}"), "2023-01-15T00:00:00Z"))
                .await
        }));
    }
    let mut ok = 0;
    for handle in handles {
        handle
            .await
            .unwrap()
            .expect("concurrent create_item must succeed");
        ok += 1;
    }
    assert_eq!(ok, N, "all concurrent creates succeeded");

    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
    let count: i64 = client
        .query_one("SELECT count(*) FROM items WHERE collection='c1'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, N as i64, "every concurrently-created item landed");
    pool.close();
}

/// The async tighten sweep clears every dirty partition (oldest first), exercising the loop over a
/// multi-partition (month-truncated) collection.
#[tokio::test]
async fn tighten_sweep_clears_all_dirty_partitions() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
    // Month-truncated collection -> items in different months land in different partitions.
    client
        .execute(
            "SELECT create_collection($1::jsonb, 'month')",
            &[&collection()],
        )
        .await
        .unwrap();

    pool.create_items(
        vec![
            item("a", "2023-01-10T00:00:00Z"),
            item("b", "2023-02-20T00:00:00Z"),
            item("c", "2023-03-05T00:00:00Z"),
        ],
        ConflictPolicy::Ignore,
    )
    .await
    .unwrap();

    let dirty_before: i64 = client
        .query_one("SELECT count(*) FROM partition_stats WHERE dirty", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        dirty_before >= 3,
        "each month partition is dirty after load (got {dirty_before})"
    );

    let tightened: i32 = client
        .query_one("SELECT tighten_dirty_partition_stats(NULL)", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        i64::from(tightened),
        dirty_before,
        "sweep tightens every dirty partition"
    );

    let dirty_after: i64 = client
        .query_one("SELECT count(*) FROM partition_stats WHERE dirty", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(dirty_after, 0, "no dirty partitions remain after the sweep");
    pool.close();
}

/// The typed `stac::api::TransactionClient` routes writes through the Rust loader (not SQL
/// `create_item`): a pooled `Client` (`pool.client()`) lands items via `add_items`, a `Client<Client>`
/// lands one via `add_item`, and `add_item` to an absent collection errors (the loader behavior, vs the
/// legacy SQL silent-drop).
#[tokio::test]
async fn transaction_client_routes_through_loader() {
    use stac::api::TransactionClient;

    let db = CloneDb::create().await;
    let mut pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();

    // A pooled Client as TransactionClient: typed add_items -> loader.
    let items: Vec<stac::Item> = vec![
        serde_json::from_value(item("ta", "2023-01-05T00:00:00Z")).unwrap(),
        serde_json::from_value(item("tb", "2023-02-05T00:00:00Z")).unwrap(),
    ];
    let mut pooled = pool.client().await.unwrap();
    TransactionClient::add_items(&mut pooled, items)
        .await
        .unwrap();

    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
    let n: i64 = client
        .query_one("SELECT count(*) FROM items WHERE collection='c1'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 2, "typed add_items landed both items through the loader");

    // Client<tokio_postgres::Client> as TransactionClient: typed add_item -> loader.
    let (raw, conn) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    raw.batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
    let mut wrapped = pgstac::Client::new(raw);
    let third: stac::Item = serde_json::from_value(item("tc", "2023-04-05T00:00:00Z")).unwrap();
    TransactionClient::add_item(&mut wrapped, third)
        .await
        .unwrap();
    let n: i64 = client
        .query_one("SELECT count(*) FROM items WHERE collection='c1'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 3, "Client<Client> add_item landed via the loader");

    // add_item to an absent collection errors (loader), not a silent drop.
    let mut orphan: stac::Item =
        serde_json::from_value(item("orphan", "2023-03-05T00:00:00Z")).unwrap();
    orphan.collection = Some("does-not-exist".to_string());
    assert!(
        TransactionClient::add_item(&mut pooled, orphan)
            .await
            .is_err(),
        "add_item errors on an absent collection"
    );

    pool.close();
}

/// The loader widens `item_field_registry` on every load: after a load the registry must be populated
/// and a SUPERSET of every field path in the loaded items (cross-checked against SQL `jsonb_field_rows`).
/// Guards against the widen call being silently dropped, which would leave the registry empty.
#[tokio::test]
async fn load_widens_field_registry() {
    let db = CloneDb::create().await;
    let mut pool = pool(&db).await;
    pool.create_collection(&collection()).await.unwrap();
    pool.create_items(
        vec![
            item("fr-a", "2023-01-05T00:00:00Z"),
            item("fr-b", "2023-02-05T00:00:00Z"),
        ],
        ConflictPolicy::Error,
    )
    .await
    .unwrap();

    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();

    let paths: i64 = client
        .query_one(
            "SELECT count(*) FROM item_field_registry WHERE collection='c1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(paths > 0, "the load populated the field registry");

    // Every path the items actually carried is in the registry (superset). `content_hydrate` injects an
    // empty top-level `links` array that the loaded items didn't have — a hydrate artifact, not a loaded data
    // field — so exclude it; the registry tracks what was loaded.
    let missing: i64 = client
        .query_one(
            "WITH sql_paths AS (SELECT DISTINCT r.path FROM items i \
                 CROSS JOIN LATERAL jsonb_field_rows(content_hydrate(i)) r \
                 WHERE i.collection='c1' AND r.path <> 'links') \
             SELECT count(*) FROM sql_paths s \
             WHERE NOT EXISTS (SELECT 1 FROM item_field_registry reg WHERE reg.collection='c1' AND reg.path=s.path)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        missing, 0,
        "field registry is a superset of every loaded item field"
    );

    pool.close();
}

/// The Rust loader populates the stored-row metadata the SQL `create_item` path did — `item_hash`
/// (32-byte sha256), `pgstac_updated_at`, `fragment_id` for a fragment collection, and the promoted
/// `datetime`/`geometry` — an upsert with changed content refreshes `item_hash`, and `delete_item`
/// writes a tombstone to `items_deleted_log`. This preserves, on the Rust loader, the coverage the
/// pgtap `003_items` create_item/update_item/delete_item assertions provided.
#[tokio::test]
async fn loader_stored_row_invariants() {
    let db = CloneDb::create().await;
    let pool = pool(&db).await;
    pool.create_collection(&frag_collection("cf"))
        .await
        .unwrap();
    pool.create_items(vec![frag_item("cf", "f1")], ConflictPolicy::Error)
        .await
        .unwrap();

    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT octet_length(item_hash) AS hash_len, \
                    pgstac_updated_at IS NOT NULL AS has_ts, \
                    fragment_id IS NOT NULL AS has_frag, \
                    datetime IS NOT NULL AS has_dt, \
                    geometry IS NOT NULL AS has_geom \
             FROM items WHERE id='f1' AND collection='cf'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, i32>("hash_len"),
        32,
        "item_hash is a 32-byte sha256"
    );
    assert!(row.get::<_, bool>("has_ts"), "pgstac_updated_at populated");
    assert!(
        row.get::<_, bool>("has_frag"),
        "fragment_id assigned for a fragment collection"
    );
    assert!(
        row.get::<_, bool>("has_dt"),
        "datetime promoted to its column"
    );
    assert!(row.get::<_, bool>("has_geom"), "geometry stored");

    let hash_before: Vec<u8> = client
        .query_one("SELECT item_hash FROM items WHERE id='f1'", &[])
        .await
        .unwrap()
        .get(0);
    let mut changed = frag_item("cf", "f1");
    changed["properties"]["platform"] = json!("landsat-9");
    pool.create_items(vec![changed], ConflictPolicy::Upsert)
        .await
        .unwrap();
    let hash_after: Vec<u8> = client
        .query_one("SELECT item_hash FROM items WHERE id='f1'", &[])
        .await
        .unwrap()
        .get(0);
    assert_ne!(
        hash_before, hash_after,
        "upsert with changed content refreshes item_hash"
    );

    pool.delete_item("cf", "f1").await.unwrap();
    let tombstones: i64 = client
        .query_one(
            "SELECT count(*) FROM items_deleted_log WHERE item_id='f1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        tombstones >= 1,
        "delete_item writes a tombstone to items_deleted_log"
    );
    pool.close();
}
