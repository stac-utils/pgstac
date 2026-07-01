//! High-concurrency ingest correctness tests — especially many writers hammering a SINGLE partition,
//! which is where the partition-creation / partition_stats / fragment locking has to be right.
//!
//! Gated on `PGSTAC_TEST_DSN` (a DSN to a pgstac DB that already has the pgstac schema); the test is a
//! no-op skip when it's unset, so `cargo test` stays green without a database. Each test uses a unique
//! collection id so runs don't collide.
//!
//! Run: `PGSTAC_TEST_DSN=postgresql://username:password@localhost:5439/<db> cargo test --features pool \
//!       --test concurrency -- --nocapture`

#![cfg(feature = "pool")]

use pgstac::ingest::ConflictPolicy;
use pgstac::{ConnectConfig, PgstacPool};
use serde_json::{Value, json};

/// Flatten an error + its source chain into one string, so a failed assertion shows the real Postgres
/// message (SQLSTATE / server text) instead of tokio_postgres's terse top-line `db error`.
fn full_error(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut src = e.source();
    while let Some(inner) = src {
        s.push_str(" | ");
        s.push_str(&inner.to_string());
        src = inner.source();
    }
    s
}

fn dsn() -> Option<String> {
    std::env::var("PGSTAC_TEST_DSN").ok()
}

async fn connect(dsn: &str) -> PgstacPool {
    PgstacPool::connect(ConnectConfig {
        dsn: Some(dsn.to_string()),
        ..Default::default()
    })
    .await
    .expect("pool connect")
}

fn collection_json(id: &str) -> Value {
    json!({
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": id,
        "description": "concurrency test",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}
        },
        "links": []
    })
}

/// A minimal self-contained STAC item with the given id, in the given UTC month/day (so a `month`
/// partition_trunc routes it to one partition window).
fn make_item(collection: &str, id: &str, year: i32, month: u32, day: u32) -> Value {
    let lon = -100.0 + (f64::from(day) * 0.01);
    let lat = 40.0;
    json!({
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": id,
        "collection": collection,
        "geometry": {"type": "Polygon", "coordinates": [[
            [lon, lat], [lon + 0.1, lat], [lon + 0.1, lat + 0.1], [lon, lat + 0.1], [lon, lat]
        ]]},
        "bbox": [lon, lat, lon + 0.1, lat + 0.1],
        "properties": {"datetime": format!("{year}-{month:02}-{day:02}T00:00:00Z")},
        "assets": {},
        "links": []
    })
}

/// Create a fresh month-partitioned collection, returning a pooled connection's executor for assertions.
async fn setup_collection(pool: &PgstacPool, collection: &str) {
    pool.create_collection(&collection_json(collection))
        .await
        .expect("create_collection");
    let client = pool.get().await.expect("get");
    client
        .execute(
            "UPDATE pgstac.collections SET partition_trunc='month' WHERE id=$1",
            &[&collection],
        )
        .await
        .expect("set month");
}

async fn count(pool: &PgstacPool, sql: &str, collection: &str) -> i64 {
    let client = pool.get().await.expect("get");
    client
        .query_one(sql, &[&collection])
        .await
        .expect("count query")
        .get(0)
}

async fn cleanup(pool: &PgstacPool, collection: &str) {
    if let Ok(client) = pool.get().await {
        let _ = client
            .execute("SELECT pgstac.delete_collection($1)", &[&collection])
            .await;
    }
}

/// Many concurrent bulk loads, ALL landing in the SAME month partition. Asserts no task errors, no lost or
/// duplicated rows, exactly one partition, and a partition_stats.n that covers the data (golden rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_single_partition() {
    let Some(dsn) = dsn() else {
        eprintln!("skip concurrent_writers_single_partition: PGSTAC_TEST_DSN unset");
        return;
    };
    let pool = connect(&dsn).await;
    let collection = format!("conc_single_{}", std::process::id());
    cleanup(&pool, &collection).await;
    setup_collection(&pool, &collection).await;

    let writers = 8usize;
    let per_writer = 1500usize;
    let total = writers * per_writer;

    // Every item is in 2020-07 -> one partition; ids are globally unique across writers.
    let mut batches: Vec<Vec<Value>> = (0..writers)
        .map(|_| Vec::with_capacity(per_writer))
        .collect();
    for i in 0..total {
        let day = 1 + u32::try_from(i % 28).unwrap();
        batches[i % writers].push(make_item(&collection, &format!("item-{i}"), 2020, 7, day));
    }

    let mut set = tokio::task::JoinSet::new();
    for batch in batches {
        let pool = pool.clone();
        let _ = set.spawn(async move { pool.create_items(batch, ConflictPolicy::Error).await });
    }
    let mut errors = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined.expect("task panicked") {
            Ok(_) => {}
            Err(e) => errors.push(full_error(&e)),
        }
    }
    assert!(errors.is_empty(), "concurrent loads errored: {errors:?}");

    let n_items = count(
        &pool,
        "SELECT count(*) FROM pgstac.items WHERE collection=$1",
        &collection,
    )
    .await;
    let n_distinct = count(
        &pool,
        "SELECT count(DISTINCT id) FROM pgstac.items WHERE collection=$1",
        &collection,
    )
    .await;
    let n_parts = count(
        &pool,
        "SELECT count(*) FROM pgstac.partition_stats WHERE collection=$1",
        &collection,
    )
    .await;
    let stats_n = count(
        &pool,
        "SELECT COALESCE(sum(n),0)::bigint FROM pgstac.partition_stats WHERE collection=$1",
        &collection,
    )
    .await;

    cleanup(&pool, &collection).await;

    assert_eq!(
        n_items, total as i64,
        "lost or duplicated rows under concurrency"
    );
    assert_eq!(n_distinct, total as i64, "duplicate ids under concurrency");
    assert_eq!(n_parts, 1, "expected exactly one month partition");
    assert!(
        stats_n >= total as i64,
        "partition_stats.n ({stats_n}) under-counts the data ({total}) — golden rule violated"
    );
}

/// Concurrent bulk loads spread across MANY months (so writers race on different + shared partitions and
/// on parent-partition creation). Asserts no errors, no lost/duplicated rows, and the right partition count.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_many_partitions() {
    let Some(dsn) = dsn() else {
        eprintln!("skip concurrent_writers_many_partitions: PGSTAC_TEST_DSN unset");
        return;
    };
    let pool = connect(&dsn).await;
    let collection = format!("conc_many_{}", std::process::id());
    cleanup(&pool, &collection).await;
    setup_collection(&pool, &collection).await;

    let writers = 8usize;
    let months = 12u32;
    let per_writer = 1200usize;
    let total = writers * per_writer;

    // Each writer's items span all 12 months -> every writer touches every partition (max contention on
    // shared parent creation + each month's stats row).
    let mut batches: Vec<Vec<Value>> = (0..writers)
        .map(|_| Vec::with_capacity(per_writer))
        .collect();
    for i in 0..total {
        let month = 1 + u32::try_from(i).unwrap() % months;
        batches[i % writers].push(make_item(
            &collection,
            &format!("item-{i}"),
            2021,
            month,
            15,
        ));
    }

    let mut set = tokio::task::JoinSet::new();
    for batch in batches {
        let pool = pool.clone();
        let _ = set.spawn(async move { pool.create_items(batch, ConflictPolicy::Error).await });
    }
    let mut errors = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined.expect("task panicked") {
            Ok(_) => {}
            Err(e) => errors.push(full_error(&e)),
        }
    }
    assert!(
        errors.is_empty(),
        "concurrent multi-partition loads errored: {errors:?}"
    );

    let n_items = count(
        &pool,
        "SELECT count(*) FROM pgstac.items WHERE collection=$1",
        &collection,
    )
    .await;
    let n_distinct = count(
        &pool,
        "SELECT count(DISTINCT id) FROM pgstac.items WHERE collection=$1",
        &collection,
    )
    .await;
    let n_parts = count(
        &pool,
        "SELECT count(*) FROM pgstac.partition_stats WHERE collection=$1",
        &collection,
    )
    .await;

    cleanup(&pool, &collection).await;

    assert_eq!(
        n_items, total as i64,
        "lost or duplicated rows under concurrency"
    );
    assert_eq!(n_distinct, total as i64, "duplicate ids under concurrency");
    assert_eq!(
        n_parts, months as i64,
        "expected one partition per month touched"
    );
}

/// Precheck-driven upsert: skips unchanged items on re-ingest, detects a content change, and correctly
/// handles a datetime change that MOVES an item to a different partition (no duplicate across partitions —
/// the case per-partition id uniqueness would miss).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn precheck_skip_unchanged_and_partition_move() {
    let Some(dsn) = dsn() else {
        eprintln!("skip precheck_skip_unchanged_and_partition_move: PGSTAC_TEST_DSN unset");
        return;
    };
    let pool = connect(&dsn).await;
    let collection = format!("precheck_{}", std::process::id());
    cleanup(&pool, &collection).await;
    setup_collection(&pool, &collection).await;

    let n = 1000usize;
    let items: Vec<Value> = (0..n)
        .map(|i| {
            make_item(
                &collection,
                &format!("item-{i}"),
                2020,
                7,
                1 + u32::try_from(i % 28).unwrap(),
            )
        })
        .collect();
    let count_sql = "SELECT count(*) FROM pgstac.items WHERE collection=$1";

    // initial upsert: everything is new.
    let (unchanged0, loaded0) = pool
        .upsert_items_precheck(items.clone(), ConflictPolicy::Upsert)
        .await
        .unwrap();
    assert_eq!(
        (unchanged0, loaded0),
        (0, n as u64),
        "initial load: all new"
    );

    // re-upsert identical items: all unchanged, nothing loaded (the skip-unchanged / re-ingest win).
    let (unchanged1, loaded1) = pool
        .upsert_items_precheck(items.clone(), ConflictPolicy::Upsert)
        .await
        .unwrap();
    assert_eq!(
        (unchanged1, loaded1),
        (n as u64, 0),
        "re-ingest of identical items skips everything"
    );

    // change one item's content -> exactly one changed/loaded, the rest skipped, no duplicate.
    let mut changed = items.clone();
    changed[0]["properties"]["pgstac:test"] = json!("modified");
    let (unchanged2, loaded2) = pool
        .upsert_items_precheck(changed, ConflictPolicy::Upsert)
        .await
        .unwrap();
    assert_eq!(
        (unchanged2, loaded2),
        ((n - 1) as u64, 1),
        "one content change -> one load"
    );
    assert_eq!(
        count(&pool, count_sql, &collection).await,
        n as i64,
        "content change must not duplicate"
    );

    // move one item to a different month (datetime change -> different partition). The precheck reports it
    // 'new' (the new partition holds no such id), so we load with DELSERT, whose cross-partition delete
    // removes the old row from its former partition -> exactly one row. (UPSERT would orphan it by design —
    // it only touches the partition the new row routes to.)
    let mut moved = items.clone();
    moved[0]["properties"]["pgstac:test"] = json!("modified"); // item-0 already stored as modified -> unchanged
    moved[1] = make_item(&collection, "item-1", 2020, 8, 15); // item-1: 2020-07 -> 2020-08
    let (unchanged3, loaded3) = pool
        .upsert_items_precheck(moved, ConflictPolicy::Delsert)
        .await
        .unwrap();
    assert_eq!(
        (unchanged3, loaded3),
        ((n - 1) as u64, 1),
        "only the moved item is loaded"
    );
    assert_eq!(
        count(&pool, count_sql, &collection).await,
        n as i64,
        "partition move must not duplicate"
    );
    let item1_rows = count(
        &pool,
        "SELECT count(*) FROM pgstac.items WHERE collection=$1 AND id='item-1'",
        &collection,
    )
    .await;
    assert_eq!(
        item1_rows, 1,
        "moved item must exist exactly once (not in both partitions)"
    );

    cleanup(&pool, &collection).await;
}

/// Locks in the upsert vs delsert semantics on a cross-partition move: `Upsert` only touches the partition
/// the new row routes to, so it ORPHANS the old row; `Delsert` deletes cross-partition, so it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_orphans_on_move_but_delsert_does_not() {
    let Some(dsn) = dsn() else {
        eprintln!("skip upsert_orphans_on_move_but_delsert_does_not: PGSTAC_TEST_DSN unset");
        return;
    };
    let pool = connect(&dsn).await;
    let collection = format!("orphan_{}", std::process::id());
    cleanup(&pool, &collection).await;
    setup_collection(&pool, &collection).await;
    let id_rows = "SELECT count(*) FROM pgstac.items WHERE collection=$1 AND id='x'";

    // initial: x in 2020-07.
    let _ = pool
        .create_items(
            vec![make_item(&collection, "x", 2020, 7, 1)],
            ConflictPolicy::Ignore,
        )
        .await
        .unwrap();
    assert_eq!(count(&pool, id_rows, &collection).await, 1, "initial load");

    // UPSERT a move 2020-07 -> 2020-08: same-partition only, so the 2020-07 row is left behind -> 2 rows.
    let _ = pool
        .create_items(
            vec![make_item(&collection, "x", 2020, 8, 1)],
            ConflictPolicy::Upsert,
        )
        .await
        .unwrap();
    assert_eq!(
        count(&pool, id_rows, &collection).await,
        2,
        "Upsert on a cross-partition move orphans the old row (by design)"
    );

    // DELSERT a move -> 2020-09: the cross-partition delete removes BOTH prior rows -> 1 row.
    let _ = pool
        .create_items(
            vec![make_item(&collection, "x", 2020, 9, 1)],
            ConflictPolicy::Delsert,
        )
        .await
        .unwrap();
    assert_eq!(
        count(&pool, id_rows, &collection).await,
        1,
        "Delsert removes the old rows cross-partition"
    );

    cleanup(&pool, &collection).await;
}
