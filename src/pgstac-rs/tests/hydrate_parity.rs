//! Hydration parity gate (ARCHITECTURE A2, §8).
//!
//! For sampled items, asserts the Rust `Hydrator` output equals the SQL
//! `content_hydrate(i)` output, on **both** a 0.9.11 (base_item) and a 0.10
//! (fragment) instance. This is the top-risk component and the gate for Phase 1.
//!
//! These tests connect to disposable clones. By default they target:
//!
//! * 0.9.11: `PGSTAC_PARITY_DB_0911` (default `...:5439/a_parity0911`)
//! * 0.10:   `PGSTAC_PARITY_DB_010`  (default `...:5439/a_parity010`)
//!
//! Each test skips (passes with a logged note) if its DB is unreachable, so the
//! suite stays green on machines without the fixtures; CI/overnight runs point
//! the env vars at real clones.
//!
//! Comparison is **semantic** (`serde_json::Value` equality is order-independent
//! for objects), which is the correct criterion: jsonb has no inherent key order
//! and `content_hydrate` makes no order guarantee.

#![allow(unused_crate_dependencies)]

use pgstac::hydrate::{HydrationModel, Hydrator};
use pgstac::source::{detect_hydration_model, load_collection_context};
use pgstac::source::{fetch_dehydrated_limited, fetch_fragments};
use serde_json::Value;
use std::collections::HashMap;
use tokio_postgres::{Client, NoTls};

async fn connect(env: &str, default: &str) -> Option<Client> {
    let conn = std::env::var(env).unwrap_or_else(|_| default.to_string());
    match tokio_postgres::connect(&conn, NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            if client
                .batch_execute("SET search_path = pgstac, public;")
                .await
                .is_err()
            {
                return None;
            }
            Some(client)
        }
        Err(e) => {
            eprintln!("[parity] skipping: cannot connect via {env} ({default}): {e}");
            None
        }
    }
}

/// Lists collection ids in the instance.
async fn list_collections(client: &Client) -> Vec<String> {
    let rows = client
        .query("SELECT id FROM collections ORDER BY id", &[])
        .await
        .expect("list collections");
    rows.iter().map(|r| r.get::<_, String>("id")).collect()
}

/// Fetches the SQL `content_hydrate(i)` output for each item id in a collection,
/// limited to `sample_limit` ordered `(datetime, id)`.
async fn sql_hydrated(
    client: &Client,
    collection: &str,
    sample_limit: i64,
) -> HashMap<String, Value> {
    let rows = client
        .query(
            "SELECT id, content_hydrate(i) AS h \
             FROM items i WHERE collection = $1 \
             ORDER BY datetime, id LIMIT $2",
            &[&collection, &sample_limit],
        )
        .await
        .expect("sql content_hydrate");
    rows.iter()
        .map(|r| (r.get::<_, String>("id"), r.get::<_, Value>("h")))
        .collect()
}

/// Runs the parity comparison for one instance.
async fn run_parity(client: &Client, sample_limit: i64) {
    let version: String = client
        .query_one("SELECT get_version() AS v", &[])
        .await
        .expect("get_version")
        .get("v");
    let model = detect_hydration_model(client).await.expect("detect model");
    let hydrator = Hydrator::new(model);
    eprintln!("[parity] instance version {version} -> model {model:?}");

    let collections = list_collections(client).await;
    assert!(!collections.is_empty(), "instance has no collections");

    let mut total = 0usize;
    for collection in &collections {
        let ctx = load_collection_context(client, model, collection)
            .await
            .expect("load context");

        let where_clause = "collection = $1";
        let items = fetch_dehydrated_limited(
            client,
            model,
            "items",
            Some(where_clause),
            &[&collection],
            Some(sample_limit),
        )
        .await
        .expect("fetch dehydrated");
        if items.is_empty() {
            continue;
        }

        // Load fragments for any items that reference one (0.10).
        let frag_ids: Vec<i64> = items.iter().filter_map(|i| i.fragment_id).collect();
        let fragments = fetch_fragments(client, &frag_ids)
            .await
            .expect("fetch fragments");

        let sql = sql_hydrated(client, collection, sample_limit).await;

        for item in &items {
            let fragment = item.fragment_id.and_then(|id| fragments.get(&id));
            let rust = hydrator.hydrate(item.clone(), &ctx, fragment);
            let expected = sql
                .get(&item.id)
                .unwrap_or_else(|| panic!("no SQL hydrate for id={}", item.id));
            assert_eq!(
                &rust,
                expected,
                "hydration mismatch for id={} collection={}\n--- rust ---\n{}\n--- sql ---\n{}",
                item.id,
                item.collection,
                serde_json::to_string_pretty(&rust).unwrap(),
                serde_json::to_string_pretty(expected).unwrap(),
            );
            total += 1;
        }
        eprintln!("[parity] {collection}: {} items matched", items.len());
    }
    assert!(total > 0, "compared zero items");
    eprintln!("[parity] {model:?}: total {total} items, all match");
}

#[tokio::test]
async fn parity_base_item_0911() {
    let Some(client) = connect(
        "PGSTAC_PARITY_DB_0911",
        "postgresql://username:password@localhost:5439/pgstac_v0911_bench",
    )
    .await
    else {
        return;
    };
    assert_eq!(
        detect_hydration_model(&client).await.unwrap(),
        HydrationModel::BaseItem,
        "0911 fixture should be base_item model"
    );
    run_parity(&client, 2000).await;
}

#[tokio::test]
async fn parity_fragment_010() {
    let Some(client) = connect(
        "PGSTAC_PARITY_DB_010",
        "postgresql://username:password@localhost:5439/pgstac_rs_test_rich",
    )
    .await
    else {
        return;
    };
    assert_eq!(
        detect_hydration_model(&client).await.unwrap(),
        HydrationModel::Fragment,
        "010 fixture should be fragment model"
    );
    run_parity(&client, 2000).await;
}
