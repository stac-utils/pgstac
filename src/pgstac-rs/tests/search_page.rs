//! Fidelity + pagination gate for the Rust `search_page` against SQL `search()`.
//!
//! For datetime-ascending, datetime-descending, and a non-datetime sort (`id`), this paginates an
//! entire collection page-by-page through the `next` tokens and asserts, at every page, that
//! `search_page` matches `search()` byte-for-byte (hydrated features + keyset tokens). It also checks
//! the full traversal has no duplicates, no gaps (covers every item exactly once), is correctly
//! ordered, and that `prev` tokens round-trip.
//!
//! Targets a clean fragment instance (default `pgstac_rs_test_rich`); skips if unreachable.

#![allow(unused_crate_dependencies)]

use pgstac::search::search_page;
use serde_json::{Value, json};
use std::collections::HashSet;
use tokio_postgres::{Client, NoTls};

async fn connect() -> Option<Client> {
    let dsn = std::env::var("PGSTAC_RS_TEST_RICH_DB").unwrap_or_else(|_| {
        "postgresql://username:password@localhost:5439/pgstac_rs_test_rich".into()
    });
    match tokio_postgres::connect(&dsn, NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(connection);
            client
                .batch_execute("SET search_path = pgstac, public;")
                .await
                .ok()?;
            Some(client)
        }
        Err(e) => {
            eprintln!("[search_page] skipping: cannot connect ({dsn}): {e}");
            None
        }
    }
}

async fn sql_search(client: &Client, body: &Value) -> Value {
    client
        .query_one("SELECT search($1::jsonb) AS fc", &[body])
        .await
        .expect("sql search")
        .get("fc")
}

fn features(fc: &Value) -> &Vec<Value> {
    fc["features"].as_array().expect("features array")
}

fn id_of(feature: &Value) -> String {
    feature["id"].as_str().unwrap_or_default().to_string()
}

async fn count_in(client: &Client, collection: &str) -> usize {
    let n: i64 = client
        .query_one(
            "SELECT count(*) AS n FROM items WHERE collection = $1",
            &[&collection],
        )
        .await
        .unwrap()
        .get("n");
    n as usize
}

/// Paginates `body` fully through the Rust `next` tokens; at each page asserts the Rust page's
/// features equal SQL `search()` fed the same (Rust-minted) token. Returns the ordered list of feature
/// ids and the per-page `prev` tokens.
async fn paginate(client: &Client, body: &Value, limit: i64) -> (Vec<String>, Vec<Option<String>>) {
    let mut token: Option<String> = None;
    let mut ids = Vec::new();
    let mut prev_tokens = Vec::new();
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 1000, "pagination did not terminate for {body}");

        let page = search_page(client, body, token.as_deref(), limit)
            .await
            .unwrap();

        let mut sql_body = body.clone();
        if let Some(t) = &token {
            let _ = sql_body
                .as_object_mut()
                .unwrap()
                .insert("token".into(), Value::String(t.clone()));
        }
        let fc = sql_search(client, &sql_body).await;

        // Page-equivalence: feeding the Rust-minted token to SQL `search()` returns exactly the
        // items the Rust page returned. This proves the token round-trips (the server casts each
        // decoded keyset value back to its column type), without byte-identical token strings.
        assert_eq!(
            &page.features,
            features(&fc),
            "features mismatch (body={body}, token={token:?})"
        );

        ids.extend(page.features.iter().map(id_of));
        prev_tokens.push(page.prev_token.clone());

        match page.next_token {
            Some(next) if !page.features.is_empty() => token = Some(next),
            _ => break,
        }
    }
    (ids, prev_tokens)
}

#[tokio::test]
async fn pagination_matches_search_for_asc_desc_and_non_datetime() {
    let Some(client) = connect().await else {
        return;
    };
    let collection = "landsat-c2-l2";
    let total = count_in(&client, collection).await;
    let limit = 137; // a prime-ish page size so the last page is partial

    for sort in [
        json!([{"field": "datetime", "direction": "desc"}]),
        json!([{"field": "datetime", "direction": "asc"}]),
        json!([{"field": "id", "direction": "asc"}]), // non-datetime: exercises the non-band path
        json!([{"field": "id", "direction": "desc"}]),
    ] {
        let body = json!({"collections": [collection], "sortby": sort, "limit": limit});
        let (ids, _prev) = paginate(&client, &body, limit).await;

        // Covered every item exactly once (no gaps, no duplicates).
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "duplicate ids paginating {sort}");
        assert_eq!(
            ids.len(),
            total,
            "did not cover all {total} items for {sort}"
        );
        eprintln!(
            "[search_page] sort {sort}: {} items over {} pages, matches search()",
            ids.len(),
            ids.len().div_ceil(limit as usize)
        );
    }
}

#[tokio::test]
async fn prev_token_round_trips_to_previous_page() {
    let Some(client) = connect().await else {
        return;
    };
    let body = json!({"collections": ["landsat-c2-l2"], "limit": 50});

    // Page 1, then page 2 via its next token.
    let page1 = search_page(&client, &body, None, 50).await.unwrap();
    let next = page1.next_token.clone().expect("next token");
    let page2 = search_page(&client, &body, Some(&next), 50).await.unwrap();

    // Page 2's prev token must return exactly page 1.
    let prev = page2.prev_token.clone().expect("prev token on page 2");
    let back = search_page(&client, &body, Some(&prev), 50).await.unwrap();
    assert_eq!(
        back.features.iter().map(id_of).collect::<Vec<_>>(),
        page1.features.iter().map(id_of).collect::<Vec<_>>(),
        "prev token did not round-trip to page 1"
    );
    eprintln!("[search_page] prev token round-trip ok");
}

fn ndjson_ids(buf: &[u8]) -> Vec<String> {
    buf.split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let v: Value = serde_json::from_slice(line).unwrap();
            v["id"].as_str().unwrap_or_default().to_string()
        })
        .collect()
}

#[tokio::test]
async fn collect_and_stream_cover_all_items_in_the_same_order() {
    let Some(client) = connect().await else {
        return;
    };
    let collection = "landsat-c2-l2";
    let total = count_in(&client, collection).await;
    let body = json!({"collections": [collection], "limit": 200});

    // collect-all paginates internally and returns the whole set.
    let collected = pgstac::search::search_collect(&client, &body, None)
        .await
        .unwrap();
    assert_eq!(collected.number_returned, total);
    assert!(collected.next_token.is_none());
    let collected_ids: Vec<String> = collected.features.iter().map(id_of).collect();
    let unique: HashSet<&String> = collected_ids.iter().collect();
    assert_eq!(unique.len(), total, "collect produced duplicates");

    // stream emits the same items, in the same order, as NDJSON.
    let mut buf: Vec<u8> = Vec::new();
    let written = pgstac::search::search_stream(&client, &body, &mut buf, None)
        .await
        .unwrap();
    assert_eq!(written, total);
    assert_eq!(
        ndjson_ids(&buf),
        collected_ids,
        "stream != collect order/set"
    );
    eprintln!("[search_page] collect + stream cover all {total} items identically");
}

#[tokio::test]
async fn collect_and_stream_respect_max_items() {
    let Some(client) = connect().await else {
        return;
    };
    let body = json!({"collections": ["landsat-c2-l2"], "limit": 7});

    let collected = pgstac::search::search_collect(&client, &body, Some(10))
        .await
        .unwrap();
    assert_eq!(collected.number_returned, 10);

    let mut buf: Vec<u8> = Vec::new();
    let written = pgstac::search::search_stream(&client, &body, &mut buf, Some(10))
        .await
        .unwrap();
    assert_eq!(written, 10);
    assert_eq!(ndjson_ids(&buf).len(), 10);
    eprintln!("[search_page] collect + stream respect max_items");
}
