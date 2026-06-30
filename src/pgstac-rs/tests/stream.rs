//! The flat-memory streaming iterator: must yield the same items, in the same order, with the same
//! byte-for-byte hydration as the buffered `search_page`. Targets the rich fragment instance; skips if
//! unreachable. Requires the `pool` feature.

#![cfg(feature = "pool")]
#![allow(unused_crate_dependencies)]

use futures::StreamExt;
use pgstac::{ConnectConfig, DEFAULT_SEARCH_PATH, PgstacPool};
use serde_json::{Value, json};

async fn pool() -> Option<PgstacPool> {
    let dsn = std::env::var("PGSTAC_RS_TEST_RICH_DB").unwrap_or_else(|_| {
        "postgresql://username:password@localhost:5439/pgstac_rs_test_rich".into()
    });
    let config = ConnectConfig {
        dsn: Some(dsn),
        search_path: DEFAULT_SEARCH_PATH.to_string(),
        ..Default::default()
    };
    match PgstacPool::connect(config).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("[stream] skipping: cannot connect: {e}");
            None
        }
    }
}

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap_or_default().to_string()
}

#[tokio::test]
async fn stream_matches_search_page_order_and_hydration() {
    let Some(pool) = pool().await else {
        return;
    };
    let body = json!({"collections": ["landsat-c2-l2"], "limit": 1000});

    // The streaming iterator (flat memory).
    let streamed: Vec<Value> = {
        let stream = pool.search_items(body.clone(), None, Some(1000));
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.unwrap());
        }
        out
    };

    // The buffered reference, on a pooled connection.
    let client = pool.get().await.unwrap();
    let page = pgstac::search::search_page(&**client, &body, None, 1000)
        .await
        .unwrap();

    assert_eq!(streamed.len(), 1000, "stream returned {}", streamed.len());
    assert_eq!(
        streamed.iter().map(id_of).collect::<Vec<_>>(),
        page.features.iter().map(id_of).collect::<Vec<_>>(),
        "stream order/set differs from search_page",
    );
    // Full hydration must be byte-identical (same Hydrator, same fragment content).
    assert_eq!(
        streamed, page.features,
        "stream hydration differs from search_page"
    );
    eprintln!("[stream] 1000 items match search_page byte-for-byte");
}

#[tokio::test]
async fn search_matched_equals_page_number_matched() {
    let Some(pool) = pool().await else {
        return;
    };
    let body = json!({"collections": ["landsat-c2-l2"]});
    // The standalone count (parallel-context primitive) must agree with the page's numberMatched.
    let (page, matched) = tokio::join!(
        pool.search_page(&body, None, 10),
        pool.search_matched(&body)
    );
    assert_eq!(
        matched.unwrap(),
        page.unwrap().number_matched,
        "search_matched != search_page numberMatched"
    );
    eprintln!("[stream] search_matched agrees with search_page numberMatched");
}

#[tokio::test]
async fn collect_and_ndjson_helpers() {
    let Some(pool) = pool().await else {
        return;
    };
    let body = json!({"collections": ["landsat-c2-l2"]});

    let collected = pool
        .search_collect_items(body.clone(), Some(50))
        .await
        .unwrap();
    assert_eq!(collected.len(), 50);

    let mut buf: Vec<u8> = Vec::new();
    let n = pool.stream_ndjson(body, None, Some(50), &mut buf).await.unwrap();
    assert_eq!(n, 50);
    assert_eq!(buf.iter().filter(|&&b| b == b'\n').count(), 50);
    // The byte-path NDJSON (serialize-time merge) must equal the Value path feature-for-feature on real
    // landsat items (25-asset structure + shared fragment) — transitively == SQL content_hydrate.
    let line_features: Vec<Value> = buf
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice::<Value>(l).unwrap())
        .collect();
    assert_eq!(line_features, collected, "byte-path NDJSON != Value path");
    eprintln!("[stream] byte-path NDJSON matches the Value path for 50 landsat items");
}
