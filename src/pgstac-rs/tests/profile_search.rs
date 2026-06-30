#![cfg(feature = "pool")]
//! Manual profiling harness for the Rust search engine ([`PgstacPool::search_page`]) vs the SQL
//! `search()` function, warm-cache, on a populated database. Ignored by default; run with:
//!   PGSTAC_RS_PROFILE_DB=postgresql://…/pgstac_profile \
//!     cargo test --features pool --test profile_search -- --ignored --nocapture

use pgstac::{ConnectConfig, PgstacPool};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;

async fn median<F, Fut>(iters: usize, mut run: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for _ in 0..3 {
        run().await; // warm
    }
    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        run().await;
        times.push(t.elapsed());
    }
    times.sort();
    times[times.len() / 2]
}

#[tokio::test]
#[ignore = "manual profiling; set PGSTAC_RS_PROFILE_DB to a populated database"]
async fn profile_search_page() {
    let Ok(dsn) = std::env::var("PGSTAC_RS_PROFILE_DB") else {
        eprintln!("skip: set PGSTAC_RS_PROFILE_DB");
        return;
    };
    let pool = PgstacPool::connect(ConnectConfig {
        dsn: Some(dsn.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
        .batch_execute("SET search_path TO pgstac, public")
        .await
        .unwrap();

    let queries: [(&str, Value); 4] = [
        ("broad limit=100", json!({"limit": 100})),
        (
            "bbox (CONUS)",
            json!({"limit": 100, "bbox": [-125.0, 25.0, -65.0, 50.0]}),
        ),
        (
            "datetime 1mo",
            json!({"limit": 100, "datetime": "2020-07-01T00:00:00Z/2020-07-31T23:59:59Z"}),
        ),
        (
            "cql2 cloud<20",
            json!({"limit": 100, "filter-lang": "cql2-json", "filter": {"op": "<", "args": [{"property": "eo:cloud_cover"}, 20]}}),
        ),
    ];

    println!("\n=== search_page (Rust engine) vs SQL search() — median of 20 warm runs ===");
    for (name, body) in &queries {
        let rust = median(20, || async {
            let _ = pool.search_page(body, None, 100).await.unwrap();
        })
        .await;
        let sql = median(20, || async {
            let _ = client.query_one("SELECT search($1)", &[body]).await;
        })
        .await;
        println!(
            "  {name:<18} rust {:>7.1} ms    sql {:>7.1} ms",
            rust.as_secs_f64() * 1000.0,
            sql.as_secs_f64() * 1000.0,
        );
    }
    pool.close();
}
