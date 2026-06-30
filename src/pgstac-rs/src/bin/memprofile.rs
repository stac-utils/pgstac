//! Heap profiler for the Rust loader (built with `--features profiling`).
//!
//! Usage: `memprofile <collection.json> <parquet> <limit>` (DSN from $PGSTAC_DSN).
//!
//! Reads `limit` items from a stac-geoparquet file (outside the profiled scope), then profiles a single
//! `create_items` batch through the Rust loader under [`dhat`], printing the peak live heap ("t-gmax")
//! and writing `dhat-heap.json` (open in https://nnethercote.github.io/dh_view/dh_view.html for the
//! allocation sites). Run it at several `limit`s to see how per-batch memory scales with batch size.

use pgstac::ingest::ConflictPolicy;
use pgstac::{ConnectConfig, PgstacPool};
use serde_json::Value;
use std::fs::File;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: memprofile <collection.json> <parquet> <limit>");
        std::process::exit(2);
    }
    let collection_path = &args[1];
    let parquet_path = &args[2];
    let limit: usize = args[3].parse()?;

    // --- setup (NOT profiled): connect, create the collection, read `limit` items ---
    let mut config = ConnectConfig::from_env();
    if let Ok(dsn) = std::env::var("PGSTAC_DSN") {
        config.dsn = Some(dsn);
    }
    let pool = PgstacPool::connect(config).await?;

    let collection: Value = serde_json::from_slice(&std::fs::read(collection_path)?)?;
    let collection = collection.get("stac").cloned().unwrap_or(collection);
    pool.create_collection(&collection).await?;

    let mut items: Vec<Value> = Vec::with_capacity(limit);
    'outer: for batch in stac::geoparquet::from_reader_iter(File::open(parquet_path)?)? {
        for item in batch? {
            items.push(serde_json::to_value(item)?);
            if items.len() >= limit {
                break 'outer;
            }
        }
    }
    let count = items.len();
    eprintln!("read {count} items; profiling create_items …");

    // --- profiled scope: dehydrate + fragment split + binary COPY of one batch ---
    let profiler = dhat::Profiler::new_heap();
    let loaded = pool.create_items(items, ConflictPolicy::Upsert).await?;
    drop(profiler); // prints Total / t-gmax (peak) / t-end to stderr + writes dhat-heap.json

    eprintln!("loaded {loaded} items (batch={count})");
    pool.close();
    Ok(())
}
