//! The `pgstac` CLI binary (built with `--features cli`).
//!
//! Thin wrapper over the `pgstac` library: one binary, subcommand per feature
//! (`dump`/`search`/`load`/`restore`). The CLI is pure arg-parsing + wiring; all
//! logic lives in the lib. `load`/`restore` route through the Rust loader
//! ([`PgstacPool::create_items`] → `load_items`), so dehydration + fragment
//! splitting + the binary COPY all happen in Rust.

use clap::{Parser, Subcommand, ValueEnum};
use pgstac::export::format::ParquetCompression;
use pgstac::export::plan::Prefilter;
use pgstac::export::sink::{DirSink, StdoutSink, TarSink};
use pgstac::export::{DumpConfig, DumpPlanner};
use pgstac::ingest::ConflictPolicy;
use pgstac::{ConnectConfig, PgstacPool, PoolOptions};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;
use tokio_postgres::NoTls;

#[derive(Parser, Debug)]
#[command(
    name = "pgstac",
    version,
    about = "pgstac tooling: dump, search, load, restore"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Dump a pgstac instance (or a subset) to a directory, tar, S3, or stdout.
    Dump(DumpArgs),
    /// Stream a search/CQL2 result as NDJSON / ItemCollection / geoparquet
    /// (0.10 only — rides the keyset search engine).
    Search(SearchArgs),
    /// Load STAC items + collections (stac-geoparquet, NDJSON, or JSON) through
    /// the Rust loader: dehydration, fragment splitting, and the binary COPY all
    /// run in Rust. Collections are created before items.
    Load(LoadArgs),
    /// Restore a `pgstac dump` directory/tar (collections + partition geoparquet)
    /// through the same Rust loader.
    Restore(RestoreArgs),
    /// Run the async partition-stats maintenance:
    /// recompute exact bounds + counts for partitions ingest left dirty.
    Maintain(MaintainArgs),
    /// Delete an item (with --item) or a whole collection (without --item).
    Delete(DeleteArgs),
}

#[derive(clap::Args, Debug)]
struct DeleteArgs {
    /// Postgres connection string. Defaults to $PGSTAC_DSN or a local dev DSN.
    #[arg(long, env = "PGSTAC_DSN")]
    dsn: Option<String>,

    /// Collection id (required).
    #[arg(long, short)]
    collection: String,

    /// Item id. Given, deletes that item; omitted, deletes the whole collection (and its items).
    #[arg(long)]
    item: Option<String>,
}

#[derive(clap::Args, Debug)]
struct MaintainArgs {
    /// Postgres connection string. Defaults to $PGSTAC_DSN or a local dev DSN.
    #[arg(long, env = "PGSTAC_DSN")]
    dsn: Option<String>,

    /// Cap the number of dirty partitions tightened this run (oldest first).
    /// Omit to tighten all dirty partitions.
    #[arg(long)]
    limit: Option<i32>,
}

#[derive(clap::Args, Debug)]
struct LoadArgs {
    /// Postgres connection string. Defaults to $PGSTAC_DSN or a local dev DSN.
    #[arg(long, env = "PGSTAC_DSN")]
    dsn: Option<String>,

    /// Inputs to load: stac-geoparquet (`.parquet`/`.geoparquet`), NDJSON
    /// (`.ndjson`), JSON (item or ItemCollection), or collection JSON. Directories
    /// are scanned recursively. Collection files are loaded before items.
    #[arg(required = true)]
    inputs: Vec<PathBuf>,

    /// Items per binary-COPY batch (one loader transaction per batch).
    #[arg(long, default_value_t = 5_000)]
    batch_size: usize,

    /// Number of batches loaded concurrently (each uses one pooled connection).
    #[arg(long, default_value_t = default_ingest_parallelism())]
    concurrency: usize,

    /// Conflict policy when an item id already exists.
    #[arg(long, value_enum, default_value_t = LoadPolicy::Upsert)]
    policy: LoadPolicy,

    /// Pool size. Defaults to max(concurrency, 4).
    #[arg(long)]
    pool_size: Option<usize>,

    /// Cap the total number of items loaded (across all sources). For benchmarking / sampling.
    #[arg(long)]
    limit: Option<usize>,
}

/// Default ingest parallelism: CPU count capped at 8 (a safe ceiling for decode/connection fan-out).
/// Powers both `--concurrency` (parallel load batches) and the ndjson decode-thread default, so the loader
/// parallelizes out of the box — profiling showed single-threaded decode was the big-item ingest floor.
fn default_ingest_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(8)
}

#[derive(clap::Args, Debug)]
struct RestoreArgs {
    /// Postgres connection string. Defaults to $PGSTAC_DSN or a local dev DSN.
    #[arg(long, env = "PGSTAC_DSN")]
    dsn: Option<String>,

    /// A `pgstac dump` directory (collections.ndjson + per-partition geoparquet).
    src: PathBuf,

    /// Items per binary-COPY batch.
    #[arg(long, default_value_t = 5_000)]
    batch_size: usize,

    /// Number of partition files restored concurrently.
    #[arg(long, default_value_t = default_ingest_parallelism())]
    concurrency: usize,

    /// Pool size. Defaults to max(concurrency, 4).
    #[arg(long)]
    pool_size: Option<usize>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum LoadPolicy {
    /// Replace an existing item only when its content changed.
    Upsert,
    /// Skip items whose id already exists.
    Ignore,
    /// Fail the batch if any item id already exists.
    Error,
}

impl From<LoadPolicy> for ConflictPolicy {
    fn from(p: LoadPolicy) -> Self {
        match p {
            LoadPolicy::Upsert => ConflictPolicy::Upsert,
            LoadPolicy::Ignore => ConflictPolicy::Ignore,
            LoadPolicy::Error => ConflictPolicy::Error,
        }
    }
}

#[derive(clap::Args, Debug)]
struct SearchArgs {
    /// Postgres connection string. Defaults to $PGSTAC_DSN or a local dev DSN.
    #[arg(long, env = "PGSTAC_DSN")]
    dsn: Option<String>,

    /// Output destination: a file path or `-` for stdout (default).
    #[arg(long, short, default_value = "-")]
    out: String,

    /// Output format.
    #[arg(long, value_enum, default_value_t = SearchFormat::Ndjson)]
    format: SearchFormat,

    /// Restrict to these collection ids (repeatable).
    #[arg(long = "collection", short = 'c')]
    collections: Vec<String>,

    /// Restrict to these item ids (repeatable).
    #[arg(long = "id")]
    ids: Vec<String>,

    /// Bbox: west,south,east,north (EPSG:4326).
    #[arg(long, value_delimiter = ',', num_args = 4, allow_hyphen_values = true)]
    bbox: Option<Vec<f64>>,

    /// Datetime / interval (RFC3339, STAC datetime syntax).
    #[arg(long)]
    datetime: Option<String>,

    /// CQL2-JSON filter (a JSON object).
    #[arg(long)]
    filter: Option<String>,

    /// Full STAC search body as JSON (sortby / fields / intersects / query / conf / ...). When set it
    /// overrides the individual filter flags above, letting the CLI drive the whole search spec — handy
    /// for benchmarking the engine on sort + fields (full-vs-slim) + complex geometry.
    #[arg(long)]
    search_json: Option<String>,

    /// Page size (the keyset limit). Omit for the server default.
    #[arg(long)]
    limit: Option<u64>,

    /// Continuation token (`next:<keyset>` / `prev:<keyset>`). With this set the
    /// ItemCollection format returns exactly that one page.
    #[arg(long)]
    token: Option<String>,

    /// Max rows per parquet row-group (geoparquet only). Smaller values bound peak memory when
    /// streaming out parquet. Defaults to $PGSTAC_PARQUET_ROW_GROUP_SIZE, else the parquet default.
    #[arg(long, env = "PGSTAC_PARQUET_ROW_GROUP_SIZE")]
    row_group_size: Option<usize>,

    /// Cap the total items streamed (NDJSON / geoparquet).
    #[arg(long)]
    max_items: Option<usize>,

    /// Parquet compression codec (geoparquet only).
    #[arg(long, value_enum, default_value_t = CompressionArg::Zstd)]
    compression: CompressionArg,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum SearchFormat {
    /// Newline-delimited JSON, one item per line (streams all pages).
    Ndjson,
    /// A single STAC FeatureCollection page (SQL-faithful next/prev + context).
    Itemcollection,
    /// stac-geoparquet of the (bounded) result.
    Geoparquet,
}

#[derive(clap::Args, Debug)]
struct DumpArgs {
    /// Postgres connection string. Defaults to $PGSTAC_DSN or a local dev DSN.
    #[arg(long, env = "PGSTAC_DSN")]
    dsn: Option<String>,

    /// Output destination: a directory path, `s3://bucket/prefix` (or other
    /// object-store URL), a `*.tar` / `*.tar.zst` file, or `-` for stdout.
    #[arg(long, short)]
    out: String,

    /// Restrict to these collection ids (repeatable). Omit for a full dump.
    #[arg(long = "collection", short = 'c')]
    collections: Vec<String>,

    /// Datetime prefilter start (RFC3339, inclusive).
    #[arg(long, requires = "datetime_end")]
    datetime_start: Option<String>,

    /// Datetime prefilter end (RFC3339, exclusive).
    #[arg(long, requires = "datetime_start")]
    datetime_end: Option<String>,

    /// Bbox prefilter: west,south,east,north (EPSG:4326).
    #[arg(long, value_delimiter = ',', num_args = 4, allow_hyphen_values = true)]
    bbox: Option<Vec<f64>>,

    /// Parquet compression codec.
    #[arg(long, value_enum, default_value_t = CompressionArg::Zstd)]
    compression: CompressionArg,

    /// Continue past per-item errors, recording skips in the report.
    #[arg(long)]
    skip_errors: bool,

    /// Memory budget for the buffered geoparquet path (bytes). Default ~25% RAM.
    #[arg(long)]
    memory_budget: Option<u64>,

    /// Max partitions dumped concurrently. 1 = sequential. Values > 1
    /// open one extra Postgres connection per worker.
    #[arg(long, default_value_t = default_ingest_parallelism())]
    concurrency: usize,

    /// Dump the whole instance under one repeatable-read snapshot so
    /// concurrent ingest cannot tear the output. Implies a parallel run.
    #[arg(long)]
    consistent: bool,

    /// Write a `.tar.zst` (compressed) tar when --out is a `.tar` path.
    #[arg(long)]
    tar_zstd: bool,

    /// Report the plan without writing data.
    #[arg(long)]
    dry_run: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum CompressionArg {
    Zstd,
    Snappy,
    Uncompressed,
}

impl From<CompressionArg> for ParquetCompression {
    fn from(c: CompressionArg) -> Self {
        match c {
            CompressionArg::Zstd => ParquetCompression::Zstd,
            CompressionArg::Snappy => ParquetCompression::Snappy,
            CompressionArg::Uncompressed => ParquetCompression::Uncompressed,
        }
    }
}

const DEFAULT_DSN: &str = "postgresql://username:password@localhost:5432/postgis";

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: failed to start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let handle = runtime.handle().clone();
    let result = match cli.command {
        Command::Dump(args) => runtime.block_on(run_dump(args, handle)),
        Command::Search(args) => runtime.block_on(run_search(args)),
        Command::Load(args) => runtime.block_on(run_load(args)),
        Command::Restore(args) => runtime.block_on(run_restore(args)),
        Command::Maintain(args) => runtime.block_on(run_maintain(args)),
        Command::Delete(args) => runtime.block_on(run_delete(args)),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            // Walk the source chain: tokio_postgres::Error renders as a terse `db error`, so a Postgres
            // failure otherwise hides its SQLSTATE + server message. Surface them (plus detail/hint).
            let mut src = e.source();
            while let Some(s) = src {
                if let Some(db) = s.downcast_ref::<tokio_postgres::error::DbError>() {
                    eprintln!("  SQLSTATE {}: {}", db.code().code(), db.message());
                    if let Some(detail) = db.detail() {
                        eprintln!("  detail: {detail}");
                    }
                    if let Some(hint) = db.hint() {
                        eprintln!("  hint: {hint}");
                    }
                } else if s.downcast_ref::<tokio_postgres::Error>().is_none() {
                    // tokio_postgres::Error renders as a terse "db error"; skip it — its DbError source
                    // (handled on the next hop) carries the real SQLSTATE + message.
                    eprintln!("  caused by: {s}");
                }
                src = s.source();
            }
            ExitCode::FAILURE
        }
    }
}

async fn run_dump(
    args: DumpArgs,
    runtime: tokio::runtime::Handle,
) -> Result<(), Box<dyn std::error::Error>> {
    let dsn = args.dsn.clone().unwrap_or_else(|| DEFAULT_DSN.to_string());
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    client
        .batch_execute("SET search_path = pgstac, public;")
        .await?;

    let datetime = match (&args.datetime_start, &args.datetime_end) {
        (Some(s), Some(e)) => Some((s.clone(), e.clone())),
        _ => None,
    };
    let bbox = match &args.bbox {
        Some(b) if b.len() == 4 => Some([b[0], b[1], b[2], b[3]]),
        _ => None,
    };
    let prefilter = Prefilter { datetime, bbox };

    // Resume: if a prior _checkpoint.json sits next to a directory dump,
    // load + verify it so already-completed partition files are skipped.
    let resume_completed =
        if args.out != "-" && !is_tar_path(&args.out) && object_store_url(&args.out).is_none() {
            load_resume_set(&args.out)
        } else {
            Default::default()
        };

    let config = DumpConfig {
        collection_ids: if args.collections.is_empty() {
            None
        } else {
            Some(args.collections.clone())
        },
        prefilter,
        compression: args.compression.into(),
        skip_errors: args.skip_errors,
        resume_completed,
        memory_budget: args.memory_budget,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        server: None,
        concurrency: args.concurrency,
        consistent: args.consistent,
    };

    if args.dry_run {
        return dry_run(&client, &config).await;
    }

    // A parallel run is requested when concurrency > 1 or --consistent is set.
    // The TAR/stdout sinks serialize internally but still work; fs/object_store
    // sinks write concurrently.
    let parallel = config.concurrency > 1 || config.consistent;
    let planner = DumpPlanner::new(config);

    // Choose a sink from --out (format ⟂ sink; here output is always the
    // backup geoparquet layout).
    let report = if parallel {
        use pgstac::export::DsnFactory;
        use std::sync::Arc;
        let factory = DsnFactory::new(dsn.clone());
        if args.out == "-" {
            planner
                .run_parallel(&client, Arc::new(StdoutSink), &factory)
                .await?
        } else if is_tar_path(&args.out) {
            let zstd = args.tar_zstd || args.out.ends_with(".tar.zst");
            let sink = Arc::new(TarSink::new(&args.out, zstd)?);
            planner.run_parallel(&client, sink, &factory).await?
        } else if let Some(url) = object_store_url(&args.out) {
            let sink = Arc::new(pgstac::export::sink::ObjectStoreSink::from_url_opts(
                &url,
                std::iter::empty(),
                runtime,
            )?);
            planner.run_parallel(&client, sink, &factory).await?
        } else {
            let sink = Arc::new(DirSink::new(&args.out)?);
            planner.run_parallel(&client, sink, &factory).await?
        }
    } else if args.out == "-" {
        planner.run(&client, &StdoutSink).await?
    } else if is_tar_path(&args.out) {
        let zstd = args.tar_zstd || args.out.ends_with(".tar.zst");
        let sink = TarSink::new(&args.out, zstd)?;
        planner.run(&client, &sink).await?
    } else if let Some(url) = object_store_url(&args.out) {
        let sink = pgstac::export::sink::ObjectStoreSink::from_url_opts(
            &url,
            std::iter::empty(),
            runtime,
        )?;
        planner.run(&client, &sink).await?
    } else {
        let sink = DirSink::new(&args.out)?;
        planner.run(&client, &sink).await?
    };

    eprintln!(
        "dump complete: {} collections, {} partition files, {} items{}",
        report.collections,
        report.partitions,
        report.items,
        if report.skipped > 0 {
            format!(", {} skipped", report.skipped)
        } else {
            String::new()
        }
    );
    Ok(())
}

/// Opens the output sink for a search export: a file, or stdout for `-`.
fn make_sink(out: &str) -> Result<Box<dyn std::io::Write>, Box<dyn std::error::Error>> {
    Ok(if out == "-" {
        Box::new(std::io::BufWriter::new(std::io::stdout().lock()))
    } else {
        Box::new(std::io::BufWriter::new(std::fs::File::create(out)?))
    })
}

/// Search-driven export (0.10 only): stream a search / CQL2 result as NDJSON, a single ItemCollection
/// page, or stac-geoparquet. Every page is built in Rust from `search_plan` (Rust band-stepping +
/// keyset minting); the SQL `search()` function is never called.
async fn run_search(args: SearchArgs) -> Result<(), Box<dyn std::error::Error>> {
    use stac::api::{ItemsClient, Search};
    use std::io::Write as _;

    let mut config = ConnectConfig::from_env();
    if let Some(dsn) = args.dsn.clone() {
        config.dsn = Some(dsn);
    }
    let pool = PgstacPool::connect(config).await?;

    // Build the Search from CLI args.
    let mut search = Search {
        collections: args.collections.clone(),
        ids: args.ids.clone(),
        ..Default::default()
    };
    if let Some(b) = &args.bbox
        && b.len() == 4
    {
        search = search.bbox(stac::Bbox::new(b[0], b[1], b[2], b[3]));
    }
    if let Some(dt) = &args.datetime {
        search = search.datetime(dt);
    }
    if let Some(limit) = args.limit {
        search = search.limit(limit);
    }
    if let Some(filter) = &args.filter {
        let filter_json: serde_json::Value = serde_json::from_str(filter)?;
        search.items.filter = Some(stac::api::Filter::Cql2Json(serde_json::from_value(
            filter_json,
        )?));
    }
    // A full JSON body overrides the typed args (whole-spec searches: sortby, fields, intersects, query, conf).
    if let Some(body) = &args.search_json {
        search = serde_json::from_str(body)?;
    }

    let max_items = args.max_items.map(|m| m as i64);
    match args.format {
        SearchFormat::Ndjson => {
            // Flat-memory streaming: rows arrive from a server-side portal and are written one at a
            // time with the shared fragment merged at serialize time — never a buffered page.
            let mut out = make_sink(&args.out)?;
            let body = serde_json::to_value(&search)?;
            let n = pool
                .stream_ndjson(body, args.token.as_deref(), max_items, &mut out)
                .await?;
            out.flush()?;
            eprintln!("search: streamed {n} item(s) as NDJSON");
        }
        SearchFormat::Itemcollection => {
            // A single keyset page assembled in Rust via ItemsClient::search (search_plan + Rust
            // band-stepping): features, next/prev tokens, links, and context all minted client-side.
            let mut out = make_sink(&args.out)?;
            if let Some(token) = &args.token {
                let _ = search
                    .additional_fields
                    .insert("token".into(), serde_json::Value::String(token.clone()));
            }
            let item_collection = ItemsClient::search(&pool, search).await?;
            let mut fc = serde_json::to_value(&item_collection)?;
            // ItemCollection serializes next/prev only into links; also surface them as top-level
            // fields (as before) so token-paging callers need not re-parse links.
            if let serde_json::Value::Object(map) = &mut fc {
                if let Some(t) = item_collection.next.as_ref().and_then(|m| m.get("token")) {
                    let _ = map.insert("next".into(), t.clone());
                }
                if let Some(t) = item_collection.prev.as_ref().and_then(|m| m.get("token")) {
                    let _ = map.insert("prev".into(), t.clone());
                }
            }
            serde_json::to_writer(&mut out, &fc)?;
            out.flush()?;
            let n = item_collection
                .number_returned
                .unwrap_or(item_collection.items.len() as u64);
            eprintln!("search: wrote 1 ItemCollection page ({n} item(s))");
        }
        SearchFormat::Geoparquet => {
            // Flat-memory streaming: items stream from the search portal into row-group batches, so the
            // item working set is one batch (0.10 registry schema), not the whole result.
            let body = serde_json::to_value(&search)?;
            let compression = args.compression.into();
            let row_group_size = args.row_group_size;
            let n = if args.out == "-" {
                // The geoparquet writer needs a Send sink and stdout's lock is not Send: encode into a
                // buffer, then write it out.
                let mut buf: Vec<u8> = Vec::new();
                let n = pool
                    .stream_geoparquet(body, max_items, compression, row_group_size, &mut buf)
                    .await?;
                std::io::stdout().write_all(&buf)?;
                n
            } else {
                let file = std::fs::File::create(&args.out)?;
                pool.stream_geoparquet(body, max_items, compression, row_group_size, file)
                    .await?
            };
            eprintln!("search: wrote geoparquet ({n} item(s))");
        }
    }
    Ok(())
}

/// Load STAC items + collections through the Rust loader. Collections are
/// created first (items reference them); then each item source is read into
/// memory, chunked into `batch_size` batches, and loaded via
/// [`PgstacPool::create_items`] (`load_items`) — so dehydration, fragment
/// splitting, and the binary COPY all run in Rust. `concurrency` batches run in
/// parallel, each on its own pooled connection.
async fn run_load(args: LoadArgs) -> Result<(), Box<dyn std::error::Error>> {
    let (collection_files, item_sources) = classify_inputs(&args.inputs)?;
    if collection_files.is_empty() && item_sources.is_empty() {
        return Err("no loadable inputs found (.parquet/.geoparquet/.ndjson/.json)".into());
    }

    let mut config = ConnectConfig::from_env();
    if let Some(dsn) = args.dsn.clone() {
        config.dsn = Some(dsn);
    }
    let pool_size = args.pool_size.unwrap_or_else(|| args.concurrency.max(4));
    let pool = PgstacPool::connect_with(
        config,
        PoolOptions {
            max_size: pool_size,
            ..Default::default()
        },
    )
    .await?;
    let policy: ConflictPolicy = args.policy.into();

    for cf in &collection_files {
        let raw: Value = serde_json::from_slice(&std::fs::read(cf)?)?;
        // A `pgstac dump` writes `{"stac": <collection>, "pgstac": {partition_trunc, fragment_config,
        // private}}`; a plain collection file is the bare STAC Collection. Unwrap the dump envelope and
        // apply its partition_trunc so a restore recreates the same partition layout (fragment_config is
        // re-derived by create_collection from item_assets).
        let (collection, partition_trunc) = match raw.get("stac") {
            Some(stac) => (
                stac.clone(),
                raw.get("pgstac")
                    .and_then(|p| p.get("partition_trunc"))
                    .and_then(|t| t.as_str())
                    .map(str::to_string),
            ),
            None => (raw, None),
        };
        let id = collection
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        // Upsert (not bare create) so load/restore are idempotent: a re-run updates the collection instead
        // of failing on the duplicate id — which is what makes `pgstac restore` resumable after an interrupt.
        // partition_trunc is applied on insert and preserved (same value) on a re-run.
        pool.upsert_collection(&collection, partition_trunc.as_deref())
            .await?;
        eprintln!("collection: {id}");
    }

    let start = Instant::now();
    let mut total = 0u64;
    let mut remaining = args.limit.unwrap_or(usize::MAX);
    for src in &item_sources {
        if remaining == 0 {
            break;
        }
        load_source_streaming(
            &pool,
            src,
            args.batch_size,
            args.concurrency,
            policy,
            &mut remaining,
            &mut total,
        )
        .await?;
        eprintln!("  {} done", src.display());
    }
    let secs = start.elapsed().as_secs_f64();
    eprintln!(
        "load complete: {total} items in {secs:.2}s ({:.0} items/s, streaming decode+load)",
        total as f64 / secs.max(1e-9),
    );
    pool.close();
    Ok(())
}

/// Restore a `pgstac dump` directory through the Rust loader. The dump layout
/// (per-collection `collection.json` + partition geoparquet) is exactly what
/// [`run_load`] consumes, so this is `load` over the dump directory with an
/// upsert policy.
async fn run_restore(args: RestoreArgs) -> Result<(), Box<dyn std::error::Error>> {
    if !args.src.is_dir() {
        return Err(format!(
            "restore source must be a dump directory: {} (extract a .tar dump first)",
            args.src.display()
        )
        .into());
    }
    run_load(LoadArgs {
        dsn: args.dsn,
        inputs: vec![args.src],
        batch_size: args.batch_size,
        concurrency: args.concurrency,
        policy: LoadPolicy::Upsert,
        pool_size: args.pool_size,
        limit: None,
    })
    .await
}

/// Run the async partition-stats maintenance:
/// recompute exact bounds + row counts for partitions ingest left dirty. Safe +
/// optional — a generous (un-tightened) envelope only over-includes a partition
/// in search, never loses rows; tightening restores tight pruning + honest
/// counts. Operators usually schedule this off-hours (pg_cron).
async fn run_maintain(args: MaintainArgs) -> Result<(), Box<dyn std::error::Error>> {
    let dsn = args.dsn.clone().unwrap_or_else(|| DEFAULT_DSN.to_string());
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    client
        .batch_execute("SET search_path = pgstac, public;")
        .await?;
    let row = client
        .query_one(
            "SELECT tighten_dirty_partition_stats($1::int)",
            &[&args.limit],
        )
        .await?;
    let tightened: i32 = row.get(0);
    eprintln!("maintain: tightened {tightened} dirty partition(s)");
    Ok(())
}

/// Delete an item (with `--item`) or a whole collection (without). Both go through the pool's
/// SECURITY DEFINER delete functions.
async fn run_delete(args: DeleteArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ConnectConfig::from_env();
    if let Some(dsn) = args.dsn.clone() {
        config.dsn = Some(dsn);
    }
    let pool = PgstacPool::connect(config).await?;
    match &args.item {
        Some(item) => {
            pool.delete_item(&args.collection, item).await?;
            eprintln!("deleted item {}/{}", args.collection, item);
        }
        None => {
            pool.delete_collection(&args.collection).await?;
            eprintln!("deleted collection {}", args.collection);
        }
    }
    pool.close();
    Ok(())
}

/// Expand the inputs (recursing into directories) and split into collection
/// files (loaded first) and item sources, each sorted for a deterministic order.
fn classify_inputs(
    inputs: &[PathBuf],
) -> Result<(Vec<PathBuf>, Vec<PathBuf>), Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for input in inputs {
        collect_files(input, &mut files)?;
    }
    let mut collections = Vec::new();
    let mut items = Vec::new();
    for f in files {
        match classify_file(&f)? {
            FileRole::Collection => collections.push(f),
            FileRole::Items => items.push(f),
            FileRole::Skip => {} // a dump's manifest/settings/queryables JSON, etc.
        }
    }
    collections.sort();
    items.sort();
    Ok((collections, items))
}

/// How an input file is loaded.
enum FileRole {
    Collection,
    Items,
    Skip,
}

/// Recursively collect loadable files (by extension) under `path`.
fn collect_files(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_dir() {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(path)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|e| e.path())
            .collect();
        paths.sort();
        for p in paths {
            collect_files(&p, out)?;
        }
    } else if path.is_file() {
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if matches!(ext, "parquet" | "geoparquet" | "ndjson" | "json") {
            out.push(path.to_path_buf());
        }
    }
    Ok(())
}

/// Classify a file. Parquet/NDJSON are always item sources. A `.json` is routed by its STAC `type`
/// (with a `collection.json` name fast-path): `Collection` → a collection, `Feature`/`FeatureCollection`
/// → items, anything else → skipped — so a dump's `manifest.json` / `settings.json` / `queryables.json`
/// are ignored rather than mis-loaded as items.
fn classify_file(path: &Path) -> Result<FileRole, Box<dyn std::error::Error>> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if matches!(ext, "parquet" | "geoparquet" | "ndjson") {
        return Ok(FileRole::Items);
    }
    if path.file_name().and_then(|n| n.to_str()) == Some("collection.json") {
        return Ok(FileRole::Collection);
    }
    let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(match value.get("type").and_then(|t| t.as_str()) {
        Some("Collection") => FileRole::Collection,
        Some("Feature") | Some("FeatureCollection") => FileRole::Items,
        _ => FileRole::Skip,
    })
}

/// A streaming batch iterator over an item source. Each yielded `Vec` is one chunk of item JSON values:
/// a stac-geoparquet **row group** (via [`stac::geoparquet::from_reader_iter`], so a multi-GB file is
/// never held in memory), a slice of NDJSON lines, or a whole `.json` file (item / ItemCollection).
type ItemBatches = Box<dyn Iterator<Item = Result<Vec<Value>, Box<dyn std::error::Error>>>>;

fn item_batches(path: &Path) -> Result<ItemBatches, Box<dyn std::error::Error>> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match ext {
        "parquet" | "geoparquet" => {
            let file = std::fs::File::open(path)?;
            let iter = stac::geoparquet::from_reader_iter(file)?.map(|batch| {
                batch?
                    .into_iter()
                    .map(|item| serde_json::to_value(item).map_err(Into::into))
                    .collect::<Result<Vec<Value>, Box<dyn std::error::Error>>>()
            });
            Ok(Box::new(iter))
        }
        "ndjson" => {
            use std::io::BufRead;
            const NDJSON_BATCH: usize = 10_000;
            let mut lines = std::io::BufReader::new(std::fs::File::open(path)?).lines();
            let iter = std::iter::from_fn(move || {
                let mut batch = Vec::with_capacity(NDJSON_BATCH);
                for line in lines.by_ref() {
                    let line = match line {
                        Ok(line) => line,
                        Err(e) => return Some(Err(e.into())),
                    };
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str(&line) {
                        Ok(value) => batch.push(value),
                        Err(e) => return Some(Err(e.into())),
                    }
                    if batch.len() >= NDJSON_BATCH {
                        return Some(Ok(batch));
                    }
                }
                (!batch.is_empty()).then_some(Ok(batch))
            });
            Ok(Box::new(iter))
        }
        "json" => {
            let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
            let items = match value.get("features").and_then(|f| f.as_array()) {
                Some(features) => features.clone(),
                None => vec![value],
            };
            Ok(Box::new(std::iter::once(Ok(items))))
        }
        _ => Err(format!("unsupported input extension: {}", path.display()).into()),
    }
}

/// Parallel ndjson decode: split the file into `n` byte ranges and spawn one decoder thread per range.
/// Each thread parses only the lines whose START offset falls in its range — boundary-aligned (a thread
/// discards the partial line straddling its start, UNLESS its start sits exactly on a line boundary, and
/// fully consumes the line straddling its end) so no line is dropped or double-decoded. Small files use a
/// single range (no alignment). All threads feed the shared `tx`. This parallelizes the serde parse, the
/// single-thread decode ceiling. Order is not preserved (fine for load).
fn spawn_ndjson_range_decoders(
    path: &Path,
    n: usize,
    tx: tokio::sync::mpsc::Sender<Result<(Vec<Value>, usize), String>>,
) -> std::io::Result<Vec<std::thread::JoinHandle<()>>> {
    use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
    const BATCH: usize = 10_000;
    // Also flush at a byte budget, not just an item count: the channel holds up to `concurrency` batches and
    // each decoder builds one, so the in-flight set is bounded by (decoders + channel slots) * BYTE_CAP
    // regardless of item size OR file size. Without this, big STAC items (~60 KB each as a parsed Value) at
    // 10k/batch across an 8-slot channel sustained ~12 GB on a full multi-GB load and OOM-killed it.
    const BYTE_CAP: usize = 8 * 1024 * 1024;
    let size = std::fs::metadata(path)?.len();
    let n = (n.max(1)) as u64;
    // Sharding a small file buys nothing (a sub-MB decode is instant) and only invites the line-boundary
    // edge cases below, so cap to ~one decoder per MB; tiny files fall back to a single range.
    let n = n.min((size / (1u64 << 20)).max(1));
    let mut handles = Vec::with_capacity(n as usize);
    for i in 0..n {
        let start = i * size / n;
        let end = (i + 1) * size / n;
        let path = path.to_path_buf();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let f = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e.to_string()));
                    return;
                }
            };
            let mut reader = BufReader::with_capacity(256 * 1024, f);
            let mut consumed = start;
            if i > 0 {
                // Align to a line boundary. The line straddling `start` belongs to the previous range, so
                // discard it — UNLESS `start` already sits on a line boundary (the byte before it is '\n'),
                // in which case the line AT `start` is ours and skipping it would silently drop a record (a
                // shard boundary landing exactly on a newline).
                if let Err(e) = reader.seek(SeekFrom::Start(start - 1)) {
                    let _ = tx.blocking_send(Err(e.to_string()));
                    return;
                }
                let mut prev = [0u8; 1];
                let on_boundary = match reader.read_exact(&mut prev) {
                    Ok(()) => prev[0] == b'\n',
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e.to_string()));
                        return;
                    }
                };
                if !on_boundary {
                    let mut junk = Vec::new();
                    match reader.read_until(b'\n', &mut junk) {
                        Ok(nr) => consumed += nr as u64,
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e.to_string()));
                            return;
                        }
                    }
                }
            }
            let mut batch = Vec::with_capacity(BATCH);
            let mut batch_bytes = 0usize;
            let mut line = String::new();
            loop {
                if consumed >= end {
                    break; // the next range owns lines starting at/after `end`
                }
                line.clear();
                let nr = match reader.read_line(&mut line) {
                    Ok(nr) => nr,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e.to_string()));
                        return;
                    }
                };
                if nr == 0 {
                    break; // EOF
                }
                consumed += nr as u64;
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    match serde_json::from_str::<Value>(trimmed) {
                        Ok(v) => batch.push(v),
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e.to_string()));
                            return;
                        }
                    }
                    batch_bytes += trimmed.len();
                    if batch.len() >= BATCH || batch_bytes >= BYTE_CAP {
                        let sent = std::mem::replace(&mut batch, Vec::with_capacity(BATCH));
                        if tx.blocking_send(Ok((sent, batch_bytes))).is_err() {
                            return; // receiver gone
                        }
                        batch_bytes = 0;
                    }
                }
            }
            if !batch.is_empty() {
                let _ = tx.blocking_send(Ok((batch, batch_bytes)));
            }
        }));
    }
    Ok(handles)
}

/// Stream one source into the loader. Decoding (the blocking parquet/geoarrow work) runs on a dedicated
/// thread feeding a **bounded channel**; the async side re-chunks to `batch_size` and keeps up to
/// `concurrency` COPY batches in flight (each on its own pooled connection). So CPU decode overlaps with
/// the DB loads, and the bounded channel + bounded in-flight set keep memory flat regardless of file
/// size (decode blocks when the loaders fall behind). Stops after `remaining` items; adds rows to `total`.
async fn load_source_streaming(
    pool: &PgstacPool,
    src: &Path,
    batch_size: usize,
    concurrency: usize,
    policy: ConflictPolicy,
    remaining: &mut usize,
    total: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch_size = batch_size.max(1);
    let concurrency = concurrency.max(1);
    // Cap the decoded-item bytes held per in-flight load task. Large STAC items (e.g. landsat with many asset
    // bands) cost ~6x their text size once parsed + dehydrated + COPY-encoded, so item-count batching alone
    // (batch_size * concurrency) can balloon to tens of GB and OOM a multi-GB ndjson load. Tunable via
    // PGSTAC_LOAD_BYTES (MB); only ndjson decoders report byte sizes, so other formats batch by count.
    let load_bytes = std::env::var("PGSTAC_LOAD_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(64)
        .saturating_mul(1024 * 1024);

    // Decoder thread(s): build + drain the (non-Send) batch iterator here, sending only the Send
    // `Vec<Value>` batches across. The bounded channel applies backpressure (decode pauses when loaders
    // are behind). PGSTAC_DECODE_THREADS>1 shards an ndjson file across N parallel range decoders (by
    // byte range) or a stac-geoparquet file across N decoders (by row group) to lift the single-thread
    // decode ceiling; other formats (plain .json) keep the single decoder.
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<Result<(Vec<Value>, usize), String>>(concurrency.max(2));
    let path = src.to_path_buf();
    let decode_threads = std::env::var("PGSTAC_DECODE_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(default_ingest_parallelism)
        .max(1);
    let ext = path.extension().and_then(|e| e.to_str());
    let is_ndjson = ext == Some("ndjson");
    let is_parquet = matches!(ext, Some("parquet") | Some("geoparquet"));
    let decoders: Vec<std::thread::JoinHandle<()>> = if is_ndjson && decode_threads > 1 {
        spawn_ndjson_range_decoders(&path, decode_threads, tx)?
    } else if is_parquet && decode_threads > 1 {
        // Parquet decode was the single-thread ingest bottleneck (much slower than ndjson). Shard the
        // file's row groups across N decoder threads; each reports per-batch byte estimates so the
        // load loop's PGSTAC_LOAD_BYTES budget applies to parquet too.
        pgstac::parquet_decode::spawn_parquet_decoders(&path, decode_threads, tx)?
    } else {
        vec![std::thread::spawn(move || match item_batches(&path) {
            Ok(batches) => {
                for batch in batches {
                    // Non-ndjson decoders don't track byte size; send 0 so the load loop batches them by
                    // item count only (the byte budget is an ndjson-streaming safeguard).
                    let msg = batch.map(|b| (b, 0usize)).map_err(|e| e.to_string());
                    if tx.blocking_send(msg).is_err() {
                        break; // receiver gone (early stop / error)
                    }
                }
            }
            Err(e) => {
                let _ = tx.blocking_send(Err(e.to_string()));
            }
        })]
    };

    // Single ingest path: create_items (staging + flush), which resolves every conflict policy (a raw
    // COPY can't).

    let mut set: tokio::task::JoinSet<Result<u64, pgstac::Error>> = tokio::task::JoinSet::new();
    let mut pending: Vec<Value> = Vec::with_capacity(batch_size);
    let mut pending_bytes = 0usize;
    loop {
        let msg = rx.recv().await;
        let Some(msg) = msg else { break };
        if *remaining == 0 {
            break;
        }
        let (mut batch, bytes) = msg.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        if batch.len() > *remaining {
            batch.truncate(*remaining);
        }
        *remaining -= batch.len();
        pending.append(&mut batch);
        pending_bytes += bytes;
        // Flush a task when pending reaches batch_size items OR the byte budget — the byte budget bounds the
        // in-flight working set (concurrency * chunk) for large items, which otherwise OOMs a big ndjson load.
        if pending.len() >= batch_size || (pending_bytes >= load_bytes && !pending.is_empty()) {
            while set.len() >= concurrency {
                let r = set.join_next().await.expect("set is non-empty");
                *total += r??;
            }
            let chunk = std::mem::replace(&mut pending, Vec::with_capacity(batch_size));
            pending_bytes = 0;
            let pool = pool.clone();
            set.spawn(async move { pool.create_items(chunk, policy).await });
        }
    }
    if !pending.is_empty() {
        let pool = pool.clone();
        set.spawn(async move { pool.create_items(pending, policy).await });
    }
    while let Some(res) = set.join_next().await {
        *total += res??;
    }
    // Unblock the decoder(s) if we stopped early (channel full), then join them.
    rx.close();
    for decoder in decoders {
        let _ = decoder.join();
    }
    Ok(())
}

/// Dry-run: report the plan (collections, partitions, est. items)
/// without writing data.
async fn dry_run(
    client: &tokio_postgres::Client,
    config: &DumpConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let plans = pgstac::export::plan::plan_collections(
        client,
        config.collection_ids.as_deref(),
        &config.prefilter,
    )
    .await?;
    let mut total_parts = 0usize;
    let mut est_items = 0f64;
    println!("dry-run plan:");
    for plan in &plans {
        let parts = plan.partitions.len();
        total_parts += parts;
        let coll_est: f64 = plan.partitions.iter().map(|p| p.reltuples as f64).sum();
        est_items += coll_est;
        println!(
            "  {} ({:?}): {} partitions, ~{} items",
            plan.id, plan.partition_trunc, parts, coll_est as u64
        );
    }
    println!(
        "total: {} collections, {} partitions, ~{} items (estimated from reltuples)",
        plans.len(),
        total_parts,
        est_items as u64
    );
    Ok(())
}

fn is_tar_path(out: &str) -> bool {
    out.ends_with(".tar") || out.ends_with(".tar.zst")
}

/// Returns an object_store URL if `out` looks like a remote/object-store URL.
fn object_store_url(out: &str) -> Option<String> {
    for scheme in ["s3://", "gs://", "az://", "azure://", "file://"] {
        if out.starts_with(scheme) {
            return Some(out.to_string());
        }
    }
    None
}

/// Loads + verifies a prior `_checkpoint.json` from a directory dump for resume
///. Returns the verified completed entries keyed by relative path: a file
/// is included only if it exists on disk AND matches its recorded SHA-256; a
/// missing or mismatched file is left out (it is redone whole). Absent checkpoint
/// => empty map (fresh dump). A finished dump (manifest present, no checkpoint)
/// also yields an empty map, so a re-run is idempotent.
fn load_resume_set(
    dir: &str,
) -> std::collections::HashMap<String, pgstac::export::manifest::CheckpointEntry> {
    use pgstac::export::manifest::{Checkpoint, sha256_hex};
    let root = std::path::Path::new(dir);
    let cp_path = root.join("_checkpoint.json");
    let Ok(bytes) = std::fs::read(&cp_path) else {
        return Default::default();
    };
    let Ok(cp): Result<Checkpoint, _> = serde_json::from_slice(&bytes) else {
        eprintln!("warning: ignoring unreadable _checkpoint.json");
        return Default::default();
    };
    let mut verified = std::collections::HashMap::new();
    let mut skipped = 0usize;
    for entry in &cp.completed {
        let p = root.join(&entry.file);
        match std::fs::read(&p) {
            Ok(b) if sha256_hex(&b) == entry.sha256 => {
                let _ = verified.insert(entry.file.clone(), entry.clone());
            }
            _ => skipped += 1,
        }
    }
    if !verified.is_empty() || skipped > 0 {
        eprintln!(
            "resume: {} completed partition file(s) verified and will be skipped{}",
            verified.len(),
            if skipped > 0 {
                format!("; {skipped} missing/mismatched will be redone")
            } else {
                String::new()
            }
        );
    }
    verified
}
