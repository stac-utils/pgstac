//! Loading dehydrated item rows into pgstac via binary COPY.
//!
//! The Rust loader binary-COPYs fully-dehydrated rows ([`DehydratedRow`]) into a session-local staging
//! table shaped like `items` (created by the SQL `make_binary_staging`), then calls
//! `flush_items_staging_binary` to move them into `items` (the only path that writes `items` — direct writes
//! are revoked from `pgstac_ingest`). This module owns the client-side binary encoding of a row.
//!
//! Geometry is sent as raw EWKB through [`WkbGeometry`]: the PostGIS `geometry` binary wire format *is*
//! EWKB, so the bytes [`dehydrate`](crate::dehydrate) already produced go out untouched.

use crate::Result;
use crate::canonical::jsonb_hash;
use crate::dehydrate::{DehydrateSchema, DehydratedRow, PromotedValue, dehydrate};
use crate::field_registry::FieldRegistry;
use crate::fragment::{FragmentConfig, build_fragment_payload, strip_fragment_col};
use bytes::BytesMut;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use futures::pin_mut;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

/// EWKB bytes wrapped so they encode as a PostGIS `geometry` on a binary COPY.
///
/// The geometry binary wire format is EWKB, so the bytes are written verbatim.
#[derive(Debug, Clone)]
pub struct WkbGeometry(pub Vec<u8>);

impl ToSql for WkbGeometry {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn StdError + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "geometry"
    }

    to_sql_checked!();
}

impl PromotedValue {
    /// Boxes the inner value as a trait object for the binary COPY writer. `Send` so the loader's futures
    /// (which hold these across the COPY await) are `Send` and can be `tokio::spawn`ed under concurrency.
    fn into_boxed(self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            PromotedValue::Text(v) => Box::new(v),
            PromotedValue::Float(v) => Box::new(v),
            PromotedValue::Int(v) => Box::new(v),
            PromotedValue::BigInt(v) => Box::new(v),
            PromotedValue::TextArray(v) => Box::new(v),
            PromotedValue::Jsonb(v) => Box::new(v),
            PromotedValue::Timestamptz(v) => Box::new(v),
        }
    }
}

/// The fixed (non-promoted) staging columns, in the exact order [`row_boxes`] pushes them. A staging
/// column not listed here and not a promoted column COPYs as SQL NULL (the sentinel slot).
const FIXED_COLUMNS: [&str; 18] = [
    "id",
    "geometry",
    "collection",
    "datetime",
    "end_datetime",
    "datetime_is_range",
    "stac_version",
    "stac_extensions",
    "pgstac_updated_at",
    "item_hash",
    "fragment_id",
    "bbox",
    "links",
    "assets",
    "properties",
    "extra",
    "link_hrefs",
    "private",
];

/// Marshals a [`DehydratedRow`] into binary-COPY trait objects in a **fixed positional order** (the 18
/// [`FIXED_COLUMNS`], then the promoted columns in `schema` order, then one trailing NULL sentinel),
/// consuming the row (fields are moved into the boxes — no deep clones, and no per-row map or key
/// strings; [`copy_rows`] reorders these into staging-column order via a permutation computed once).
fn row_boxes(row: DehydratedRow, n_promoted: usize) -> Vec<Box<dyn ToSql + Sync + Send>> {
    let mut v: Vec<Box<dyn ToSql + Sync + Send>> =
        Vec::with_capacity(FIXED_COLUMNS.len() + n_promoted + 1);
    v.push(Box::new(row.id));
    v.push(Box::new(WkbGeometry(row.geometry)));
    v.push(Box::new(row.collection));
    v.push(Box::new(row.datetime));
    v.push(Box::new(row.end_datetime));
    v.push(Box::new(row.datetime_is_range));
    v.push(Box::new(row.stac_version));
    v.push(Box::new(row.stac_extensions));
    v.push(Box::new(Utc::now()));
    v.push(Box::new(row.item_hash.to_vec()));
    v.push(Box::new(row.fragment_id));
    v.push(Box::new(row.bbox));
    v.push(Box::new(row.links));
    v.push(Box::new(row.assets));
    v.push(Box::new(row.properties));
    v.push(Box::new(row.extra));
    v.push(Box::new(row.link_hrefs));
    v.push(Box::new(Option::<Value>::None)); // private
    for value in row.promoted {
        v.push(value.into_boxed());
    }
    v.push(Box::new(Option::<Value>::None)); // NULL sentinel for unmapped staging columns
    v
}

/// Binary-COPYs `rows` into the staging table `staging` (a session-local table shaped like `items`).
///
/// Reads the staging table's column names + types once, then writes each row in column order. Returns
/// the number of rows written. Takes a transaction because the staging table is session-local
/// (`ON COMMIT DROP`), so the COPY must share the transaction that created it.
pub async fn copy_rows(
    tx: &tokio_postgres::Transaction<'_>,
    staging: &str,
    rows: Vec<DehydratedRow>,
    schema: &DehydrateSchema,
) -> Result<u64> {
    let describe = tx
        .prepare(&format!("SELECT * FROM {staging} WHERE false"))
        .await?;
    let types: Vec<Type> = describe
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect();

    // Permutation computed ONCE: for each staging column, the index into the positional `row_boxes`
    // vector that fills it (fixed columns 0..18, promoted 18.., NULL sentinel last). Unmapped staging
    // columns point at the sentinel and COPY as SQL NULL. No per-row map or column-name strings.
    let n_promoted = schema.columns().len();
    let null_idx = FIXED_COLUMNS.len() + n_promoted;
    let mut name_to_box: HashMap<&str, usize> = HashMap::with_capacity(null_idx);
    for (i, name) in FIXED_COLUMNS.iter().enumerate() {
        let _ = name_to_box.insert(name, i);
    }
    for (j, col) in schema.columns().iter().enumerate() {
        let _ = name_to_box.insert(col.column.as_str(), FIXED_COLUMNS.len() + j);
    }
    let perm: Vec<usize> = describe
        .columns()
        .iter()
        .map(|c| name_to_box.get(c.name()).copied().unwrap_or(null_idx))
        .collect();

    let sink = tx
        .copy_in(&format!("COPY {staging} FROM STDIN WITH (FORMAT BINARY)"))
        .await?;
    let writer = BinaryCopyInWriter::new(sink, &types);
    pin_mut!(writer);

    for row in rows {
        let boxes = row_boxes(row, n_promoted);
        // Reorder into staging-column order via the precomputed permutation. Drop the `Send` bound for
        // the writer's `&[&(dyn ToSql + Sync)]` signature (implicit coercion).
        let params: Vec<&(dyn ToSql + Sync)> = perm
            .iter()
            .map(|&i| -> &(dyn ToSql + Sync) { &*boxes[i] })
            .collect();
        writer.as_mut().write(&params).await?;
    }
    Ok(writer.finish().await?)
}

/// Whether a failed DB call is worth retrying: transient contention (deadlock, serialization failure,
/// lock-not-available, object-in-use) from many writers racing to CREATE + widen the same partitions.
/// Determinate errors (bad data, unique violation, missing collection, …) are NOT retried.
fn is_retryable_db(err: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    match err.as_db_error() {
        Some(db) => matches!(
            *db.code(),
            SqlState::T_R_DEADLOCK_DETECTED
                | SqlState::T_R_SERIALIZATION_FAILURE
                | SqlState::LOCK_NOT_AVAILABLE
                | SqlState::OBJECT_IN_USE
        ),
        None => false,
    }
}

/// How `flush_items_staging_binary` resolves an id collision with an existing item.
///
/// ORPHAN CAVEAT: `Ignore` and `Upsert` only touch the partition the *incoming* item routes to. If a
/// datetime change moves an item to a DIFFERENT partition, the old row in its former partition is left
/// behind as an orphan (a duplicate `(collection, id)` across partitions). Use `Delsert` when updates may
/// move an item between partitions; it deletes the old row wherever it lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Fail the whole load if any item id already exists.
    Error,
    /// Replace a changed item IN THE PARTITION IT ROUTES TO (fast: the delete is window-pruned to that one
    /// partition). A datetime change that moves the item to another partition orphans the old row — use
    /// `Delsert` for that case.
    Upsert,
    /// Replace a changed item wherever it lives: delete the old row CROSS-partition (by collection+id) first,
    /// then insert. Move-safe, but the delete probes every partition for the id.
    Delsert,
    /// Skip items whose id already exists (insert-if-absent). Like `Upsert`, a cross-partition move orphans
    /// the old row.
    Ignore,
}

impl ConflictPolicy {
    /// The SQL token `flush_items_staging_binary` expects.
    fn as_sql(self) -> &'static str {
        match self {
            ConflictPolicy::Error => "error",
            ConflictPolicy::Upsert => "upsert",
            ConflictPolicy::Delsert => "delsert",
            ConflictPolicy::Ignore => "ignore",
        }
    }
}

/// Loads a batch of full STAC items into pgstac via the Rust dehydrate + binary-COPY path.
///
/// For each item: dehydrate it; for a collection with a `fragment_config`, split out the shared fragment
/// (upserted + deduped via `ensure_fragments`) and strip the fragment-owned keys from the row; ensure its
/// partition exists and its stats cover it (`ensure_partitions`, committed up front); then
/// binary-COPY all rows into a session-local staging table and `flush_items_staging_binary` them into
/// `items` resolving id collisions by `policy`. Matches `items_staging_dehydrate` for both the no-fragment
/// and fragment paths. Returns the number of rows flushed. Collections must already exist.
pub async fn load_items(
    client: &mut tokio_postgres::Client,
    items: Vec<Value>,
    schema: &DehydrateSchema,
    policy: ConflictPolicy,
) -> Result<u64> {
    if items.is_empty() {
        return Ok(0);
    }

    // Env-gated phase profiling (PGSTAC_LOAD_PROFILE): splits Rust CPU work (dehydrate/hash/fragment
    // payload) from the DB-side calls (ensure_partitions/stamp/COPY/flush/commit). Off by default.
    let prof = std::env::var_os("PGSTAC_LOAD_PROFILE").is_some();
    let ms = |since: std::time::Instant| since.elapsed().as_secs_f64() * 1000.0;
    let mark = std::time::Instant::now();

    // Per-collection fragment_config for any fragment-enabled collection in the batch (empty/absent =
    // the no-fragment fast path).
    let configs = fetch_fragment_configs(client, &items).await?;
    let (t_configs, mark) = (ms(mark), std::time::Instant::now());

    // Dehydrate each item; for a fragment collection, compute its fragment payload from the ORIGINAL item
    // (before dehydrate consumes it) and strip the fragment-owned keys from the dehydrated row.
    let mut rows: Vec<DehydratedRow> = Vec::with_capacity(items.len());
    let mut payloads: Vec<Option<Value>> = Vec::with_capacity(items.len());
    let mut field_registry: HashMap<String, FieldRegistry> = HashMap::new();
    for item in items {
        let collection = item.get("collection").and_then(Value::as_str);
        // Field registry: walk the FULL item content (every item, no sampling) into the per-collection
        // registry, flushed below as an add-only widen so it stays a superset of every field the items carry.
        if let Some(coll) = collection {
            field_registry
                .entry(coll.to_string())
                .or_default()
                .observe(&item);
        }
        let config = collection
            .and_then(|collection| configs.get(collection))
            .filter(|config| !config.is_empty());
        let payload = config.and_then(|config| build_fragment_payload(&item, config));
        let mut row = dehydrate(item, schema)?;
        if let Some(config) = config {
            apply_fragment_split(&mut row, config);
        }
        rows.push(row);
        payloads.push(payload);
    }
    let (t_dehydrate, mark) = (ms(mark), std::time::Instant::now());

    // Create + widen every partition the batch lands in BEFORE the load. Bucket client-side
    // by (collection, partition window) and call prepare_partition_for_load once per partition — per-partition
    // metadata, O(#partitions), NOT the per-item batch-length arrays the old `ensure_partitions` shipped just
    // to group them server-side. Each call widens dt/edt + the real SPATIAL envelope (`ensure_partitions`
    // passed NULL) AND bumps n by this partition's staged count, so the flush writes only `items`, never
    // partition_stats. Over-counting n is safe (ignore/upsert may insert fewer); the async tightener resets it.
    let mut seen_collections: HashSet<&str> = HashSet::new();
    let distinct_collections: Vec<&str> = rows
        .iter()
        .map(|r| r.collection.as_str())
        .filter(|c| seen_collections.insert(*c))
        .collect();
    let truncs: HashMap<String, Option<String>> = client
        .query(
            "SELECT id, partition_trunc FROM collections WHERE id = ANY($1)",
            &[&distinct_collections],
        )
        .await?
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect();
    let mut aggs: HashMap<(String, i64), PartitionAgg> = HashMap::new();
    for row in &rows {
        let trunc = truncs.get(&row.collection).and_then(Option::as_deref);
        let (xmin, ymin, xmax, ymax) = bbox_envelope(&row.bbox);
        let agg = aggs
            .entry((row.collection.clone(), window_key(row.datetime, trunc)))
            .or_insert_with(|| PartitionAgg {
                collection: row.collection.clone(),
                dt_lo: row.datetime,
                dt_hi: row.datetime,
                edt_lo: row.end_datetime,
                edt_hi: row.end_datetime,
                xmin,
                ymin,
                xmax,
                ymax,
                count: 0,
            });
        agg.dt_lo = agg.dt_lo.min(row.datetime);
        agg.dt_hi = agg.dt_hi.max(row.datetime);
        agg.edt_lo = agg.edt_lo.min(row.end_datetime);
        agg.edt_hi = agg.edt_hi.max(row.end_datetime);
        agg.xmin = agg.xmin.min(xmin);
        agg.ymin = agg.ymin.min(ymin);
        agg.xmax = agg.xmax.max(xmax);
        agg.ymax = agg.ymax.max(ymax);
        agg.count += 1;
    }
    for agg in aggs.values() {
        // Many writers race to CREATE + widen the same partitions; concurrent partition DDL can transiently
        // deadlock / serialization-fail / hit lock-not-available. Retry those a few times with a small backoff;
        // determinate errors propagate.
        let mut attempt = 0u32;
        loop {
            match client
                .execute(
                    "SELECT prepare_partition_for_load($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    &[
                        &agg.collection,
                        &agg.dt_lo,
                        &agg.dt_hi,
                        &agg.edt_lo,
                        &agg.edt_hi,
                        &agg.xmin,
                        &agg.ymin,
                        &agg.xmax,
                        &agg.ymax,
                        &agg.count,
                    ],
                )
                .await
            {
                Ok(_) => break,
                Err(e) if attempt < 5 && is_retryable_db(&e) => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20 * u64::from(attempt)))
                        .await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    let (t_partitions, mark) = (ms(mark), std::time::Instant::now());

    // Add-only UPSERT of the COMPLETE per-collection path set walked above, so item_field_registry stays a
    // superset of every field the items carry. Direct INSERT ... ON CONFLICT (no SECURITY DEFINER function) —
    // pgstac_ingest keeps INSERT/UPDATE on item_field_registry (DELETE/TRUNCATE revoked, so this can only
    // widen, never narrow). Retried on transient contention, since concurrent writers UPSERT the same rows.
    for (collection, registry) in &field_registry {
        if registry.is_empty() {
            continue;
        }
        let entries = registry.to_entries();
        let mut attempt = 0u32;
        loop {
            match client
                .execute(
                    "INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen) \
                     SELECT $1, e->>'path', (e->>'is_leaf')::boolean, \
                            ARRAY(SELECT jsonb_array_elements_text(e->'value_kinds')), now(), now() \
                     FROM jsonb_array_elements($2::jsonb) AS e \
                     ON CONFLICT (collection, path) DO UPDATE SET \
                         is_leaf = item_field_registry.is_leaf AND EXCLUDED.is_leaf, \
                         value_kinds = (SELECT array_agg(DISTINCT v) \
                                        FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)), \
                         last_seen = now()",
                    &[collection, &entries],
                )
                .await
            {
                Ok(_) => break,
                Err(e) if attempt < 5 && is_retryable_db(&e) => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20 * u64::from(attempt))).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    let (t_registry, mark) = (ms(mark), std::time::Instant::now());

    // Upsert + dedup the fragments and stamp each row's fragment_id, committed before the load (the
    // items COPY references fragment_id; the fragments must already exist).
    stamp_fragment_ids(client, &mut rows, &payloads).await?;
    let (t_stamp, mark) = (ms(mark), std::time::Instant::now());

    // P5: staging + COPY + flush in one transaction (the staging table is ON COMMIT DROP).
    let tx = client.transaction().await?;
    let staging: String = tx
        .query_one("SELECT make_binary_staging()", &[])
        .await?
        .get(0);
    let (t_staging, mark) = (ms(mark), std::time::Instant::now());
    let _ = copy_rows(&tx, &staging, rows, schema).await?;
    let (t_copy, mark) = (ms(mark), std::time::Instant::now());
    let flushed: i64 = tx
        .query_one(
            "SELECT flush_items_staging_binary($1, $2)",
            &[&staging, &policy.as_sql()],
        )
        .await?
        .get(0);
    let (t_flush, mark) = (ms(mark), std::time::Instant::now());
    tx.commit().await?;
    let t_commit = ms(mark);

    if prof {
        eprintln!(
            "LOADPROF rows={flushed} | RUST dehydrate={t_dehydrate:.1} | DB configs={t_configs:.1} \
             ensure_part={t_partitions:.1} registry={t_registry:.1} stamp_frag={t_stamp:.1} staging={t_staging:.1} \
             copy={t_copy:.1} flush={t_flush:.1} commit={t_commit:.1} (ms)"
        );
    }
    Ok(u64::try_from(flushed)?)
}

/// Per-partition aggregate accumulated in pass 1 (extent + count), used to widen partition_stats BEFORE
/// the COPY (golden rule: stats are at least as wide as the data before any data lands).
struct PartitionAgg {
    collection: String,
    dt_lo: DateTime<Utc>,
    dt_hi: DateTime<Utc>,
    edt_lo: DateTime<Utc>,
    edt_hi: DateTime<Utc>,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
    count: i64,
}

/// The partition-window start (epoch seconds) a datetime falls in, for client-side bucketing. Matches the
/// server's UTC `date_trunc` (the cluster runs UTC). A NULL `partition_trunc` collapses to one bucket per
/// collection (sentinel `i64::MIN`).
fn window_key(dt: DateTime<Utc>, trunc: Option<&str>) -> i64 {
    let start = match trunc {
        Some("month") => Utc
            .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
            .single(),
        Some("year") => Utc.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0).single(),
        _ => return i64::MIN,
    };
    start.map(|d| d.timestamp()).unwrap_or(i64::MIN)
}

/// The `(xmin, ymin, xmax, ymax)` envelope of a STAC bbox (2D `[w,s,e,n]` or 3D `[w,s,zmin,e,n,zmax]`).
/// A missing/odd bbox falls back to the whole world (over-wide is the safe direction for the widen).
fn bbox_envelope(bbox: &Option<Value>) -> (f64, f64, f64, f64) {
    let nums: Option<Vec<f64>> = bbox
        .as_ref()
        .and_then(|b| b.as_array())
        .map(|a| a.iter().filter_map(Value::as_f64).collect());
    match nums.as_deref() {
        Some([xmin, ymin, xmax, ymax]) => (*xmin, *ymin, *xmax, *ymax),
        Some([xmin, ymin, _zmin, xmax, ymax, _zmax]) => (*xmin, *ymin, *xmax, *ymax),
        _ => (-180.0, -90.0, 180.0, 90.0),
    }
}

/// Per-collection `partition_trunc` for the collections referenced in `items`.
async fn fetch_partition_truncs(
    client: &tokio_postgres::Client,
    items: &[Value],
) -> Result<HashMap<String, Option<String>>> {
    let mut seen = HashSet::new();
    let collections: Vec<&str> = items
        .iter()
        .filter_map(|item| item.get("collection").and_then(Value::as_str))
        .filter(|c| seen.insert(*c))
        .collect();
    let rows = client
        .query(
            "SELECT id, partition_trunc FROM collections WHERE id = ANY($1)",
            &[&collections],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1)))
        .collect())
}

/// The partition window `[start, end)` a datetime lands in, or `None` for a non-partitioned (single
/// partition) collection. Mirrors [`window_key`] / the server's UTC `date_trunc`, so the client can prune a
/// precheck probe to exactly one partition.
fn window_bounds(dt: DateTime<Utc>, trunc: Option<&str>) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    match trunc {
        Some("month") => {
            let start = Utc
                .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
                .single()?;
            let (ey, em) = if dt.month() == 12 {
                (dt.year() + 1, 1)
            } else {
                (dt.year(), dt.month() + 1)
            };
            let end = Utc.with_ymd_and_hms(ey, em, 1, 0, 0, 0).single()?;
            Some((start, end))
        }
        Some("year") => {
            let start = Utc.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0).single()?;
            let end = Utc
                .with_ymd_and_hms(dt.year() + 1, 1, 1, 0, 0, 0)
                .single()?;
            Some((start, end))
        }
        _ => None,
    }
}

/// A STAC item's nominal datetime (`properties.datetime`, falling back to `start_datetime`) as UTC.
fn item_datetime(item: &Value) -> Result<DateTime<Utc>> {
    let props = item.get("properties");
    let raw = props
        .and_then(|p| p.get("datetime"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            props
                .and_then(|p| p.get("start_datetime"))
                .and_then(Value::as_str)
        })
        .ok_or_else(|| crate::Error::Dehydrate("item missing datetime".to_string()))?;
    Ok(DateTime::parse_from_rfc3339(raw)
        .map_err(|e| crate::Error::Dehydrate(format!("unparseable datetime {raw:?}: {e}")))?
        .with_timezone(&Utc))
}

/// One partition's worth of precheck input: the original item indices, ids and canonical hashes that landed
/// in this partition window, a representative datetime (to locate the partition), and the window bounds used
/// to prune the probe to that one partition.
struct PrecheckBucket {
    collection: String,
    repr_dt: DateTime<Utc>,
    bounds: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ords: Vec<i32>,
    ids: Vec<String>,
    hashes: Vec<Vec<u8>>,
}

static PRECHECK_TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// EXPERIMENTAL precheck-driven upsert/ignore — per-partition, parallel, adaptive, with NO per-item SQL
/// function arguments. A cheap pass (canonical hash + datetime parse, no full dehydrate) buckets the item
/// indices by partition window. Per partition, in parallel, [`precheck_one_partition`] decides which items
/// are unchanged (and can be SKIPPED — the re-ingest / sync win) vs new/changed; the survivors go through
/// the normal [`crate::PgstacPool::create_items`] load (which resolves any same-partition conflict per
/// `policy`).
///
/// Matching on id (not exact datetime) catches a datetime change that stays WITHIN a partition. A
/// cross-partition move reads as 'new' (its old row is in another partition); with `Ignore` (or a future
/// same-partition upsert) that orphans the old row, while the current `Upsert` flush deletes it
/// cross-partition. Returns `(unchanged_skipped, loaded)`.
pub async fn precheck_upsert(
    pool: &crate::PgstacPool,
    items: Vec<Value>,
    policy: ConflictPolicy,
) -> Result<(u64, u64)> {
    if items.is_empty() {
        return Ok((0, 0));
    }
    let meta = pool.get().await?;
    let truncs = fetch_partition_truncs(&meta, &items).await?;
    drop(meta);

    // Cheap pass: (id, canonical hash, datetime) per item, bucketed by (collection, partition window).
    let mut buckets: HashMap<(String, i64), PrecheckBucket> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        let collection = item
            .get("collection")
            .and_then(Value::as_str)
            .ok_or_else(|| crate::Error::Dehydrate("item missing collection".to_string()))?;
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| crate::Error::Dehydrate("item missing id".to_string()))?;
        let dt = item_datetime(item)?;
        let trunc = truncs.get(collection).and_then(Option::as_deref);
        let bucket = buckets
            .entry((collection.to_string(), window_key(dt, trunc)))
            .or_insert_with(|| PrecheckBucket {
                collection: collection.to_string(),
                repr_dt: dt,
                bounds: window_bounds(dt, trunc),
                ords: Vec::new(),
                ids: Vec::new(),
                hashes: Vec::new(),
            });
        bucket
            .ords
            .push(i32::try_from(i).expect("batch index fits i32"));
        bucket.ids.push(id.to_string());
        bucket.hashes.push(jsonb_hash(item).to_vec());
    }

    // Classify each partition's bucket in parallel -> (ords to load, unchanged count).
    let mut set: tokio::task::JoinSet<Result<(Vec<i32>, u64)>> = tokio::task::JoinSet::new();
    for (_key, bucket) in buckets {
        let pool = pool.clone();
        let _ = set.spawn(async move { precheck_one_partition(&pool, bucket).await });
    }
    let mut load_ords: HashSet<i32> = HashSet::new();
    let mut unchanged: u64 = 0;
    while let Some(joined) = set.join_next().await {
        let (ords, unch) = joined.expect("precheck task panicked")?;
        load_ords.extend(ords);
        unchanged += unch;
    }

    // Load only new + changed through the normal path (which resolves same-partition conflicts per policy).
    let to_load: Vec<Value> = items
        .into_iter()
        .enumerate()
        .filter_map(|(i, item)| {
            load_ords
                .contains(&i32::try_from(i).expect("batch index fits i32"))
                .then_some(item)
        })
        .collect();
    let loaded = pool.create_items(to_load, policy).await?;
    Ok((unchanged, loaded))
}

/// Classify one partition's bucket: returns the ords (original item indices) to LOAD (new + changed) and the
/// count of UNCHANGED items skipped. Picks the cheaper side: empty/just-created partition -> all new (no
/// probe); batch bigger than the partition -> pull the partition's `(id, item_hash)` and compare in memory;
/// otherwise binary-COPY the (smaller) batch `(ord, id, hash)` into a TEMP table and JOIN this one partition
/// (window-pruned). No per-item SQL-function arguments either way.
async fn precheck_one_partition(
    pool: &crate::PgstacPool,
    bucket: PrecheckBucket,
) -> Result<(Vec<i32>, u64)> {
    // Pre-load row count for the partition that holds repr_dt (named by the server's own partition_name).
    // No row / 0 => empty or just-created => every item is new; skip the probe entirely. ONE connection for
    // the whole task (the n-query, the pull, and the probe's temp-table tx all reuse it) — holding a second
    // would deadlock the pool once there are more parallel partition tasks than pool/2.
    let mut client = pool.get().await?;
    let n: i64 = client
        .query_one(
            "SELECT COALESCE((SELECT n FROM pgstac.partition_stats \
             WHERE partition = (pgstac.partition_name($1, $2::timestamptz)).partition_name), 0)",
            &[&bucket.collection, &bucket.repr_dt],
        )
        .await?
        .get(0);
    if n == 0 {
        return Ok((bucket.ords, 0));
    }

    let batch = bucket.ords.len() as i64;
    if batch > n {
        // PULL: the partition is the smaller side. Stream its (id, item_hash); compare client-side.
        let rows = match bucket.bounds {
            Some((ws, we)) => {
                client
                    .query(
                        "SELECT id, item_hash FROM pgstac.items \
                         WHERE collection = $1 AND datetime >= $2 AND datetime < $3",
                        &[&bucket.collection, &ws, &we],
                    )
                    .await?
            }
            None => {
                client
                    .query(
                        "SELECT id, item_hash FROM pgstac.items WHERE collection = $1",
                        &[&bucket.collection],
                    )
                    .await?
            }
        };
        let mut stored: HashMap<String, Option<Vec<u8>>> = HashMap::with_capacity(rows.len());
        for row in &rows {
            let _ = stored.insert(row.get(0), row.get(1));
        }
        let mut to_load = Vec::new();
        let mut unchanged = 0u64;
        for ((ord, id), hash) in bucket.ords.iter().zip(&bucket.ids).zip(&bucket.hashes) {
            match stored.get(id) {
                Some(Some(stored_hash)) if stored_hash == hash => unchanged += 1,
                _ => to_load.push(*ord),
            }
        }
        return Ok((to_load, unchanged));
    }

    // PROBE: the batch is the smaller side. Binary-COPY (ord, id, hash) into a TEMP table, JOIN this one
    // partition (window-pruned). Status: 0 = new (no match), 1 = unchanged (hash equal), 2 = changed.
    // Reuses the task's single connection (the n-query above) — see the pool-deadlock note there.
    let tx = client.transaction().await?;
    let tmp = format!("_pc_{}", PRECHECK_TEMP_SEQ.fetch_add(1, Ordering::Relaxed));
    tx.batch_execute(&format!(
        "CREATE TEMP TABLE {tmp} (ord int4, id text, hash bytea) ON COMMIT DROP"
    ))
    .await?;
    {
        let sink = tx
            .copy_in(&format!("COPY {tmp} FROM STDIN WITH (FORMAT BINARY)"))
            .await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT, Type::BYTEA]);
        pin_mut!(writer);
        for ((ord, id), hash) in bucket.ords.iter().zip(&bucket.ids).zip(&bucket.hashes) {
            let params: [&(dyn ToSql + Sync); 3] = [ord, id, hash];
            writer.as_mut().write(&params).await?;
        }
        let _ = writer.finish().await?;
    }
    let classify = "CASE WHEN p.id IS NULL THEN 0 \
                    WHEN t.hash IS NOT DISTINCT FROM p.item_hash THEN 1 ELSE 2 END";
    let rows = match bucket.bounds {
        Some((ws, we)) => {
            tx.query(
                &format!(
                    "SELECT t.ord, {classify} FROM {tmp} t \
                     LEFT JOIN pgstac.items p \
                       ON p.collection = $1 AND p.datetime >= $2 AND p.datetime < $3 AND p.id = t.id"
                ),
                &[&bucket.collection, &ws, &we],
            )
            .await?
        }
        None => {
            tx.query(
                &format!(
                    "SELECT t.ord, {classify} FROM {tmp} t \
                     LEFT JOIN pgstac.items p ON p.collection = $1 AND p.id = t.id"
                ),
                &[&bucket.collection],
            )
            .await?
        }
    };
    let mut to_load = Vec::new();
    let mut unchanged = 0u64;
    for row in &rows {
        let ord: i32 = row.get(0);
        let status: i32 = row.get(1);
        if status == 1 {
            unchanged += 1;
        } else {
            to_load.push(ord);
        }
    }
    tx.commit().await?;
    Ok((to_load, unchanged))
}

/// Loads the `fragment_config` for the distinct collections referenced in `items`. Only collections with
/// a non-null config appear; everything else takes the no-fragment path.
async fn fetch_fragment_configs(
    client: &tokio_postgres::Client,
    items: &[Value],
) -> Result<HashMap<String, FragmentConfig>> {
    let mut seen = HashSet::new();
    let collections: Vec<&str> = items
        .iter()
        .filter_map(|item| item.get("collection").and_then(Value::as_str))
        .filter(|collection| seen.insert(*collection))
        .collect();
    let mut configs = HashMap::new();
    if collections.is_empty() {
        return Ok(configs);
    }
    let rows = client
        .query(
            "SELECT id, fragment_config FROM collections \
             WHERE id = ANY($1) AND fragment_config IS NOT NULL",
            &[&collections],
        )
        .await?;
    for row in rows {
        let id: String = row.get("id");
        let config: Vec<String> = row.get("fragment_config");
        let _ = configs.insert(id, FragmentConfig::parse(&config)?);
    }
    Ok(configs)
}

/// Strips a collection's fragment-owned keys from a dehydrated row (mirrors the `enriched` CTE of
/// `items_staging_dehydrate`). `config` is non-empty. `fragment_id` is stamped later by
/// [`stamp_fragment_ids`]; a row whose item has no fragment payload keeps `fragment_id = None`.
fn apply_fragment_split(row: &mut DehydratedRow, config: &FragmentConfig) {
    let assets = row
        .assets
        .take()
        .unwrap_or_else(|| Value::Object(Map::new()));
    row.assets = Some(strip_fragment_col(assets, "assets", config));
    let properties = std::mem::replace(&mut row.properties, Value::Null);
    row.properties = strip_fragment_col(properties, "properties", config);
    if config.has_path(&["stac_version"]) {
        row.stac_version = None;
    }
    if config.has_path(&["stac_extensions"]) {
        row.stac_extensions = Value::Array(Vec::new());
    }
    // When link_hrefs carries the per-item hrefs, the link structure lives in the fragment's
    // links_template, so the row's links column is nulled (matches items_staging_dehydrate).
    if row
        .link_hrefs
        .as_ref()
        .is_some_and(|hrefs| !hrefs.is_empty())
    {
        row.links = None;
    }
}

/// Dedups the per-collection fragment payloads, upserts them via `ensure_fragments`, and stamps each
/// contributing row's `fragment_id`. Local dedup (by serialized payload) just shrinks the array sent;
/// `ensure_fragments` is itself dup-safe and assigns ids by hash server-side.
async fn stamp_fragment_ids(
    client: &tokio_postgres::Client,
    rows: &mut [DehydratedRow],
    payloads: &[Option<Value>],
) -> Result<()> {
    // Group contributing item indices by collection (owned key — we mutate rows below).
    let mut by_collection: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, payload) in payloads.iter().enumerate() {
        if payload.is_some() {
            by_collection
                .entry(rows[index].collection.clone())
                .or_default()
                .push(index);
        }
    }

    for (collection, indices) in &by_collection {
        // Distinct payloads (preserving first-seen order) + each item's slot in that distinct list.
        let mut distinct: Vec<Value> = Vec::new();
        let mut key_to_slot: HashMap<String, usize> = HashMap::new();
        let mut slot_per_item: Vec<(usize, usize)> = Vec::with_capacity(indices.len());
        for &index in indices {
            let payload = payloads[index]
                .as_ref()
                .expect("contributing item has a payload");
            let key = serde_json::to_string(payload)?;
            let slot = *key_to_slot.entry(key).or_insert_with(|| {
                distinct.push(payload.clone());
                distinct.len() - 1
            });
            slot_per_item.push((index, slot));
        }

        // ensure_fragments returns (ord, frag_id) where ord is the 1-based input position.
        let returned = client
            .query(
                "SELECT ord, frag_id FROM ensure_fragments($1, $2)",
                &[collection, &distinct],
            )
            .await?;
        let mut ids = vec![0i64; distinct.len()];
        for row in returned {
            let ord: i32 = row.get("ord");
            ids[usize::try_from(ord - 1)?] = row.get("frag_id");
        }
        for (index, slot) in slot_per_item {
            rows[index].fragment_id = Some(ids[slot]);
        }
    }
    Ok(())
}
