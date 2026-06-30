//! Parallel stac-geoparquet decode for the loader.
//!
//! A stac-geoparquet file is a stack of independent **row groups**; decoding one needs nothing from
//! any other. [`spawn_parquet_decoders`] shards a file's row groups across N threads, each decoding its
//! shard to [`Vec<Value>`](serde_json::Value) batches and feeding the shared bounded channel the loader
//! already drains for the NDJSON path. This lifts the single-thread parquet→GeoJSON ceiling that made
//! the parquet ingest path decode-bound and far slower than NDJSON.
//!
//! The single-file reader mirrors [`stac::geoparquet::from_reader_iter`] (same geoarrow schema, same
//! per-batch decode), restricted to a row-group subset via
//! [`ParquetRecordBatchReaderBuilder::with_row_groups`]. Row-group sharding is exact: every row group
//! is assigned to exactly one shard, so the union over shards decodes every row exactly once — no
//! dropped or duplicated items.

use arrow_array::RecordBatch;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::path::Path;
use tokio::sync::mpsc::Sender;

/// Distributes the row-group indices `0..num_row_groups` across at most `n` shards.
///
/// Indices are dealt **round-robin** (`rg % shards`) rather than in contiguous ranges, so when row
/// groups differ in size — common in stac-geoparquet, whose last group is a remainder — each shard
/// still gets a balanced spread of small and large groups instead of one shard inheriting all the big
/// ones. The number of shards is capped at `num_row_groups` (an empty file yields no shards), and every
/// index `0..num_row_groups` appears in exactly one returned shard.
///
/// # Examples
///
/// ```
/// use pgstac::parquet_decode::shard_row_groups;
///
/// // 5 row groups over 2 threads: round-robin split.
/// assert_eq!(shard_row_groups(5, 2), vec![vec![0, 2, 4], vec![1, 3]]);
/// // More threads than row groups: one row group per shard, no empties.
/// assert_eq!(shard_row_groups(2, 8), vec![vec![0], vec![1]]);
/// // Empty file: no shards.
/// assert!(shard_row_groups(0, 4).is_empty());
/// ```
pub fn shard_row_groups(num_row_groups: usize, n: usize) -> Vec<Vec<usize>> {
    let shards = n.max(1).min(num_row_groups);
    let mut out: Vec<Vec<usize>> = vec![Vec::new(); shards];
    for rg in 0..num_row_groups {
        out[rg % shards].push(rg);
    }
    out
}

/// Decodes a row-group subset of a stac-geoparquet file, invoking `on_batch` once per record batch
/// with the decoded items and an estimated in-memory byte size.
///
/// Mirrors [`stac::geoparquet::from_reader_iter`] — same geoarrow schema, same per-batch
/// `items_from_record_batch` decode — but restricted to `row_groups` via
/// [`ParquetRecordBatchReaderBuilder::with_row_groups`]. The byte estimate is the row groups'
/// uncompressed columnar size prorated per item (a cheap, monotonic proxy for the JSON text size the
/// NDJSON path reports), so the loader's `PGSTAC_LOAD_BYTES` budget bounds the in-flight set for
/// parquet just as it does for NDJSON. Errors are returned as `String` to match the loader channel.
fn decode_row_groups<F>(path: &Path, row_groups: &[usize], mut on_batch: F) -> Result<(), String>
where
    F: FnMut(Vec<Value>, usize) -> Result<(), String>,
{
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;

    // Per-item byte proxy: the uncompressed columnar size of this shard's row groups, divided by their
    // row count. Reported per batch as `batch.len() * per_item_bytes` so the loader's byte budget sees a
    // sensible, monotonic size (matching the NDJSON path's summed item-text length closely enough to
    // bound memory) without a second serialization pass over every item.
    let meta = builder.metadata();
    let (shard_bytes, shard_rows) = row_groups.iter().fold((0i64, 0i64), |(b, r), &rg| {
        let g = meta.row_group(rg);
        (b + g.total_byte_size(), r + g.num_rows())
    });
    let per_item_bytes = if shard_rows > 0 {
        (shard_bytes / shard_rows).max(0) as usize
    } else {
        0
    };

    // Build the same geoarrow schema from_reader_iter would, then restrict to this shard's row groups.
    let geo_metadata = builder
        .geoparquet_metadata()
        .transpose()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "missing geoparquet metadata".to_string())?;
    let geoarrow_schema = builder
        .geoarrow_schema(&geo_metadata, true, Default::default())
        .map_err(|e| e.to_string())?;
    let reader = builder
        .with_row_groups(row_groups.to_vec())
        .build()
        .map_err(|e| e.to_string())?;
    let reader =
        GeoParquetRecordBatchReader::try_new(reader, geoarrow_schema).map_err(|e| e.to_string())?;

    for record_batch in reader {
        let record_batch: RecordBatch = record_batch.map_err(|e| e.to_string())?;
        let items =
            stac::geoarrow::items_from_record_batch(record_batch).map_err(|e| e.to_string())?;
        let batch: Vec<Value> = items
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let bytes = batch.len().saturating_mul(per_item_bytes);
        on_batch(batch, bytes)?;
    }
    Ok(())
}

/// Parallel stac-geoparquet decode: shard one file's row groups over `n` threads, each feeding `tx`.
///
/// Reads the file footer once to count row groups, shards them with [`shard_row_groups`] (round-robin,
/// capped at the row-group count), and spawns one decoder thread per shard. Each thread opens its own
/// file handle and decodes only its row groups — so every row group is decoded exactly once and the
/// union loads exactly the items a single-thread pass would, in no particular order (fine for load).
/// Every batch is sent as `(Vec<Value>, byte_estimate)`; the bounded channel applies backpressure
/// (decode pauses when the loaders fall behind). A decode error is sent as `Err(String)` and that
/// thread stops. Returns the spawned thread handles for the caller to join.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use serde_json::Value;
///
/// # tokio_test::block_on(async {
/// let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<(Vec<Value>, usize), String>>(4);
/// let handles = pgstac::parquet_decode::spawn_parquet_decoders(Path::new("items.parquet"), 4, tx)
///     .unwrap();
/// while let Some(msg) = rx.recv().await {
///     let (items, _bytes) = msg.unwrap();
///     println!("{} items", items.len());
/// }
/// for h in handles {
///     let _ = h.join();
/// }
/// # })
/// ```
pub fn spawn_parquet_decoders(
    path: &Path,
    n: usize,
    tx: Sender<Result<(Vec<Value>, usize), String>>,
) -> std::io::Result<Vec<std::thread::JoinHandle<()>>> {
    // Read the footer once to learn the row-group count, then shard. Opening the file again per thread
    // (below) re-reads the footer, which is cheap relative to decoding the data pages.
    let num_row_groups = {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        builder.metadata().num_row_groups()
    };

    let shards = shard_row_groups(num_row_groups, n);
    // A file with no row groups spawns no decoders: `tx` is dropped on return, so the loader's channel
    // closes immediately and it sees an empty load rather than hanging.
    if shards.is_empty() {
        return Ok(Vec::new());
    }

    let mut handles = Vec::with_capacity(shards.len());
    for shard in shards {
        let path = path.to_path_buf();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let result: Result<(), String> = decode_row_groups(&path, &shard, |batch, bytes| {
                // `Err` here only means the receiver is gone (early stop / downstream error); treat it
                // as a stop signal, not a decode failure, so we don't also send a spurious Err.
                if tx.blocking_send(Ok((batch, bytes))).is_err() {
                    Err("receiver closed".to_string())
                } else {
                    Ok(())
                }
            });
            if let Err(e) = result
                && e != "receiver closed"
            {
                let _ = tx.blocking_send(Err(e));
            }
        }));
    }
    Ok(handles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use stac::ItemCollection;
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    /// Round-robin sharding covers every row group exactly once, never empties a shard while indices
    /// remain, and caps shard count at the row-group count.
    #[test]
    fn shard_row_groups_partitions_exactly() {
        for num in 0..40usize {
            for n in 1..12usize {
                let shards = shard_row_groups(num, n);
                // Cap: at most min(n, num) shards, and no empty shards when any exist.
                assert!(shards.len() <= n.min(num).max(if num == 0 { 0 } else { 1 }));
                assert_eq!(shards.len(), n.min(num));
                for s in &shards {
                    assert!(!s.is_empty(), "no empty shards (num={num}, n={n})");
                }
                // Exact partition: union == 0..num, no duplicates.
                let mut seen: BTreeSet<usize> = BTreeSet::new();
                for s in &shards {
                    for &rg in s {
                        assert!(
                            seen.insert(rg),
                            "row group {rg} assigned twice (num={num}, n={n})"
                        );
                    }
                }
                let expected: BTreeSet<usize> = (0..num).collect();
                assert_eq!(seen, expected, "every row group covered (num={num}, n={n})");
            }
        }
    }

    /// Writes `count` distinct items as stac-geoparquet with a small row-group size, forcing several
    /// row groups so the parallel decode actually shards.
    fn write_multi_rowgroup_parquet(count: usize, rows_per_group: usize) -> Vec<u8> {
        use stac::IntoGeoparquet;
        let items: Vec<stac::Item> = (0..count)
            .map(|i| {
                let mut item = stac::Item::new(format!("item-{i:05}"));
                item.collection = Some("c".to_string());
                item.geometry = Some(
                    serde_json::from_value(json!({
                        "type": "Point",
                        "coordinates": [(-180.0 + i as f64 * 0.001), 40.0],
                    }))
                    .unwrap(),
                );
                item.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
                let _ = item
                    .properties
                    .additional_fields
                    .insert("seq".into(), (i as i64).into());
                item
            })
            .collect();
        let options =
            stac::geoparquet::WriterOptions::new().with_max_row_group_row_count(rows_per_group);
        ItemCollection::from(items)
            .into_geoparquet_vec(options)
            .unwrap()
    }

    /// Collects every item the parallel decoder yields for a file, keyed by id, with the total count.
    fn decode_parallel_all(path: &Path, n: usize) -> (usize, BTreeMap<String, Value>) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<(Vec<Value>, usize), String>>(4);
            let handles = spawn_parquet_decoders(path, n, tx).unwrap();
            let mut count = 0usize;
            let mut by_id: BTreeMap<String, Value> = BTreeMap::new();
            while let Some(msg) = rx.recv().await {
                let (items, _bytes) = msg.unwrap();
                count += items.len();
                for item in items {
                    let id = item["id"].as_str().unwrap().to_string();
                    assert!(
                        by_id.insert(id.clone(), item).is_none(),
                        "duplicate id {id}"
                    );
                }
            }
            for h in handles {
                h.join().unwrap();
            }
            (count, by_id)
        })
    }

    /// The parallel decode loads exactly the same items (count + content) as the single-thread
    /// `from_reader_iter` path, with no dropped or duplicated rows — across several shard counts and a
    /// file with multiple row groups.
    #[test]
    fn parallel_decode_matches_single_thread() {
        let count = 2_500usize;
        let bytes = write_multi_rowgroup_parquet(count, 300); // ~9 row groups
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("items.parquet");
        std::fs::write(&path, &bytes).unwrap();

        // Single-thread baseline via the same API the loader uses today.
        let mut single: BTreeMap<String, Value> = BTreeMap::new();
        for batch in
            stac::geoparquet::from_reader_iter(std::fs::File::open(&path).unwrap()).unwrap()
        {
            for item in batch.unwrap() {
                let v = serde_json::to_value(item).unwrap();
                let id = v["id"].as_str().unwrap().to_string();
                let _ = single.insert(id, v);
            }
        }
        assert_eq!(single.len(), count, "baseline sees every item");

        for n in [1usize, 2, 3, 4, 8, 16] {
            let (parallel_count, parallel) = decode_parallel_all(&path, n);
            assert_eq!(parallel_count, count, "n={n}: item count matches");
            assert_eq!(parallel.len(), count, "n={n}: no duplicate ids");
            assert_eq!(parallel, single, "n={n}: identical items to single-thread");
        }
    }

    /// A single-row-group file still decodes correctly when more threads are requested than row groups
    /// (the shard count caps at 1).
    #[test]
    fn parallel_decode_single_rowgroup() {
        let count = 50usize;
        let bytes = write_multi_rowgroup_parquet(count, 150_000); // one row group
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("items.parquet");
        std::fs::write(&path, &bytes).unwrap();
        let (parallel_count, parallel) = decode_parallel_all(&path, 8);
        assert_eq!(parallel_count, count);
        assert_eq!(parallel.len(), count);
    }
}
