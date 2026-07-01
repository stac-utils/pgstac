//! Output formats for a dump unit: NDJSON, ItemCollection JSON, and
//! stac-geoparquet (both write modes, A8).
//!
//! A [`Format`] is paired with a [`crate::export::sink::Sink`] (format ⟂ sink).
//! The format owns *encoding* hydrated items into bytes; the sink owns *where*
//! the bytes go. NDJSON / ItemCollection are pure serde. Geoparquet delegates to
//! the published `stac::geoparquet` writer:
//!
//! * **Buffered-widening** ([`GeoparquetMode::Buffered`], default; required for
//!   0.9.11): buffer the whole partition, then encode once so the Arrow schema is
//!   complete by construction (no statistical sampling can drop a column). An
//!   oversized partition spills to a temp file before encode (see
//!   [`crate::export::budget`]).
//! * **Stream-write** ([`GeoparquetMode::Stream`], 0.10): the 0.10 registry
//!   supplies a complete schema up front, so items are written batch-by-batch
//!   with no full buffering; the first batch fixes the schema and every later
//!   batch (registry-complete) conforms.

use crate::{Error, Result};
use serde_json::Value;
use stac::Item;
use stac::geoparquet::{Compression, WriterBuilder, WriterOptions};

/// Parquet compression codec selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetCompression {
    /// zstd (default).
    #[default]
    Zstd,
    /// snappy.
    Snappy,
    /// uncompressed.
    Uncompressed,
}

impl ParquetCompression {
    /// Manifest text form.
    pub fn as_str(&self) -> &'static str {
        match self {
            ParquetCompression::Zstd => "zstd",
            ParquetCompression::Snappy => "snappy",
            ParquetCompression::Uncompressed => "uncompressed",
        }
    }

    fn to_stac(self) -> Option<Compression> {
        match self {
            ParquetCompression::Zstd => Some(Compression::ZSTD(Default::default())),
            ParquetCompression::Snappy => Some(Compression::SNAPPY),
            ParquetCompression::Uncompressed => Some(Compression::UNCOMPRESSED),
        }
    }
}

/// Which geoparquet write strategy to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoparquetMode {
    /// Buffer the whole partition then encode once (schema complete by
    /// construction). Required for 0.9.11.
    Buffered,
    /// Stream batches with a registry-complete schema (0.10).
    Stream,
}

/// The output format for a dump unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Newline-delimited JSON, one item per line.
    Ndjson,
    /// A single STAC `FeatureCollection` JSON object.
    ItemCollectionJson,
    /// stac-geoparquet.
    Geoparquet {
        /// Compression codec.
        compression: ParquetCompression,
        /// Write mode.
        mode: GeoparquetMode,
        /// Max rows per parquet row-group (`None` = the stac/parquet default). Smaller values bound the
        /// encoder's in-memory working set when streaming, at the cost of more, smaller row-groups.
        max_row_group_row_count: Option<usize>,
    },
}

impl Format {
    /// The default partition geoparquet format for a storage model: buffered for
    /// 0.9.11 (no registry), stream for 0.10.
    pub fn geoparquet_for(model: crate::hydrate::HydrationModel) -> Format {
        let mode = match model {
            crate::hydrate::HydrationModel::BaseItem => GeoparquetMode::Buffered,
            crate::hydrate::HydrationModel::Fragment => GeoparquetMode::Stream,
        };
        Format::Geoparquet {
            compression: ParquetCompression::default(),
            mode,
            max_row_group_row_count: None,
        }
    }
}

/// Converts a hydrated item [`Value`] into a [`stac::Item`].
fn value_to_item(value: Value) -> Result<Item> {
    serde_json::from_value(value).map_err(Error::from)
}

/// Encodes a complete set of hydrated items (one dump unit) to bytes.
///
/// For NDJSON / ItemCollection this serializes directly. For geoparquet it uses
/// the buffered path (encode once from the full vec) — the simplest correct
/// strategy and the one required for 0.9.11. The streaming geoparquet path is
/// [`GeoparquetStreamWriter`].
pub fn encode_all(format: Format, items: Vec<Value>) -> Result<Vec<u8>> {
    match format {
        Format::Ndjson => encode_ndjson(&items),
        Format::ItemCollectionJson => encode_item_collection(items),
        Format::Geoparquet {
            compression,
            max_row_group_row_count,
            ..
        } => encode_geoparquet(items, compression, max_row_group_row_count),
    }
}

fn encode_ndjson(items: &[Value]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    for item in items {
        let line = serde_json::to_vec(item)?;
        buf.extend_from_slice(&line);
        buf.push(b'\n');
    }
    Ok(buf)
}

fn encode_item_collection(items: Vec<Value>) -> Result<Vec<u8>> {
    let fc = serde_json::json!({
        "type": "FeatureCollection",
        "features": items,
    });
    serde_json::to_vec(&fc).map_err(Error::from)
}

fn encode_geoparquet(
    mut items: Vec<Value>,
    compression: ParquetCompression,
    max_row_group_row_count: Option<usize>,
) -> Result<Vec<u8>> {
    // Buffered-widening completeness guarantee (USER_STORIES §9): the whole
    // partition is in hand, so resolve any property whose scalar JSON type
    // conflicts across items into a single Arrow-representable type before
    // encoding. Without this, a column that is e.g. a string in 99 items and an
    // integer in 1 makes the Arrow encoder fail and the dump would lose the
    // partition. See `findings/EXPORT-geoparquet-type-conflicts.md`.
    coerce_conflicting_properties(&mut items);

    let items: Result<Vec<Item>> = items.into_iter().map(value_to_item).collect();
    let items = items?;
    let mut buf = Vec::new();
    let mut options = WriterOptions::new().with_compression(compression.to_stac());
    if let Some(n) = max_row_group_row_count {
        options = options.with_max_row_group_row_count(n);
    }
    WriterBuilder::new(&mut buf)
        .writer_options(options)
        .build(items)?
        .finish()?;
    Ok(buf)
}

/// A coarse JSON scalar class used to detect cross-item type conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ScalarClass {
    Null,
    Bool,
    Number,
    String,
    /// Object or array — treated as non-scalar; conflicts among these or with
    /// scalars are left to the encoder (nested handling is out of scope here).
    Compound,
}

fn classify(v: &Value) -> ScalarClass {
    match v {
        Value::Null => ScalarClass::Null,
        Value::Bool(_) => ScalarClass::Bool,
        Value::Number(_) => ScalarClass::Number,
        Value::String(_) => ScalarClass::String,
        Value::Array(_) | Value::Object(_) => ScalarClass::Compound,
    }
}

/// Detects `properties.<key>` columns whose scalar type conflicts across the
/// buffered items and coerces every value of those columns to a string (numbers
/// and bools become their JSON text). This is the lossless-completeness default:
/// the field is always captured and the partition always encodes; the int/string
/// distinction for a conflicted column is normalized to string.
///
/// Only top-level `properties` scalar keys are normalized (the common conflict
/// surface); nested/compound conflicts are left to the encoder.
fn coerce_conflicting_properties(items: &mut [Value]) {
    use std::collections::HashMap;
    // First pass: collect the set of scalar classes seen per property key.
    let mut classes: HashMap<String, std::collections::HashSet<ScalarClass>> = HashMap::new();
    for item in items.iter() {
        if let Some(Value::Object(props)) = item.get("properties") {
            for (k, v) in props {
                let c = classify(v);
                if c == ScalarClass::Null {
                    continue; // null is compatible with any column (nullable)
                }
                let _ = classes.entry(k.clone()).or_default().insert(c);
            }
        }
    }
    // Conflicting = more than one non-null scalar class, and all involved are
    // scalar (we only safely stringify scalars).
    let conflicted: std::collections::HashSet<String> = classes
        .into_iter()
        .filter(|(_, set)| set.len() > 1 && set.iter().all(|c| *c != ScalarClass::Compound))
        .map(|(k, _)| k)
        .collect();
    if conflicted.is_empty() {
        return;
    }
    // Second pass: stringify the conflicted keys in place.
    for item in items.iter_mut() {
        if let Some(Value::Object(props)) = item.get_mut("properties") {
            for key in &conflicted {
                if let Some(v) = props.get_mut(key)
                    && !v.is_null()
                {
                    let s = match v {
                        Value::String(_) => continue,
                        Value::Bool(b) => b.to_string(),
                        Value::Number(n) => n.to_string(),
                        _ => continue,
                    };
                    *v = Value::String(s);
                }
            }
        }
    }
}

/// A streaming geoparquet writer for the 0.10 path: write batches as they
/// arrive into a caller-owned sink `W`, finalize at the end. The first batch
/// fixes the schema (registry-complete on 0.10), so later batches conform
/// without buffering the whole partition — the *item* working set is one batch.
///
/// The caller owns the sink `W` (a file, a `&mut Vec<u8>`, etc.) and reads/streams
/// the bytes after [`finish`](Self::finish). This keeps the encoded-file bytes off
/// the memory budget when `W` is file-backed (A6).
#[allow(missing_debug_implementations)]
pub struct GeoparquetStreamWriter<W: std::io::Write + Send> {
    inner: WriterState<W>,
    compression: ParquetCompression,
    max_row_group_row_count: Option<usize>,
}

enum WriterState<W: std::io::Write + Send> {
    /// Not yet started: holds the pending sink.
    Pending(Option<W>),
    /// Started: holds the live writer (boxed — the parquet writer is large).
    Active(Box<stac::geoparquet::Writer<W>>),
}

impl<W: std::io::Write + Send> GeoparquetStreamWriter<W> {
    /// Creates a new streaming writer that encodes into `sink`.
    pub fn new(sink: W, compression: ParquetCompression) -> Self {
        GeoparquetStreamWriter {
            inner: WriterState::Pending(Some(sink)),
            compression,
            max_row_group_row_count: None,
        }
    }

    /// Sets the max rows per parquet row-group. Smaller values flush row-groups sooner, bounding the
    /// in-memory working set while streaming (at the cost of more, smaller row-groups).
    pub fn with_max_row_group_row_count(mut self, max_row_group_row_count: usize) -> Self {
        self.max_row_group_row_count = Some(max_row_group_row_count);
        self
    }

    /// Writes one batch of hydrated items. The first non-empty batch fixes the
    /// schema.
    pub fn write_batch(&mut self, items: Vec<Value>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let items: Result<Vec<Item>> = items.into_iter().map(value_to_item).collect();
        let items = items?;
        match &mut self.inner {
            WriterState::Pending(sink) => {
                let sink = sink.take().expect("sink present until started");
                let mut options = WriterOptions::new().with_compression(self.compression.to_stac());
                if let Some(n) = self.max_row_group_row_count {
                    options = options.with_max_row_group_row_count(n);
                }
                let writer = WriterBuilder::new(sink)
                    .writer_options(options)
                    .build(items)?;
                self.inner = WriterState::Active(Box::new(writer));
            }
            WriterState::Active(writer) => {
                writer.write(items)?;
            }
        }
        Ok(())
    }

    /// Whether any batch has been written (i.e. a file would be produced).
    pub fn has_data(&self) -> bool {
        matches!(self.inner, WriterState::Active(_))
    }

    /// Finalizes the file, flushing the parquet footer into the sink. Returns
    /// `false` if no batches were written (no file should be produced).
    pub fn finish(self) -> Result<bool> {
        match self.inner {
            WriterState::Active(writer) => {
                (*writer).finish()?;
                Ok(true)
            }
            WriterState::Pending(_) => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_item(id: &str) -> Value {
        json!({
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": id,
            "collection": "c",
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
            "bbox": [1.0, 2.0, 1.0, 2.0],
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "assets": {},
            "links": []
        })
    }

    #[test]
    fn ndjson_roundtrip() {
        let items = vec![sample_item("a"), sample_item("b")];
        let bytes = encode_all(Format::Ndjson, items).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], "a");
    }

    #[test]
    fn item_collection_shape() {
        let items = vec![sample_item("a")];
        let bytes = encode_all(Format::ItemCollectionJson, items).unwrap();
        let fc: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(fc["type"], "FeatureCollection");
        assert_eq!(fc["features"][0]["id"], "a");
    }

    #[test]
    fn compression_str() {
        assert_eq!(ParquetCompression::Zstd.as_str(), "zstd");
        assert_eq!(ParquetCompression::default(), ParquetCompression::Zstd);
    }

    /// Reads geoparquet bytes back via a temp file and returns the item count.
    fn read_geoparquet(bytes: &[u8]) -> usize {
        use std::io::{Seek, SeekFrom, Write};
        let mut tf = tempfile::tempfile().unwrap();
        tf.write_all(bytes).unwrap();
        let _ = tf.seek(SeekFrom::Start(0)).unwrap();
        stac::geoparquet::from_reader(tf).unwrap().items.len()
    }

    #[test]
    fn geoparquet_buffered_readable() {
        let items = vec![sample_item("a"), sample_item("b")];
        let bytes = encode_all(
            Format::Geoparquet {
                compression: ParquetCompression::Zstd,
                mode: GeoparquetMode::Buffered,
                max_row_group_row_count: None,
            },
            items,
        )
        .unwrap();
        assert_eq!(read_geoparquet(&bytes), 2);
    }

    #[test]
    fn geoparquet_stream_readable() {
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = GeoparquetStreamWriter::new(&mut buf, ParquetCompression::Zstd);
        writer.write_batch(vec![sample_item("a")]).unwrap();
        writer.write_batch(vec![sample_item("b")]).unwrap();
        assert!(writer.has_data());
        assert!(writer.finish().unwrap());
        assert_eq!(read_geoparquet(&buf), 2);
    }

    #[test]
    fn coerce_mixed_type_property() {
        let mut items = vec![
            json!({"type":"Feature","id":"a","geometry":{"type":"Point","coordinates":[0.0,0.0]},
                   "properties":{"datetime":"2020-01-01T00:00:00Z","naip:year":"2011"}}),
            json!({"type":"Feature","id":"b","geometry":{"type":"Point","coordinates":[0.0,0.0]},
                   "properties":{"datetime":"2020-01-01T00:00:00Z","naip:year":2013}}),
        ];
        coerce_conflicting_properties(&mut items);
        // Both naip:year are now strings.
        assert_eq!(items[0]["properties"]["naip:year"], "2011");
        assert_eq!(items[1]["properties"]["naip:year"], "2013");
        // Now it encodes without error.
        let bytes = encode_all(
            Format::Geoparquet {
                compression: ParquetCompression::Zstd,
                mode: GeoparquetMode::Buffered,
                max_row_group_row_count: None,
            },
            items,
        )
        .unwrap();
        assert_eq!(read_geoparquet(&bytes), 2);
    }

    #[test]
    fn coerce_leaves_consistent_property() {
        let mut items = vec![
            json!({"properties":{"gsd":10}}),
            json!({"properties":{"gsd":20}}),
        ];
        coerce_conflicting_properties(&mut items);
        // No conflict -> numbers stay numbers.
        assert_eq!(items[0]["properties"]["gsd"], 10);
        assert!(items[1]["properties"]["gsd"].is_number());
    }

    #[test]
    fn geoparquet_stream_empty_no_file() {
        let mut buf: Vec<u8> = Vec::new();
        let writer = GeoparquetStreamWriter::new(&mut buf, ParquetCompression::Zstd);
        assert!(!writer.has_data());
        assert!(!writer.finish().unwrap(), "no batches -> no file");
        assert!(buf.is_empty());
    }

    /// Number of parquet row-groups in the encoded bytes.
    fn num_row_groups(bytes: &[u8]) -> usize {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use std::io::{Seek, SeekFrom, Write};
        let mut tf = tempfile::tempfile().unwrap();
        tf.write_all(bytes).unwrap();
        let _ = tf.seek(SeekFrom::Start(0)).unwrap();
        SerializedFileReader::new(tf)
            .unwrap()
            .metadata()
            .num_row_groups()
    }

    #[test]
    fn geoparquet_stream_row_group_size_takes_effect() {
        let items: Vec<Value> = (0..100)
            .map(|i| sample_item(&format!("item-{i}")))
            .collect();

        // Cap at 10 rows/group, fed in 10-item batches -> 10 row-groups.
        let mut capped: Vec<u8> = Vec::new();
        let mut w = GeoparquetStreamWriter::new(&mut capped, ParquetCompression::Zstd)
            .with_max_row_group_row_count(10);
        for chunk in items.chunks(10) {
            w.write_batch(chunk.to_vec()).unwrap();
        }
        assert!(w.finish().unwrap());

        // Default (no cap): the same 100 items land in a single row-group.
        let mut default: Vec<u8> = Vec::new();
        let mut w2 = GeoparquetStreamWriter::new(&mut default, ParquetCompression::Zstd);
        w2.write_batch(items.clone()).unwrap();
        assert!(w2.finish().unwrap());

        assert_eq!(read_geoparquet(&capped), 100, "all items round-trip");
        assert_eq!(num_row_groups(&default), 1, "no cap => single row-group");
        assert_eq!(
            num_row_groups(&capped),
            10,
            "cap=10 over 100 items => 10 row-groups"
        );
    }
}
