//! Dump manifest, checkpoint, and root-metadata models, plus SHA-256 helpers.
//!
//! The manifest is the cross-PR contract the ingest side reads, written **last**; its presence signals a
//! complete dump.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// The manifest format version. Import refuses an unknown major.
pub const MANIFEST_VERSION: &str = "1";

/// Computes the lowercase hex SHA-256 of a byte slice.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

/// A running SHA-256 over streamed bytes (for files written incrementally).
#[derive(Debug, Default, Clone)]
pub struct Sha256Writer {
    hasher: Sha256,
    bytes: u64,
}

impl Sha256Writer {
    /// Creates a new running hasher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds bytes into the hash.
    pub fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        self.bytes += bytes.len() as u64;
    }

    /// Number of bytes hashed so far.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Finalizes and returns the hex digest.
    pub fn finalize_hex(self) -> String {
        hex_encode(&self.hasher.finalize())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// A half-open `[start, end)` datetime range.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatetimeRange {
    /// Inclusive start, STAC text (`...Z`).
    pub start: String,
    /// Exclusive end, STAC text (`...Z`).
    pub end: String,
}

/// A recorded file: path relative to the dump root, plus integrity metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path relative to the dump root.
    pub path: String,
    /// Lowercase hex SHA-256 of the file bytes.
    pub sha256: String,
    /// File size in bytes.
    pub bytes: u64,
    /// Row count, when meaningful (e.g. queryables/settings/parquet).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}

/// One partition file in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionEntry {
    /// Source partition name (informational/debug only).
    pub name: String,
    /// Path to the parquet file relative to the dump root.
    pub file: String,
    /// Lowercase hex SHA-256.
    pub sha256: String,
    /// File size in bytes.
    pub bytes: u64,
    /// Exact rows written.
    pub item_count: u64,
    /// `datetime` footprint of the written rows.
    pub datetime_range: Option<DatetimeRange>,
    /// `end_datetime` footprint of the written rows.
    pub end_datetime_range: Option<DatetimeRange>,
    /// Spatial extent `[w, s, e, n]` (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Vec<f64>>,
}

/// One collection in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionEntry {
    /// Collection id (authoritative, from `collection.json`).
    pub id: String,
    /// The `collection.json` file entry.
    pub collection_file: FileEntry,
    /// `collections.partition_trunc` (`month`/`year`/null).
    pub partition_trunc: Option<String>,
    /// Total items dumped for this collection.
    pub item_count: u64,
    /// Partition files.
    pub partitions: Vec<PartitionEntry>,
}

/// The exporter tool block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    /// Tool name (`pgstac`).
    pub name: String,
    /// Command (`dump`).
    pub command: String,
    /// Exporter binary version.
    pub version: String,
}

/// The source-instance block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    /// `get_version()` of the dumped instance.
    pub pgstac_version: String,
    /// Postgres server version (informational).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_version: Option<String>,
    /// Server description, no credentials (informational).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
}

/// Output options that affect how files were written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Options {
    /// Always true (decision: items are always hydrated).
    pub hydrated: bool,
    /// Parquet codec used.
    pub compression: String,
    /// Always `["datetime", "id"]`.
    pub ordering: Vec<String>,
}

/// Partial-dump filter description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    /// Selected collection ids.
    pub collection_ids: Vec<String>,
    /// Optional datetime prefilter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datetime: Option<DatetimeRange>,
    /// Optional bbox prefilter `[w, s, e, n]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Vec<f64>>,
}

/// The dump manifest, written last.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest format version.
    pub manifest_version: String,
    /// RFC3339 creation timestamp.
    pub created_at: String,
    /// Exporter tool info.
    pub tool: Tool,
    /// Source instance info.
    pub source: Source,
    /// `"full"` or `"partial"`.
    pub dump_type: String,
    /// `null` for full; the filter for partial.
    pub filter: Option<Filter>,
    /// Whether `--consistent` was used.
    pub consistent: bool,
    /// Informational snapshot block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<serde_json::Value>,
    /// Output options.
    pub options: Options,
    /// Root metadata files (queryables, settings).
    pub metadata_files: BTreeMap<String, FileEntry>,
    /// Per-collection entries.
    pub collections: Vec<CollectionEntry>,
}

/// A `_checkpoint.json` entry: one fully-written partition file.
///
/// Carries the full [`PartitionEntry`] and its owning collection id so a resumed
/// run can place a skipped partition back into the manifest without re-scanning
/// it (the manifest is rebuilt completely on every run, including resumes). The
/// flat `file`/`sha256`/`item_count` mirror the entry for quick human/diff
/// reading and to verify the on-disk file before skipping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEntry {
    /// Path relative to the dump root.
    pub file: String,
    /// Lowercase hex SHA-256.
    pub sha256: String,
    /// Rows written.
    pub item_count: u64,
    /// Owning collection id (to re-place the entry on resume).
    pub collection_id: String,
    /// The full manifest entry for this partition file.
    pub entry: PartitionEntry,
}

/// The `_checkpoint.json` progress file; present only mid-dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Manifest format version.
    pub manifest_version: String,
    /// RFC3339 start timestamp.
    pub started_at: String,
    /// Completed partition files.
    pub completed: Vec<CheckpointEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_vector() {
        // SHA-256("abc")
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn streaming_matches_oneshot() {
        let mut w = Sha256Writer::new();
        w.update(b"ab");
        w.update(b"c");
        assert_eq!(w.bytes(), 3);
        assert_eq!(w.finalize_hex(), sha256_hex(b"abc"));
    }
}
