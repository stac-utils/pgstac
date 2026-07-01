//! Client-side field registry: the per-collection set of JSON paths and value kinds the loader maintains.
//!
//! The loader walks EVERY ingested item's content — no sampling — to collect the COMPLETE set of JSON paths
//! and their value kinds per collection, then UPSERTs them ADD-ONLY into `item_field_registry` so it is
//! always a *superset* of the data (too-wide is fine; expiring stale paths is deferred async maintenance).
//! The walk mirrors the SQL `jsonb_field_rows` exactly (`.`-joined paths, leaf flag, `jsonb_typeof` value
//! kind, depth cap 20) so a Rust-loaded registry matches one the SQL path would produce.

use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Matches `jsonb_field_rows`' `max_depth` guard against pathologically nested documents.
const MAX_DEPTH: u32 = 20;

/// The accumulated distinct paths (and their merged leaf flag + value kinds) observed across the items of a
/// single collection. `is_leaf` is AND-merged (a path is a leaf only if it never gains children); value kinds
/// only accumulate — so merging more items only ever widens, never narrows.
#[derive(Default)]
pub(crate) struct FieldRegistry {
    paths: BTreeMap<String, Entry>,
}

#[derive(Default)]
struct Entry {
    is_leaf: bool,
    kinds: BTreeSet<&'static str>,
}

impl FieldRegistry {
    /// Walk one item's content, merging its paths into the registry.
    pub(crate) fn observe(&mut self, content: &Value) {
        let mut path = String::new();
        walk(
            content,
            &mut path,
            MAX_DEPTH,
            &mut |p, is_leaf, kind| match self.paths.get_mut(p) {
                Some(e) => {
                    e.is_leaf = e.is_leaf && is_leaf;
                    let _ = e.kinds.insert(kind);
                }
                None => {
                    let mut kinds = BTreeSet::new();
                    let _ = kinds.insert(kind);
                    let _ = self.paths.insert(p.to_string(), Entry { is_leaf, kinds });
                }
            },
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Render as the JSONB array the loader's `item_field_registry` UPSERT expects:
    /// `[{"path": text, "is_leaf": bool, "value_kinds": [text, ...]}, ...]`.
    pub(crate) fn to_entries(&self) -> Value {
        Value::Array(
            self.paths
                .iter()
                .map(|(path, e)| {
                    let mut o = Map::new();
                    let _ = o.insert("path".into(), Value::String(path.clone()));
                    let _ = o.insert("is_leaf".into(), Value::Bool(e.is_leaf));
                    let _ = o.insert(
                        "value_kinds".into(),
                        Value::Array(e.kinds.iter().map(|k| Value::String((*k).into())).collect()),
                    );
                    Value::Object(o)
                })
                .collect(),
        )
    }
}

/// `jsonb_typeof` for a serde_json value.
fn kind(v: &Value) -> &'static str {
    match v {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
    }
}

/// Recursively emit one `(path, is_leaf, value_kind)` per field path, mirroring SQL `jsonb_field_rows`:
/// objects emit each key (a container key emits its own non-leaf row AND recurses); arrays recurse into
/// object elements under the SAME path (arrays of scalars are already covered by the container row above);
/// `path` is a reused buffer (push the `.key` segment, recurse, truncate back).
fn walk(
    value: &Value,
    path: &mut String,
    depth: u32,
    emit: &mut impl FnMut(&str, bool, &'static str),
) {
    if depth == 0 {
        return;
    }
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let len = path.len();
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(k);
                match v {
                    Value::Object(_) | Value::Array(_) => {
                        emit(path, false, kind(v));
                        walk(v, path, depth - 1, emit);
                    }
                    _ => emit(path, true, kind(v)),
                }
                path.truncate(len);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                if matches!(v, Value::Object(_)) {
                    walk(v, path, depth - 1, emit);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn walk_matches_jsonb_field_rows_semantics() {
        // object keys, nested object, array-of-objects (recurses under the same path), array-of-scalars
        // (only the container row), and a scalar leaf.
        let item = json!({
            "id": "x",
            "geometry": {"type": "Point", "coordinates": [1, 2]},
            "properties": {"datetime": "2020-01-01T00:00:00Z", "eo:cloud_cover": 10},
            "assets": [{"href": "a"}, {"href": "b", "roles": ["data"]}]
        });
        let mut reg = FieldRegistry::default();
        reg.observe(&item);
        let paths: Vec<&str> = reg.paths.keys().map(String::as_str).collect();
        // leaf scalars
        assert!(paths.contains(&"id"));
        assert!(paths.contains(&"properties.datetime"));
        assert!(paths.contains(&"properties.eo:cloud_cover"));
        // containers emit their own (non-leaf) row + recurse
        assert!(paths.contains(&"geometry"));
        assert!(paths.contains(&"geometry.type"));
        assert!(paths.contains(&"geometry.coordinates")); // array -> non-leaf row
        // array of objects: recurse under the same path, merged across elements
        assert!(paths.contains(&"assets")); // the array container
        assert!(paths.contains(&"assets.href"));
        assert!(paths.contains(&"assets.roles"));
        // leaf/kind merges
        assert!(reg.paths["id"].is_leaf);
        assert!(!reg.paths["geometry"].is_leaf);
        assert_eq!(
            reg.paths["properties.eo:cloud_cover"]
                .kinds
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec!["number"]
        );
    }

    #[test]
    fn merge_only_widens() {
        let mut reg = FieldRegistry::default();
        reg.observe(&json!({"a": 1}));
        reg.observe(&json!({"a": "s", "b": true})); // a gains a second kind; b is new
        assert_eq!(
            reg.paths["a"].kinds.iter().copied().collect::<Vec<_>>(),
            vec!["number", "string"]
        );
        assert!(reg.paths.contains_key("b"));
    }
}
