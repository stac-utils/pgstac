//! Fragment split for collections with a `fragment_config` (the 0.10 asset-dedup model).
//!
//! Ports the SQL `extract_fragment` / `strip_fragment_col` (003a_items.sql) so the Rust loader can split
//! an item the same way `items_staging_dehydrate` does: the configured paths (stable per-collection asset
//! metadata) move into a shared `item_fragments` row, and the per-item columns keep only what differs.
//!
//! This module is the pure, parity-tested core. Wiring it into the loader (dedup → `ensure_fragments` →
//! stamp `fragment_id`, plus links-template / stac_version / stac_extensions handling) is layered on top.

use crate::Result;
use serde_json::{Map, Value};

/// A collection's fragment paths, parsed from `collections.fragment_config`.
///
/// The stored form is a `text[]` whose elements are JSON arrays of path segments, e.g.
/// `["assets","B1","type"]` (see SQL `fragment_path_text`); the array form avoids ambiguity for keys
/// containing dots.
///
/// # Examples
///
/// ```
/// use pgstac::fragment::FragmentConfig;
/// let config = FragmentConfig::parse(&[r#"["assets","B1","type"]"#.to_string()]).unwrap();
/// assert!(!config.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct FragmentConfig {
    paths: Vec<Vec<String>>,
}

impl FragmentConfig {
    /// Parses the `text[]` config — each element a JSON array of path segments.
    pub fn parse(config: &[String]) -> Result<FragmentConfig> {
        let mut paths = Vec::with_capacity(config.len());
        for entry in config {
            paths.push(serde_json::from_str(entry)?);
        }
        Ok(FragmentConfig { paths })
    }

    /// Whether the collection fragments nothing (no shared metadata to extract).
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// The parsed path arrays.
    pub fn paths(&self) -> &[Vec<String>] {
        &self.paths
    }

    /// Whether the config contains exactly this path (e.g. `["stac_version"]`), used to decide whether a
    /// top-level column is fragmented wholesale.
    pub fn has_path(&self, path: &[&str]) -> bool {
        self.paths
            .iter()
            .any(|p| p.len() == path.len() && p.iter().zip(path).all(|(a, b)| a == b))
    }
}

/// Gets the value at a root-relative object path — mirrors `content #> path` for object keys (fragment
/// paths are always object keys, never array indices).
fn get_path<'a>(content: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cur = content;
    for key in path {
        cur = cur.as_object()?.get(key)?;
    }
    Some(cur)
}

/// Sets `val` at `path` in `result`, creating intermediate objects as needed (mirrors `jsonb_set_nested`,
/// so depth-3+ paths sharing intermediate keys merge rather than overwrite).
fn set_path(result: &mut Map<String, Value>, path: &[String], val: Value) {
    let (last, parents) = match path.split_last() {
        Some(parts) => parts,
        None => return,
    };
    let mut cur = result;
    for key in parents {
        let child = cur
            .entry(key.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !child.is_object() {
            *child = Value::Object(Map::new());
        }
        cur = child.as_object_mut().expect("just ensured object");
    }
    let _ = cur.insert(last.clone(), val);
}

/// Removes a nested object path, mirroring the `#-` operator for object keys.
fn remove_path(value: &mut Value, path: &[String]) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    if rest.is_empty() {
        let _ = obj.remove(first);
    } else if let Some(child) = obj.get_mut(first) {
        remove_path(child, rest);
    }
}

/// The sparse fragment overlay: every configured path present in `content`, reassembled into a nested
/// object. Returns `None` when no configured path is present (matches SQL `extract_fragment` returning
/// NULL for an empty result).
///
/// Clones the extracted values into the overlay (the same values are also stripped from the per-item
/// columns by [`strip_fragment_col`]); this is the deferred dedup path, not the per-item hot path.
pub fn extract_fragment(content: &Value, config: &FragmentConfig) -> Option<Value> {
    if config.is_empty() {
        return None;
    }
    let mut result = Map::new();
    for path in config.paths() {
        if path.is_empty() {
            continue;
        }
        if let Some(val) = get_path(content, path) {
            set_path(&mut result, path, val.clone());
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(Value::Object(result))
    }
}

/// Removes the fragment-owned sub-keys for `col_name` from `col_value` (mirrors SQL `strip_fragment_col`):
/// a depth-1 path equal to `col_name` zeroes the whole column (`{}`); deeper paths remove the nested key.
pub fn strip_fragment_col(mut col_value: Value, col_name: &str, config: &FragmentConfig) -> Value {
    for path in config.paths() {
        if path.first().map(String::as_str) != Some(col_name) {
            continue;
        }
        if path.len() == 1 {
            return Value::Object(Map::new());
        }
        remove_path(&mut col_value, &path[1..]);
    }
    col_value
}

/// Builds the fragment payload for an item — `{content?, links_template?}` — that `ensure_fragments`
/// hashes + dedups (mirrors the SQL `items_staging_dehydrate` `fragmented` CTE's
/// `jsonb_strip_nulls(jsonb_build_object('content', NULLIF(frag_content,'{}'), 'links_template', ...))`).
///
/// `content` is [`extract_fragment`] (already `None` for an empty overlay); `links_template` is
/// [`strip_link_hrefs`] of the item's `links`. Returns `None` when both are absent (the item has no
/// fragment — `fragment_id` stays NULL). Only the present keys appear, matching `jsonb_strip_nulls`.
pub fn build_fragment_payload(item: &Value, config: &FragmentConfig) -> Option<Value> {
    let content = extract_fragment(item, config);
    let links_template = item.get("links").and_then(strip_link_hrefs);
    if content.is_none() && links_template.is_none() {
        return None;
    }
    let mut payload = Map::new();
    if let Some(content) = content {
        let _ = payload.insert("content".to_string(), content);
    }
    if let Some(links_template) = links_template {
        let _ = payload.insert("links_template".to_string(), links_template);
    }
    Some(Value::Object(payload))
}

/// The links template: each link with its per-item `href` removed, preserving order (mirrors SQL
/// `stac_links_strip_hrefs`). Returns `None` for a null/non-array/empty `links` value. The template is the
/// shared part of an item's links that goes into the fragment; the per-item hrefs stay in `link_hrefs`.
pub fn strip_link_hrefs(links: &Value) -> Option<Value> {
    let array = links.as_array()?;
    if array.is_empty() {
        return None;
    }
    let stripped = array
        .iter()
        .map(|link| match link.as_object() {
            Some(object) => {
                let mut object = object.clone();
                let _ = object.remove("href");
                Value::Object(object)
            }
            None => link.clone(),
        })
        .collect();
    Some(Value::Array(stripped))
}
