//! STAC `fields` include/exclude projection, replicating pgstac's `jsonb_fields`.
//!
//! `jsonb_fields(j, f) = jsonb_exclude(jsonb_include(j, f), f)`:
//! * **include** — when `fields.include` is a non-empty array, the output keeps only those dot-paths
//!   (plus the always-required `id` and `collection`); otherwise the item passes through unchanged.
//! * **exclude** — every `fields.exclude` dot-path is then removed.
//!
//! Paths are split on `.` (e.g. `properties.eo:cloud_cover`); numeric segments index arrays.

use serde_json::{Map, Value};

/// Applies a STAC `fields` object (`{ "include": [...], "exclude": [...] }`) to a hydrated item,
/// matching pgstac's `jsonb_fields`.
///
/// # Examples
///
/// ```
/// use pgstac::fields::apply_fields;
/// use serde_json::json;
///
/// let item = json!({
///     "id": "x", "collection": "c",
///     "assets": {"data": {"href": "h"}},
///     "properties": {"datetime": "2020-01-01T00:00:00Z", "eo:cloud_cover": 5}
/// });
///
/// // Exclude drops the listed dot-paths.
/// let trimmed = apply_fields(item.clone(), &json!({"exclude": ["assets", "properties.eo:cloud_cover"]}));
/// assert!(trimmed.get("assets").is_none());
/// assert_eq!(trimmed["properties"]["datetime"], "2020-01-01T00:00:00Z");
///
/// // Include keeps only the listed paths, plus the always-required id/collection.
/// let narrowed = apply_fields(item, &json!({"include": ["properties.datetime"]}));
/// assert_eq!(narrowed["id"], "x");
/// assert!(narrowed.get("assets").is_none());
/// ```
pub fn apply_fields(item: Value, fields: &Value) -> Value {
    exclude(include(item, fields), fields)
}

/// Splits a `fields` list into dot-path segment vectors.
fn dotpaths(list: Option<&Value>) -> Vec<Vec<String>> {
    list.and_then(Value::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(Value::as_str)
                .map(|s| s.split('.').map(String::from).collect())
                .collect()
        })
        .unwrap_or_default()
}

/// `jsonb_include`: keep only the included dot-paths (+ `id`/`collection`), or pass through when there
/// is no include list.
fn include(item: Value, fields: &Value) -> Value {
    let mut paths = dotpaths(fields.get("include"));
    if paths.is_empty() {
        return item;
    }
    paths.push(vec!["id".to_string()]);
    if item.get("collection").is_some() {
        paths.push(vec!["collection".to_string()]);
    }
    let mut out = Value::Object(Map::new());
    for path in &paths {
        let value = get_path(&item, path).cloned().unwrap_or(Value::Null);
        set_nested(&mut out, path, value);
    }
    out
}

/// `jsonb_exclude`: remove each excluded dot-path.
fn exclude(mut item: Value, fields: &Value) -> Value {
    for path in dotpaths(fields.get("exclude")) {
        remove_path(&mut item, &path);
    }
    item
}

/// Reads the value at a dot-path (object key, or array index for a numeric segment).
fn get_path<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = match current {
            Value::Object(map) => map.get(segment)?,
            Value::Array(array) => array.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Sets `value` at a dot-path, creating intermediate objects (`jsonb_set_nested`).
fn set_nested(out: &mut Value, path: &[String], value: Value) {
    if !out.is_object() {
        *out = Value::Object(Map::new());
    }
    let mut current = out;
    for (i, segment) in path.iter().enumerate() {
        let map = current.as_object_mut().expect("intermediate is an object");
        if i == path.len() - 1 {
            let _ = map.insert(segment.clone(), value);
            return;
        }
        let entry = map
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        current = entry;
    }
}

/// Removes the value at a dot-path (`#-`).
fn remove_path(value: &mut Value, path: &[String]) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut current = value;
    for segment in parents {
        current = match current {
            Value::Object(map) => match map.get_mut(segment) {
                Some(child) => child,
                None => return,
            },
            Value::Array(array) => {
                match segment.parse::<usize>().ok().and_then(|i| array.get_mut(i)) {
                    Some(child) => child,
                    None => return,
                }
            }
            _ => return,
        };
    }
    match current {
        Value::Object(map) => {
            let _ = map.remove(last);
        }
        Value::Array(array) => {
            if let Ok(i) = last.parse::<usize>()
                && i < array.len()
            {
                let _ = array.remove(i);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item() -> Value {
        json!({
            "id": "i", "collection": "c", "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [1, 2]},
            "assets": {"data": {"href": "x"}},
            "properties": {"datetime": "2020-01-01T00:00:00Z", "eo:cloud_cover": 5, "platform": "l8"}
        })
    }

    #[test]
    fn exclude_removes_paths() {
        let out = apply_fields(
            item(),
            &json!({"exclude": ["assets", "properties.eo:cloud_cover"]}),
        );
        assert!(out.get("assets").is_none());
        assert!(out["properties"].get("eo:cloud_cover").is_none());
        assert_eq!(out["properties"]["platform"], "l8");
    }

    #[test]
    fn include_keeps_only_paths_plus_id_collection() {
        let out = apply_fields(item(), &json!({"include": ["properties.datetime"]}));
        let keys: Vec<&String> = out.as_object().unwrap().keys().collect();
        assert!(keys.contains(&&"id".to_string()));
        assert!(keys.contains(&&"collection".to_string()));
        assert!(keys.contains(&&"properties".to_string()));
        assert!(out.get("geometry").is_none());
        assert_eq!(out["properties"]["datetime"], "2020-01-01T00:00:00Z");
        assert!(out["properties"].get("platform").is_none());
    }

    #[test]
    fn empty_fields_pass_through() {
        assert_eq!(apply_fields(item(), &json!({})), item());
    }
}
