//! Minting keyset pagination tokens in Rust, mirroring SQL `keyset_encode` / `keyset_sortkeys`.
//!
//! A token is the base64 of the boundary row's sort-key values, joined by `chr(31)` with NULL encoded
//! as `chr(30)`. The values come from the **hydrated feature** (the sort keys are item fields), so no
//! extra server round trip is needed. On the next request the server `keyset_decode`s the token and
//! `keyset_where` casts each value back to its column type, so the rendering only has to round-trip —
//! it does not have to be byte-identical to what SQL `search()` would emit.

use base64::Engine;
use serde_json::{Value, json};
use std::collections::HashSet;

/// The unit separator (`chr(31)`) joining keyset values.
const UNIT_SEP: &str = "\u{1f}";
/// The record separator (`chr(30)`) standing in for a NULL value.
const NULL_SENTINEL: &str = "\u{1e}";

/// The ordered sort-key fields for a search: `sortby` fields, then appended `id` and `collection`,
/// deduped keeping the first occurrence. Mirrors `keyset_sortkeys` (direction is irrelevant to the
/// encoded value, so only the field order matters here).
///
/// # Examples
///
/// ```
/// use pgstac::keyset::sort_key_fields;
/// use serde_json::json;
///
/// // Default sort is by datetime, then the id/collection tiebreaks are appended.
/// assert_eq!(sort_key_fields(&json!({})), ["datetime", "id", "collection"]);
/// // An explicit id sort dedupes the appended id.
/// assert_eq!(
///     sort_key_fields(&json!({"sortby": [{"field": "id", "direction": "asc"}]})),
///     ["id", "collection"]
/// );
/// ```
pub fn sort_key_fields(search: &Value) -> Vec<String> {
    let default = json!([{"field": "datetime", "direction": "desc"}]);
    let sortby = search
        .get("sortby")
        .filter(|v| v.as_array().is_some_and(|a| !a.is_empty()))
        .unwrap_or(&default);

    let mut fields: Vec<String> = sortby
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e.get("field").and_then(Value::as_str).map(String::from))
        .collect();
    fields.push("id".to_string());
    fields.push("collection".to_string());

    let mut seen = HashSet::new();
    fields.retain(|f| seen.insert(f.clone()));
    fields
}

/// The keyset value text for one sort-key field, pulled from a hydrated feature.
fn keyset_value(feature: &Value, field: &str) -> Option<String> {
    let props = &feature["properties"];
    match field {
        "id" => feature["id"].as_str().map(String::from),
        "collection" => feature["collection"].as_str().map(String::from),
        // The keyset uses the `datetime` column, which is `start_datetime` for range items (whose
        // `properties.datetime` is null).
        "datetime" => props["datetime"]
            .as_str()
            .or_else(|| props["start_datetime"].as_str())
            .map(String::from),
        "end_datetime" => props["end_datetime"]
            .as_str()
            .or_else(|| props["datetime"].as_str())
            .map(String::from),
        other => json_text(&props[other]),
    }
}

/// Renders a JSON value to the text Postgres `->>` would produce (for property sort keys).
fn json_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

/// Encodes keyset values into a token, matching SQL `keyset_encode`.
///
/// # Examples
///
/// ```
/// use pgstac::keyset::keyset_encode;
///
/// // base64("a" + chr(31) + "b")
/// assert_eq!(keyset_encode(&[Some("a".into()), Some("b".into())]), "YR9i");
/// // A NULL becomes chr(30): base64("a" + chr(31) + chr(30))
/// assert_eq!(keyset_encode(&[Some("a".into()), None]), "YR8e");
/// ```
pub fn keyset_encode(values: &[Option<String>]) -> String {
    let joined = values
        .iter()
        .map(|v| v.as_deref().unwrap_or(NULL_SENTINEL))
        .collect::<Vec<_>>()
        .join(UNIT_SEP);
    base64::engine::general_purpose::STANDARD.encode(joined.as_bytes())
}

/// Mints the keyset token for a hydrated feature given the search's ordered sort-key `fields`.
pub fn mint_token(feature: &Value, fields: &[String]) -> String {
    let values: Vec<Option<String>> = fields.iter().map(|f| keyset_value(feature, f)).collect();
    keyset_encode(&values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_matches_sql_keyset_encode() {
        assert_eq!(keyset_encode(&[Some("a".into()), Some("b".into())]), "YR9i");
        assert_eq!(keyset_encode(&[Some("a".into()), None]), "YR8e");
    }

    #[test]
    fn mint_uses_datetime_then_id_collection() {
        let feature = json!({
            "id": "item-1",
            "collection": "coll-a",
            "properties": {"datetime": "2013-04-19T00:00:00Z"}
        });
        let fields = sort_key_fields(&json!({}));
        let token = mint_token(&feature, &fields);
        // datetime + chr(31) + id + chr(31) + collection
        let expected = keyset_encode(&[
            Some("2013-04-19T00:00:00Z".into()),
            Some("item-1".into()),
            Some("coll-a".into()),
        ]);
        assert_eq!(token, expected);
    }

    #[test]
    fn range_item_uses_start_datetime_for_keyset() {
        let feature = json!({
            "id": "r1",
            "collection": "c",
            "properties": {"datetime": null, "start_datetime": "2020-01-01T00:00:00Z", "end_datetime": "2020-01-02T00:00:00Z"}
        });
        assert_eq!(
            keyset_value(&feature, "datetime").as_deref(),
            Some("2020-01-01T00:00:00Z")
        );
    }
}
