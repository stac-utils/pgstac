//! Rust-side dehydration of a full STAC item into pgstac's split `items`-row columns.
//!
//! This is the inverse of [`hydrate`](crate::hydrate): it takes a self-contained STAC item and produces
//! the column values pgstac stores — promoted queryable columns, the residual `properties`/`extra`, the
//! EWKB geometry, the temporal columns, and the canonical [`item_hash`](crate::canonical). It must match
//! the SQL `content_dehydrate` (003a_items.sql) so an item ingested through Rust is byte-identical to one
//! ingested through the SQL path; that is gated by `tests/dehydrate_parity.rs`.
//!
//! Fragment extraction (factoring shared content into `item_fragments`) is layered on top separately;
//! this module produces the pre-fragment row (`fragment_id = None`, full `assets`/`properties`/`links`),
//! exactly like `content_dehydrate`.
//!
//! Allocation discipline: the input item is consumed and its sub-values are **moved** out of the JSON
//! map (`Map::remove`); per-collection context (the promoted-column [`DehydrateSchema`]) is shared by
//! reference / `Arc`, never deep-cloned per item.

use crate::canonical::jsonb_hash;
use crate::geom::geojson_to_ewkb;
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use tokio_postgres::GenericClient;

/// Top-level item keys that map to dedicated `items` columns and so are removed before the remainder
/// becomes `extra`. Mirrors the `content - '{...}'` set in `content_dehydrate`.
const KNOWN_TOP_LEVEL: &[&str] = &[
    "id",
    "geometry",
    "collection",
    "type",
    "bbox",
    "links",
    "assets",
    "properties",
    "stac_version",
    "stac_extensions",
];

/// The Postgres storage type of a promoted column, controlling how its property value is coerced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotedKind {
    /// `text`.
    Text,
    /// `float8` / `float4` / `numeric`.
    Float,
    /// `int4` / `int2`.
    Int,
    /// `int8`.
    BigInt,
    /// `text[]`.
    TextArray,
    /// `jsonb` (stored verbatim).
    Jsonb,
    /// `timestamptz`.
    Timestamptz,
}

impl PromotedKind {
    fn from_udt(udt: &str) -> PromotedKind {
        match udt {
            "text" | "varchar" | "bpchar" => PromotedKind::Text,
            "float8" | "float4" | "numeric" => PromotedKind::Float,
            "int4" | "int2" => PromotedKind::Int,
            "int8" => PromotedKind::BigInt,
            "_text" | "_varchar" => PromotedKind::TextArray,
            "timestamptz" | "timestamp" => PromotedKind::Timestamptz,
            _ => PromotedKind::Jsonb,
        }
    }
}

/// One promoted queryable column: its STAC property name, the `items` column, and the column type.
#[derive(Debug, Clone)]
pub struct PromotedColumn {
    /// STAC property name in `properties`, e.g. `eo:cloud_cover`.
    pub stac_name: String,
    /// `items` column name, e.g. `eo_cloud_cover`.
    pub column: String,
    /// The column's storage type.
    pub kind: PromotedKind,
}

/// The promoted-column schema, derived at runtime from `promoted_item_property_defs()` joined with the
/// `items` column types — the ordered `(stac_name, column, kind)` mapping the dehydrate promotes out of
/// `properties`. Loaded once per ingest and shared (by reference / `Arc`) across all items.
#[derive(Debug, Clone)]
pub struct DehydrateSchema {
    columns: Vec<PromotedColumn>,
}

impl DehydrateSchema {
    /// Loads the promoted-column schema from the live database.
    pub async fn load<C: GenericClient>(client: &C) -> Result<DehydrateSchema> {
        let type_rows = client
            .query(
                "SELECT column_name, udt_name FROM information_schema.columns \
                 WHERE table_schema = 'pgstac' AND table_name = 'items'",
                &[],
            )
            .await?;
        let mut types = std::collections::HashMap::new();
        for row in &type_rows {
            let name: String = row.get("column_name");
            let udt: String = row.get("udt_name");
            let _ = types.insert(name, udt);
        }

        let def_rows = client
            .query(
                "SELECT name, property_path FROM promoted_item_property_defs() \
                 WITH ORDINALITY AS d(name, definition, property_path, ord) ORDER BY ord",
                &[],
            )
            .await?;
        let mut columns = Vec::with_capacity(def_rows.len());
        for row in &def_rows {
            let stac_name: String = row.get("name");
            let column: String = row.get("property_path");
            let kind = types
                .get(&column)
                .map(|u| PromotedKind::from_udt(u))
                .unwrap_or(PromotedKind::Jsonb);
            columns.push(PromotedColumn {
                stac_name,
                column,
                kind,
            });
        }
        Ok(DehydrateSchema { columns })
    }

    /// The promoted columns in `promoted_item_property_defs()` order.
    pub fn columns(&self) -> &[PromotedColumn] {
        &self.columns
    }
}

/// A coerced promoted-column value, ready for a typed binary COPY into `items`.
#[derive(Debug, Clone, PartialEq)]
pub enum PromotedValue {
    /// A `text` value (`None` = SQL NULL).
    Text(Option<String>),
    /// A `float8` value.
    Float(Option<f64>),
    /// An `int4` value.
    Int(Option<i32>),
    /// An `int8` value.
    BigInt(Option<i64>),
    /// A `text[]` value.
    TextArray(Option<Vec<String>>),
    /// A `jsonb` value (stored verbatim).
    Jsonb(Option<Value>),
    /// A `timestamptz` value.
    Timestamptz(Option<DateTime<Utc>>),
}

/// A fully dehydrated `items` row, ready for a binary COPY. Jsonb columns are kept as
/// [`Value`](serde_json::Value); `geometry` is EWKB (SRID 4326).
#[derive(Debug, Clone)]
pub struct DehydratedRow {
    /// `items.id`.
    pub id: String,
    /// `items.collection`.
    pub collection: String,
    /// `items.geometry` as EWKB bytes (SRID 4326).
    pub geometry: Vec<u8>,
    /// `items.datetime` (start_datetime for ranges).
    pub datetime: DateTime<Utc>,
    /// `items.end_datetime`.
    pub end_datetime: DateTime<Utc>,
    /// `items.datetime_is_range`.
    pub datetime_is_range: bool,
    /// `items.stac_version`.
    pub stac_version: Option<String>,
    /// `items.stac_extensions` (defaults to `[]`).
    pub stac_extensions: Value,
    /// `items.item_hash` — the canonical content hash.
    pub item_hash: [u8; 32],
    /// `items.fragment_id` — always `None` from this (pre-fragment) dehydrate.
    pub fragment_id: Option<i64>,
    /// `items.bbox`.
    pub bbox: Option<Value>,
    /// `items.links` (`None` when absent or empty `[]`).
    pub links: Option<Value>,
    /// `items.assets` (`None` when absent or empty `{}`).
    pub assets: Option<Value>,
    /// `items.properties` — the residual after promoting columns and removing temporal keys.
    pub properties: Value,
    /// `items.extra` — top-level keys outside the known column set.
    pub extra: Value,
    /// `items.link_hrefs` — the per-link `href`s (NULL entries preserved).
    pub link_hrefs: Option<Vec<Option<String>>>,
    /// Promoted queryable columns, in schema order, aligned with `schema.columns()`.
    pub promoted: Vec<PromotedValue>,
}

/// Dehydrates one self-contained STAC item into a [`DehydratedRow`], consuming `item`.
///
/// Matches SQL `content_dehydrate(item)`: computes the canonical `item_hash` over the whole item, splits
/// the geometry to EWKB, derives the temporal columns ([`stac_daterange`] semantics), promotes the
/// queryable columns out of `properties`, and routes the remaining top-level keys to `extra`.
pub fn dehydrate(item: Value, schema: &DehydrateSchema) -> Result<DehydratedRow> {
    // The hash is over the full item, so compute it before deconstructing (borrow, no clone).
    let item_hash = jsonb_hash(&item);

    let mut map = match item {
        Value::Object(map) => map,
        _ => return Err(Error::Dehydrate("item is not a JSON object".to_string())),
    };

    let id =
        take_string(&mut map, "id").ok_or_else(|| Error::Dehydrate("item has no id".into()))?;
    let collection = take_string(&mut map, "collection")
        .ok_or_else(|| Error::Dehydrate(format!("item {id} has no collection")))?;

    let geometry_value = map
        .remove("geometry")
        .filter(|v| !v.is_null())
        .ok_or_else(|| Error::Dehydrate(format!("item {id} has no geometry")))?;
    let geometry = geojson_to_ewkb(&geometry_value)?;

    let stac_version = take_string(&mut map, "stac_version");
    let stac_extensions = map
        .remove("stac_extensions")
        .filter(|v| !v.is_null())
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let bbox = map.remove("bbox").filter(|v| !v.is_null());
    let links_raw = map.remove("links");
    let assets = map
        .remove("assets")
        .filter(|v| !v.is_null() && v.as_object().is_none_or(|o| !o.is_empty()));
    let _ = map.remove("type");

    // `properties` is taken out as a map we can mutate (promote + strip temporal keys).
    let mut properties = match map.remove("properties") {
        Some(Value::Object(p)) => p,
        _ => Map::new(),
    };

    // Temporal columns: stac_daterange semantics over `properties`.
    let (datetime, end_datetime, datetime_is_range) = temporal(&properties, &id)?;

    // Promote each queryable column out of `properties` (removing it), coercing per the column type.
    let mut promoted = Vec::with_capacity(schema.columns.len());
    for col in &schema.columns {
        let raw = properties.remove(&col.stac_name).filter(|v| !v.is_null());
        promoted.push(coerce_promoted(raw, col.kind, &col.stac_name, &id)?);
    }
    // strip_promoted_properties also removes the temporal keys.
    let _ = properties.remove("datetime");
    let _ = properties.remove("start_datetime");
    let _ = properties.remove("end_datetime");

    // links column: NULL when absent or empty `[]`. Compute link_hrefs (borrow) before moving links_raw.
    let link_hrefs = link_hrefs_of(links_raw.as_ref());
    let links = links_raw.filter(|v| v.as_array().is_some_and(|a| !a.is_empty()));

    // Everything left at the top level is `extra`.
    for key in KNOWN_TOP_LEVEL {
        let _ = map.remove(*key);
    }
    let extra = Value::Object(map);

    Ok(DehydratedRow {
        id,
        collection,
        geometry,
        datetime,
        end_datetime,
        datetime_is_range,
        stac_version,
        stac_extensions,
        item_hash,
        fragment_id: None,
        bbox,
        links,
        assets,
        properties: Value::Object(properties),
        extra,
        link_hrefs,
        promoted,
    })
}

/// Removes a string-valued key from `map`, returning the owned `String` (or `None` if absent/non-string).
fn take_string(map: &mut Map<String, Value>, key: &str) -> Option<String> {
    match map.remove(key) {
        Some(Value::String(s)) => Some(s),
        _ => None,
    }
}

/// Computes `(datetime, end_datetime, datetime_is_range)` from `properties`, matching `stac_daterange`
/// + `content_dehydrate`'s `datetime_is_range`.
fn temporal(props: &Map<String, Value>, id: &str) -> Result<(DateTime<Utc>, DateTime<Utc>, bool)> {
    let datetime_present = props.get("datetime").is_some_and(|v| !v.is_null());
    let start = props.get("start_datetime").filter(|v| !v.is_null());
    let end = props.get("end_datetime").filter(|v| !v.is_null());

    let (dt_v, edt_v) = if start.is_some() && end.is_some() {
        (start, end)
    } else {
        let dt = props.get("datetime").filter(|v| !v.is_null());
        (dt, dt)
    };
    let dt = parse_stac_datetime(dt_v, id)?;
    let edt = parse_stac_datetime(edt_v, id)?;

    let datetime_is_range = if datetime_present {
        false
    } else {
        start.is_some() || end.is_some()
    };
    Ok((dt, edt, datetime_is_range))
}

/// Parses a STAC datetime [`Value`] (an RFC 3339 string) into a UTC timestamp.
fn parse_stac_datetime(v: Option<&Value>, id: &str) -> Result<DateTime<Utc>> {
    let s = v.and_then(Value::as_str).ok_or_else(|| {
        Error::Dehydrate(format!(
            "item {id}: datetime, or both start_datetime and end_datetime, must be set"
        ))
    })?;
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::Dehydrate(format!("item {id}: invalid datetime {s:?}: {e}")))
}

/// Coerces a promoted property value to its column type, matching `content_dehydrate`'s per-column casts.
fn coerce_promoted(
    raw: Option<Value>,
    kind: PromotedKind,
    stac_name: &str,
    id: &str,
) -> Result<PromotedValue> {
    let bad =
        |what: &str| Error::Dehydrate(format!("item {id}: property {stac_name} is not {what}"));
    Ok(match (kind, raw) {
        (PromotedKind::Text, Some(v)) => PromotedValue::Text(Some(value_to_text(v))),
        (PromotedKind::Text, None) => PromotedValue::Text(None),
        (PromotedKind::Float, Some(v)) => {
            PromotedValue::Float(Some(value_to_f64(&v).ok_or_else(|| bad("a number"))?))
        }
        (PromotedKind::Float, None) => PromotedValue::Float(None),
        (PromotedKind::Int, Some(v)) => PromotedValue::Int(Some(
            value_to_i64(&v).ok_or_else(|| bad("an integer"))? as i32,
        )),
        (PromotedKind::Int, None) => PromotedValue::Int(None),
        (PromotedKind::BigInt, Some(v)) => {
            PromotedValue::BigInt(Some(value_to_i64(&v).ok_or_else(|| bad("an integer"))?))
        }
        (PromotedKind::BigInt, None) => PromotedValue::BigInt(None),
        (PromotedKind::TextArray, Some(v)) => {
            PromotedValue::TextArray(Some(value_to_text_array(v)))
        }
        (PromotedKind::TextArray, None) => PromotedValue::TextArray(None),
        (PromotedKind::Timestamptz, Some(v)) => {
            PromotedValue::Timestamptz(Some(parse_stac_datetime(Some(&v), id)?))
        }
        (PromotedKind::Timestamptz, None) => PromotedValue::Timestamptz(None),
        (PromotedKind::Jsonb, raw) => PromotedValue::Jsonb(raw),
    })
}

/// `->>` semantics: a JSON string yields its contents; any other scalar yields its JSON text.
fn value_to_text(v: Value) -> String {
    match v {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn value_to_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// `to_text_array`: an array yields each element as text; a scalar yields a one-element array.
fn value_to_text_array(v: Value) -> Vec<String> {
    match v {
        Value::Array(items) => items.into_iter().map(value_to_text).collect(),
        other => vec![value_to_text(other)],
    }
}

/// Extracts each link's `href` (NULL when absent), matching `stac_links_href_array`. Returns `None` when
/// there are no links.
fn link_hrefs_of(links: Option<&Value>) -> Option<Vec<Option<String>>> {
    let arr = links?.as_array()?;
    if arr.is_empty() {
        return None;
    }
    Some(
        arr.iter()
            .map(|link| link.get("href").and_then(Value::as_str).map(str::to_string))
            .collect(),
    )
}
