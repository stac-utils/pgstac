//! Rust-side hydration of dehydrated pgstac item rows back into full STAC items.
//!
//! pgstac stores items in a "dehydrated" form: shared content is factored out into
//! a per-collection `base_item` (pgstac 0.9.11) or shared `item_fragments`
//! (pgstac 0.10), and a fixed set of queryable properties is promoted to dedicated
//! columns. [content_hydrate] reassembles the full self-contained STAC item.
//!
//! This module reproduces that reassembly client-side so a dump can emit fully
//! hydrated items without a per-row server round trip. It targets the **export
//! path only**, where the requested fields are always the full item (`fields =
//! {}`), so the include/exclude projection that `content_hydrate(_, fields)`
//! applies is the identity and is intentionally omitted here.
//!
//! Two storage models are supported:
//!
//! * [`HydrationModel::BaseItem`] — pgstac 0.9.11. The dehydrated row carries the
//!   item `content` jsonb; hydration deep-merges it over the collection
//!   `base_item` using the same precedence and deletion-sentinel rules as the SQL
//!   `merge_jsonb`.
//! * [`HydrationModel::Fragment`] — pgstac 0.10. Promoted columns are folded back
//!   into `properties`, optional shared fragment content is recursively merged in
//!   (item wins), and per-item link hrefs are spliced into the fragment's link
//!   template.
//!
//! The output must equal the SQL `content_hydrate` over the same rows; this is
//! gated by parity tests in `tests/hydrate_parity.rs`.
//!
//! [content_hydrate]: https://github.com/stac-utils/pgstac/blob/main/src/pgstac/sql/003a_items.sql

use crate::rawjson::RawJson;
use serde_json::{Map, Value, json};

/// Which pgstac storage model a dehydrated row came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationModel {
    /// pgstac 0.9.11: per-collection `base_item` deep-merge.
    BaseItem,
    /// pgstac 0.10: promoted columns + shared `item_fragments`.
    Fragment,
}

/// Per-collection context needed to hydrate an item.
///
/// For [`HydrationModel::BaseItem`] only `base_item` is used. For
/// [`HydrationModel::Fragment`] the fragment (looked up by the row's
/// `fragment_id`) is supplied per row, so this carries nothing model-specific.
#[derive(Debug, Default, Clone)]
pub struct CollectionContext {
    /// The collection's `base_item` jsonb (0.9.11). `None`/`null` on 0.10.
    pub base_item: Option<Value>,
}

/// The shared fragment content for a 0.10 item, looked up by `fragment_id`.
#[derive(Debug, Default, Clone)]
pub struct FragmentContext {
    /// `item_fragments.content` — shared assets/properties/etc.
    pub content: Option<Value>,
    /// `item_fragments.links_template` — link list with hrefs templated out.
    pub links_template: Option<Value>,
}

/// A dehydrated item row plus everything needed to hydrate it.
///
/// All jsonb-typed columns are kept as [`serde_json::Value`] for byte-fidelity
/// with the SQL path. `geometry` is the already-computed `ST_AsGeoJSON(geom,
/// 20)::jsonb` value (the export query computes it server-side so Rust never
/// re-encodes WKB and cannot drift on coordinate precision).
#[derive(Debug, Default, Clone)]
pub struct DehydratedItem {
    // --- always present identity / geometry ---
    /// `items.id`.
    pub id: String,
    /// `items.collection`.
    pub collection: String,
    /// `ST_AsGeoJSON(geometry, 20)::jsonb`, or `None` if the geometry is null.
    pub geometry: Option<Value>,

    // --- 0.9.11 base_item model ---
    /// `items.content` jsonb (0.9.11 only).
    pub content: Option<Value>,

    // --- 0.10 fragment model: split columns ---
    /// `items.bbox`, kept as raw JSON so the byte path can emit it verbatim (preserving PG numeric
    /// precision) and the Value path parses it on demand.
    pub bbox: Option<RawJson>,
    /// `items.links` (per-item link list when there is no fragment).
    pub links: Option<Value>,
    /// `items.assets`, kept as raw JSON so the byte path can merge it with the fragment without
    /// deep-parsing the large per-asset arrays; the Value path parses it on demand.
    pub assets: Option<RawJson>,
    /// `items.properties` (non-promoted, non-fragment properties).
    pub properties: Option<Value>,
    /// `items.extra` (top-level keys outside the known set).
    pub extra: Option<Value>,
    /// `items.stac_version`.
    pub stac_version: Option<String>,
    /// `items.stac_extensions`.
    pub stac_extensions: Option<Value>,
    /// `items.link_hrefs` — per-item hrefs spliced into a fragment template.
    pub link_hrefs: Option<Vec<Option<String>>>,
    /// `items.fragment_id` — set when the item shares a fragment.
    pub fragment_id: Option<i64>,
    /// Promoted queryable columns (0.10), in `promoted_properties_from_item` order.
    pub promoted: PromotedProperties,
}

/// The promoted queryable columns, in the exact STAC-name order that
/// `promoted_properties_from_item` rebuilds them.
///
/// Each non-temporal column value is already rendered as a JSON-ready
/// [`serde_json::Value`] (timestamps as STAC text, arrays as arrays, etc.). A
/// `Value::Null` (or absent) column is dropped, matching the SQL's
/// `jsonb_strip_nulls`.
///
/// The temporal keys are handled separately because the range case emits an
/// **explicit** `"datetime": null` which `jsonb_strip_nulls` must *not* remove
/// (it is produced by `jsonb_build_object`, outside the stripped block — see
/// `temporal_properties_from_item`).
#[derive(Debug, Default, Clone)]
pub struct PromotedProperties {
    /// The temporal block (`temporal_properties_from_item`):
    ///
    /// * instant item: `{ "datetime": "<text>" }`
    /// * range item: `{ "datetime": null, "start_datetime": "<text>",
    ///   "end_datetime": "<text>" }` (start/end stripped if null).
    ///
    /// Built by the source so the precise tstz-text formatting matches the SQL.
    pub temporal: Map<String, Value>,
    /// Ordered `(stac_name, value)` pairs for non-temporal promoted columns with
    /// **static** names (used by unit tests / hardcoded callers). The order is
    /// load-bearing for byte-parity.
    pub fields: Vec<(&'static str, Value)>,
    /// Ordered `(stac_name, value)` pairs for non-temporal promoted columns with
    /// **owned** names (the runtime-derived path; see `PromotedSchema`). Emitted
    /// after [`fields`](Self::fields). The order is load-bearing for byte-parity.
    pub owned_fields: Vec<(String, Value)>,
}

impl PromotedProperties {
    /// Builds the promoted-properties object: the temporal block first (with its
    /// explicit `datetime` key preserved), then the non-temporal columns with
    /// null values dropped (`jsonb_strip_nulls`).
    fn to_object(&self) -> Map<String, Value> {
        let mut out = self.temporal.clone();
        for (name, value) in &self.fields {
            if value.is_null() {
                continue;
            }
            let _ = out.insert((*name).to_string(), value.clone());
        }
        for (name, value) in &self.owned_fields {
            if value.is_null() {
                continue;
            }
            let _ = out.insert(name.clone(), value.clone());
        }
        out
    }
}

/// Hydrates dehydrated rows back into full STAC items.
#[derive(Debug, Clone, Copy)]
pub struct Hydrator {
    model: HydrationModel,
}

impl Hydrator {
    /// Creates a hydrator for the given storage model.
    pub fn new(model: HydrationModel) -> Self {
        Hydrator { model }
    }

    /// The storage model this hydrator targets.
    pub fn model(&self) -> HydrationModel {
        self.model
    }

    /// Hydrates one dehydrated row into a full STAC item object.
    ///
    /// `fragment` is the shared fragment content for the row's `fragment_id`
    /// (0.10 only); pass `None` for 0.9.11 or for items without a fragment.
    pub fn hydrate(
        &self,
        item: DehydratedItem,
        ctx: &CollectionContext,
        fragment: Option<&FragmentContext>,
    ) -> Value {
        match self.model {
            HydrationModel::BaseItem => hydrate_base_item(item, ctx),
            HydrationModel::Fragment => hydrate_fragment(item, fragment),
        }
    }
}

// ---------------------------------------------------------------------------
// 0.9.11 base_item model
// ---------------------------------------------------------------------------

/// The pgstac 0.9.11 deletion sentinel (`"𒍟※"`): a base_item key with this value
/// in the per-item content marks the key as removed in the hydrated output.
const DELETION_SENTINEL: &str = "𒍟※";

/// 0.9.11: `merge_jsonb(item_jsonb || content, base_item)` where
/// `item_jsonb = {id, geometry, collection, type:Feature}`.
fn hydrate_base_item(item: DehydratedItem, ctx: &CollectionContext) -> Value {
    let DehydratedItem {
        id,
        collection,
        geometry,
        content,
        ..
    } = item;
    let mut item_obj = Map::new();
    let _ = item_obj.insert("id".to_string(), Value::String(id));
    let _ = item_obj.insert("geometry".to_string(), geometry.unwrap_or(Value::Null));
    let _ = item_obj.insert("collection".to_string(), Value::String(collection));
    let _ = item_obj.insert("type".to_string(), Value::String("Feature".to_string()));

    // `{id,geometry,collection,type} || content` — jsonb concat, content wins.
    if let Some(Value::Object(content)) = content {
        for (k, v) in content {
            let _ = item_obj.insert(k, v);
        }
    }

    let base = ctx.base_item.clone().unwrap_or(Value::Null);
    merge_jsonb(Value::Object(item_obj), base)
}

/// Port of the SQL `merge_jsonb(_a, _b)`: `_a` is the per-item value, `_b` the
/// base_item value. `_a` takes precedence; objects deep-merge; equal-length
/// arrays merge element-wise; the deletion sentinel in `_a` removes the key.
fn merge_jsonb(a: Value, b: Value) -> Value {
    // WHEN _a = sentinel THEN NULL
    if let Value::String(s) = &a
        && s == DELETION_SENTINEL
    {
        return Value::Null;
    }
    // WHEN _a IS NULL OR jsonb_typeof(_a) = 'null' THEN _b
    if a.is_null() {
        return b;
    }
    match (&a, &b) {
        (Value::Object(ao), Value::Object(bo)) => {
            // jsonb_strip_nulls(jsonb_object_agg(key, merge_jsonb(a.value, b.value)))
            // over a FULL JOIN on key.
            let mut keys: Vec<String> = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for k in ao.keys().chain(bo.keys()) {
                if seen.insert(k.clone()) {
                    keys.push(k.clone());
                }
            }
            let mut out = Map::new();
            for k in keys {
                let av = ao.get(&k).cloned();
                let bv = bo.get(&k).cloned();
                let merged = match (av, bv) {
                    (Some(av), Some(bv)) => merge_jsonb(av, bv),
                    // FULL JOIN: a missing side is SQL NULL.
                    (Some(av), None) => merge_jsonb(av, Value::Null),
                    (None, Some(bv)) => merge_jsonb(Value::Null, bv),
                    (None, None) => Value::Null,
                };
                // jsonb_strip_nulls drops null *values* at every object level.
                if !merged.is_null() {
                    let _ = out.insert(k, merged);
                }
            }
            Value::Object(out)
        }
        (Value::Array(aa), Value::Array(ba)) if aa.len() == ba.len() => {
            // Element-wise merge of equal-length arrays.
            let merged: Vec<Value> = aa
                .iter()
                .zip(ba.iter())
                .map(|(x, y)| merge_jsonb(x.clone(), y.clone()))
                .collect();
            Value::Array(merged)
        }
        // ELSE _a
        _ => a,
    }
}

// ---------------------------------------------------------------------------
// 0.10 fragment model
// ---------------------------------------------------------------------------

/// 0.10: reassemble from split columns + promoted columns + optional fragment.
///
/// Consumes `item` so its (potentially large) split-column values move into the output instead of
/// being cloned per row; only the shared `fragment` parts are cloned.
fn hydrate_fragment(item: DehydratedItem, fragment: Option<&FragmentContext>) -> Value {
    let (mut output, assets_raw, bbox_raw) = hydrate_fragment_core(item, fragment);

    // bbox (parsed for the Value path).
    if let Some(bbox) = bbox_raw.map(|r| r.to_value())
        && !bbox.is_null()
    {
        let _ = output.insert("bbox".to_string(), bbox);
    }

    // merged_assets := jsonb_merge_recursive(frag.assets, COALESCE(item.assets, {}))
    let frag_assets = fragment
        .and_then(|f| f.content.as_ref())
        .and_then(|c| c.get("assets"))
        .cloned();
    let item_assets = assets_raw
        .map(|r| r.to_value())
        .unwrap_or_else(|| json!({}));
    let merged_assets =
        jsonb_merge_recursive(frag_assets, Some(item_assets)).unwrap_or_else(|| json!({}));
    if merged_assets != json!({}) {
        let _ = output.insert("assets".to_string(), merged_assets);
    }

    Value::Object(output)
}

/// The fragment-model item assembled for **every key except `assets` and `bbox`**, returned alongside
/// the item's raw `assets`/`bbox`.
///
/// The Value path ([`hydrate_fragment`]) parses + merges those into the map; the byte path
/// ([`crate::feature`]) emits the raw `bbox` verbatim and merges `assets` at serialize time. Object key
/// order is irrelevant to STAC semantics (and to the [`Value`] parity check), so the two callers add
/// `assets`/`bbox` wherever is convenient.
pub(crate) fn hydrate_fragment_core(
    item: DehydratedItem,
    fragment: Option<&FragmentContext>,
) -> (Map<String, Value>, Option<RawJson>, Option<RawJson>) {
    let DehydratedItem {
        id,
        collection,
        geometry,
        bbox,
        links,
        assets,
        properties,
        extra,
        stac_version,
        stac_extensions,
        link_hrefs,
        fragment_id,
        promoted,
        ..
    } = item;

    let frag_content = fragment.and_then(|f| f.content.as_ref());
    let frag_links_template = fragment.and_then(|f| f.links_template.as_ref());
    let has_fragment = fragment_id.is_some() && fragment.is_some();

    // merged_properties := jsonb_merge_recursive(frag.properties, COALESCE(item.properties, {}))
    let frag_properties = frag_content.and_then(|c| c.get("properties")).cloned();
    let item_properties = properties.unwrap_or_else(|| json!({}));
    let merged_properties = jsonb_merge_recursive(frag_properties, Some(item_properties));
    // merged_properties := promoted_properties_from_item || COALESCE(merged, {})
    let merged_properties = concat_objects(
        Value::Object(promoted.to_object()),
        merged_properties.unwrap_or_else(|| json!({})),
    );

    // hydrated_stac_version := COALESCE(item.stac_version, frag.stac_version)
    let hydrated_stac_version = stac_version.or_else(|| {
        frag_content
            .and_then(|c| c.get("stac_version"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
    });

    // hydrated_stac_extensions:
    //   item.stac_extensions when non-null and != []
    //   else COALESCE(frag.stac_extensions, item.stac_extensions)
    let hydrated_stac_extensions = match stac_extensions {
        Some(e) if !e.is_null() && !is_empty_array(&e) => Some(e),
        other => frag_content
            .and_then(|c| c.get("stac_extensions"))
            .cloned()
            .or(other),
    };

    // links
    let hydrated_links = if has_fragment {
        stac_links_hydrate(frag_links_template, link_hrefs.as_deref())
    } else {
        links.unwrap_or_else(|| json!([]))
    };

    let mut output = Map::new();
    let _ = output.insert("id".to_string(), Value::String(id));
    let _ = output.insert("geometry".to_string(), geometry.unwrap_or(Value::Null));
    let _ = output.insert("collection".to_string(), Value::String(collection));
    let _ = output.insert("type".to_string(), Value::String("Feature".to_string()));
    if let Some(v) = hydrated_stac_version {
        let _ = output.insert("stac_version".to_string(), Value::String(v));
    }
    if let Some(exts) = hydrated_stac_extensions
        && !exts.is_null()
        && !is_empty_array(&exts)
    {
        let _ = output.insert("stac_extensions".to_string(), exts);
    }
    // hydrated_links IS NOT NULL is always true here (we coalesce to []).
    let _ = output.insert("links".to_string(), hydrated_links);
    // merged_properties IS NOT NULL is always true (we coalesce).
    let _ = output.insert("properties".to_string(), merged_properties);

    // output := output || item.extra
    if let Some(Value::Object(extra)) = extra {
        for (k, v) in extra {
            let _ = output.insert(k, v);
        }
    }

    (output, assets, bbox)
}

/// `_a || _b` jsonb concatenation for two objects (right side wins on key
/// collision). Used for `promoted_properties_from_item || merged_properties`.
fn concat_objects(a: Value, b: Value) -> Value {
    match (a, b) {
        (Value::Object(mut ao), Value::Object(bo)) => {
            for (k, v) in bo {
                let _ = ao.insert(k, v);
            }
            Value::Object(ao)
        }
        // promoted is always an object; merged_properties coalesced to {}.
        (_, b) => b,
    }
}

/// Port of `jsonb_merge_recursive(frag, item)`: deep merge with *item*
/// precedence. Object keys merge recursively; for non-object collisions the item
/// value wins. Disjoint object key sets short-circuit to a shallow concat
/// (`frag || item`), which the SQL does for performance and is byte-equivalent.
fn jsonb_merge_recursive(frag: Option<Value>, item: Option<Value>) -> Option<Value> {
    match (frag, item) {
        // WHEN frag IS NULL THEN COALESCE(item, {})
        (None, item) => Some(item.unwrap_or_else(|| json!({}))),
        (Some(frag), item) => {
            let item = match item {
                // WHEN item IS NULL OR item = {} THEN frag
                None => return Some(frag),
                Some(i) if i == json!({}) => return Some(frag),
                Some(i) => i,
            };
            match (&frag, &item) {
                (Value::Object(fo), Value::Object(io)) => {
                    // FULL JOIN on key; per-key:
                    //   i null -> f ; f null -> i
                    //   both objects -> disjoint? f||i : recurse
                    //   else -> i
                    let mut keys: Vec<String> = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    for k in fo.keys().chain(io.keys()) {
                        if seen.insert(k.clone()) {
                            keys.push(k.clone());
                        }
                    }
                    let mut out = Map::new();
                    for k in keys {
                        let fv = fo.get(&k);
                        let iv = io.get(&k);
                        let merged = match (fv, iv) {
                            (Some(fv), None) => fv.clone(),
                            (None, Some(iv)) => iv.clone(),
                            (Some(fv), Some(iv)) => {
                                if let (Value::Object(fvo), Value::Object(ivo)) = (fv, iv) {
                                    let disjoint = !fvo.keys().any(|fk| ivo.contains_key(fk));
                                    if disjoint {
                                        // f.value || i.value
                                        concat_objects(fv.clone(), iv.clone())
                                    } else {
                                        jsonb_merge_recursive(Some(fv.clone()), Some(iv.clone()))
                                            .unwrap_or_else(|| json!({}))
                                    }
                                } else {
                                    iv.clone()
                                }
                            }
                            (None, None) => continue,
                        };
                        let _ = out.insert(k, merged);
                    }
                    Some(Value::Object(out))
                }
                // ELSE item
                _ => Some(item),
            }
        }
    }
}

/// Port of `stac_links_hydrate(links_template, link_hrefs)`: splice per-item
/// hrefs back into the fragment link template by ordinal position.
fn stac_links_hydrate(template: Option<&Value>, hrefs: Option<&[Option<String>]>) -> Value {
    let template = match template {
        Some(Value::Array(a)) => a,
        // links_template null or not an array -> []
        _ => return json!([]),
    };

    // No hrefs -> return the template as-is.
    let hrefs = match hrefs {
        Some(h) if !h.is_empty() => h,
        _ => return Value::Array(template.clone()),
    };

    let out: Vec<Value> = template
        .iter()
        .enumerate()
        .map(|(idx, link)| match link {
            Value::Object(obj) => {
                // ordinality is 1-based in SQL; link_hrefs[ord].
                let href = hrefs.get(idx).and_then(|h| h.as_ref());
                let mut new_obj = obj.clone();
                let _ = new_obj.remove("href");
                if let Some(href) = href {
                    let _ = new_obj.insert("href".to_string(), Value::String(href.clone()));
                }
                Value::Object(new_obj)
            }
            other => other.clone(),
        })
        .collect();
    Value::Array(out)
}

fn is_empty_array(v: &Value) -> bool {
    matches!(v, Value::Array(a) if a.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temporal_instant(dt: &str) -> Map<String, Value> {
        let mut m = Map::new();
        let _ = m.insert("datetime".to_string(), Value::String(dt.to_string()));
        m
    }

    #[test]
    fn merge_jsonb_item_wins() {
        let a = json!({"a": 1, "b": 2});
        let b = json!({"b": 99, "c": 3});
        assert_eq!(
            merge_jsonb(a, b),
            json!({"a": 1, "b": 2, "c": 3}),
            "item value wins on collision; base-only keys included"
        );
    }

    #[test]
    fn merge_jsonb_deletion_sentinel() {
        let a = json!({"a": DELETION_SENTINEL, "b": 2});
        let b = json!({"a": 1, "b": 99});
        // a -> sentinel removes key (merge to null, then stripped); b kept from a.
        assert_eq!(merge_jsonb(a, b), json!({"b": 2}));
    }

    #[test]
    fn merge_jsonb_strip_nulls() {
        // base-only key whose value is null is stripped.
        let a = json!({"a": 1});
        let b = json!({"a": 1, "n": null});
        assert_eq!(merge_jsonb(a, b), json!({"a": 1}));
    }

    #[test]
    fn merge_jsonb_equal_arrays() {
        let a = json!([{"x": 1}, {"y": 2}]);
        let b = json!([{"x": 0, "z": 9}, {"y": 0, "w": 8}]);
        assert_eq!(
            merge_jsonb(a, b),
            json!([{"x": 1, "z": 9}, {"y": 2, "w": 8}])
        );
    }

    #[test]
    fn merge_jsonb_unequal_arrays_a_wins() {
        let a = json!([1, 2]);
        let b = json!([3, 4, 5]);
        assert_eq!(merge_jsonb(a, b), json!([1, 2]));
    }

    #[test]
    fn base_item_basic() {
        let h = Hydrator::new(HydrationModel::BaseItem);
        let item = DehydratedItem {
            id: "i1".into(),
            collection: "c1".into(),
            geometry: Some(json!({"type": "Point", "coordinates": [1.0, 2.0]})),
            content: Some(json!({
                "properties": {"datetime": "2020-01-01T00:00:00Z", "eo:cloud_cover": 5},
                "assets": {"data": {"href": "s3://x"}}
            })),
            ..Default::default()
        };
        let ctx = CollectionContext {
            base_item: Some(json!({
                "stac_version": "1.0.0",
                "properties": {"platform": "sentinel-2"},
                "assets": {"data": {"type": "image/tiff"}}
            })),
        };
        let out = h.hydrate(item, &ctx, None);
        assert_eq!(out["id"], "i1");
        assert_eq!(out["type"], "Feature");
        assert_eq!(out["stac_version"], "1.0.0");
        assert_eq!(out["properties"]["platform"], "sentinel-2");
        assert_eq!(out["properties"]["eo:cloud_cover"], 5);
        assert_eq!(out["assets"]["data"]["href"], "s3://x");
        assert_eq!(out["assets"]["data"]["type"], "image/tiff");
    }

    #[test]
    fn fragment_promoted_properties() {
        let h = Hydrator::new(HydrationModel::Fragment);
        let item = DehydratedItem {
            id: "i1".into(),
            collection: "c1".into(),
            geometry: Some(json!({"type": "Point", "coordinates": [1.0, 2.0]})),
            promoted: PromotedProperties {
                temporal: temporal_instant("2020-01-01T00:00:00Z"),
                fields: vec![
                    ("platform", json!("landsat-8")),
                    ("eo:cloud_cover", json!(12.5)),
                    ("nullcol", Value::Null),
                ],
                owned_fields: vec![],
            },
            ..Default::default()
        };
        let out = h.hydrate(item, &CollectionContext::default(), None);
        assert_eq!(out["properties"]["datetime"], "2020-01-01T00:00:00Z");
        assert_eq!(out["properties"]["platform"], "landsat-8");
        assert_eq!(out["properties"]["eo:cloud_cover"], 12.5);
        assert!(out["properties"].get("nullcol").is_none());
        // links always present (coalesced to []).
        assert_eq!(out["links"], json!([]));
    }

    #[test]
    fn fragment_merge_and_links() {
        let h = Hydrator::new(HydrationModel::Fragment);
        let item = DehydratedItem {
            id: "i1".into(),
            collection: "c1".into(),
            geometry: Some(json!({"type": "Point", "coordinates": [0.0, 0.0]})),
            fragment_id: Some(7),
            link_hrefs: Some(vec![Some("https://a/self".into()), None]),
            properties: Some(json!({"eo:cloud_cover": 1})),
            promoted: PromotedProperties {
                temporal: temporal_instant("2020-01-01T00:00:00Z"),
                fields: vec![],
                owned_fields: vec![],
            },
            ..Default::default()
        };
        let frag = FragmentContext {
            content: Some(json!({
                "properties": {"platform": "x"},
                "assets": {"thumb": {"href": "t"}}
            })),
            links_template: Some(json!([
                {"rel": "self", "type": "application/json"},
                {"rel": "root", "href": "https://static/root"}
            ])),
        };
        let out = h.hydrate(item, &CollectionContext::default(), Some(&frag));
        assert_eq!(out["properties"]["platform"], "x");
        assert_eq!(out["properties"]["eo:cloud_cover"], 1);
        assert_eq!(out["properties"]["datetime"], "2020-01-01T00:00:00Z");
        assert_eq!(out["assets"]["thumb"]["href"], "t");
        // link 0: href spliced from link_hrefs[0]
        assert_eq!(out["links"][0]["href"], "https://a/self");
        assert_eq!(out["links"][0]["rel"], "self");
        // link 1: href None -> href removed from template
        assert_eq!(out["links"][1]["rel"], "root");
        assert!(out["links"][1].get("href").is_none());
    }
}
