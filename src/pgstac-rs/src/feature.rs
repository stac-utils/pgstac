//! Byte-assembly output for hydrated 0.10 items.
//!
//! [`write_fragment_feature`] writes a fully-hydrated STAC item straight to a writer without ever
//! materializing the merged `assets` as a [`Value`]: the item's `assets` are read as raw JSON, the
//! shared fragment's `assets` are borrowed (by `&`, never cloned per item), and the two are merged
//! **at serialize time** — descending only where both sides are objects at a key, and emitting arrays,
//! scalars, and one-sided keys **verbatim** from the raw bytes. `bbox` is likewise emitted from raw
//! bytes (preserving PostgreSQL `numeric` precision that an f64 round-trip would lose). Everything else
//! reuses [`hydrate_fragment_core`].
//!
//! The merge reproduces SQL `jsonb_merge_recursive(frag, item)` exactly (see the parallel
//! implementation in [`crate::hydrate`]); validated by parsing the output back and comparing to
//! `content_hydrate`.

use crate::Result;
use crate::hydrate::{DehydratedItem, FragmentContext, hydrate_fragment_core};
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::io::Write;

/// Writes one fully-hydrated 0.10 item as JSON to `write`.
pub fn write_fragment_feature<W: Write>(
    item: DehydratedItem,
    fragment: Option<&FragmentContext>,
    write: &mut W,
) -> Result<()> {
    let frag_assets = fragment
        .and_then(|f| f.content.as_ref())
        .and_then(|c| c.get("assets"));
    let (core, assets_raw, bbox_raw) = hydrate_fragment_core(item, fragment);
    let feature = FeatureWriter {
        core: &core,
        bbox: bbox_raw.as_ref().map(|r| r.0.as_ref()),
        frag_assets,
        item_assets: assets_raw.as_ref().map(|r| r.0.as_ref()),
    };
    serde_json::to_writer(write, &feature)?;
    Ok(())
}

/// Parses a raw JSON object one level into `key -> raw value`, or `None` when it is not an object.
fn raw_object(raw: &RawValue) -> Option<BTreeMap<String, &RawValue>> {
    serde_json::from_str(raw.get()).ok()
}

/// Whether a raw object has at least one key.
fn raw_object_nonempty(raw: &RawValue) -> bool {
    raw_object(raw).is_some_and(|m| !m.is_empty())
}

/// The top-level feature: core keys, then raw `bbox`, then the serialize-time-merged `assets`.
struct FeatureWriter<'a> {
    core: &'a Map<String, Value>,
    bbox: Option<&'a RawValue>,
    frag_assets: Option<&'a Value>,
    item_assets: Option<&'a RawValue>,
}

impl Serialize for FeatureWriter<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (k, v) in self.core {
            map.serialize_entry(k, v)?;
        }
        if let Some(bbox) = self.bbox
            && bbox.get().trim() != "null"
        {
            map.serialize_entry("bbox", bbox)?;
        }
        // assets := jsonb_merge_recursive(frag.assets, item.assets); emitted unless it would be {}.
        let frag_nonempty = self
            .frag_assets
            .and_then(Value::as_object)
            .is_some_and(|o| !o.is_empty());
        let item_nonempty = self.item_assets.is_some_and(raw_object_nonempty);
        if frag_nonempty || item_nonempty {
            map.serialize_entry(
                "assets",
                &Merge {
                    frag: self.frag_assets,
                    item: self.item_assets,
                },
            )?;
        }
        map.end()
    }
}

/// Serializes `jsonb_merge_recursive(frag, item)` where `frag` is a parsed [`Value`] (shared, by ref)
/// and `item` is raw JSON (never deep-parsed).
struct Merge<'a> {
    frag: Option<&'a Value>,
    item: Option<&'a RawValue>,
}

impl Serialize for Merge<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match (self.frag, self.item) {
            // frag NULL -> COALESCE(item, {})
            (None, Some(item)) => item.serialize(serializer),
            (None, None) => serializer.serialize_map(Some(0))?.end(),
            // item NULL -> frag
            (Some(frag), None) => frag.serialize(serializer),
            (Some(frag), Some(item)) => {
                let item_obj = raw_object(item);
                // item = {} -> frag (regardless of frag's type)
                if item_obj.as_ref().is_some_and(|m| m.is_empty()) {
                    return frag.serialize(serializer);
                }
                match (frag.as_object(), item_obj) {
                    // both objects -> FULL JOIN merge
                    (Some(frag_obj), Some(item_obj)) => {
                        serialize_object_merge(frag_obj, &item_obj, serializer)
                    }
                    // else -> item wins, verbatim
                    _ => item.serialize(serializer),
                }
            }
        }
    }
}

/// The both-objects FULL JOIN: frag keys (then item-only keys), with per-key merge rules matching
/// `jsonb_merge_recursive`'s inner branch.
fn serialize_object_merge<S: Serializer>(
    frag: &Map<String, Value>,
    item: &BTreeMap<String, &RawValue>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(None)?;
    for (key, fv) in frag {
        match item.get(key.as_str()) {
            // frag-only
            None => map.serialize_entry(key, fv)?,
            Some(iv) => match (fv.as_object(), raw_object(iv)) {
                // both objects: disjoint -> concat (f || i); else recurse
                (Some(fvo), Some(ivo)) => {
                    let disjoint = !fvo.keys().any(|fk| ivo.contains_key(fk.as_str()));
                    if disjoint {
                        map.serialize_entry(
                            key,
                            &Concat {
                                frag: fvo,
                                item: &ivo,
                            },
                        )?;
                    } else {
                        map.serialize_entry(
                            key,
                            &Merge {
                                frag: Some(fv),
                                item: Some(iv),
                            },
                        )?;
                    }
                }
                // else -> item wins, verbatim
                _ => map.serialize_entry(key, iv)?,
            },
        }
    }
    for (key, iv) in item {
        if !frag.contains_key(key.as_str()) {
            map.serialize_entry(key, iv)?;
        }
    }
    map.end()
}

/// `frag.value || item.value` shallow concat for the disjoint-objects case (no key overlap, so order
/// is frag's keys then item's).
struct Concat<'a> {
    frag: &'a Map<String, Value>,
    item: &'a BTreeMap<String, &'a RawValue>,
}

impl Serialize for Concat<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (k, v) in self.frag {
            map.serialize_entry(k, v)?;
        }
        for (k, v) in self.item {
            if !self.frag.contains_key(k.as_str()) {
                map.serialize_entry(k, v)?;
            }
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hydrate::{
        CollectionContext, DehydratedItem, FragmentContext, HydrationModel, Hydrator,
        PromotedProperties,
    };
    use crate::rawjson::RawJson;
    use serde_json::json;

    fn temporal(dt: &str) -> Map<String, Value> {
        serde_json::from_value(json!({ "datetime": dt })).unwrap()
    }

    #[test]
    fn byte_path_equals_value_path_and_preserves_bbox_precision() {
        // Exercises every merge branch: recurse (data), disjoint concat (data.nested),
        // item-wins-on-leaf (data.type), frag-only (thumbnail), item-only (item_only).
        let make_item = || DehydratedItem {
            id: "i1".into(),
            collection: "c1".into(),
            geometry: Some(json!({"type": "Point", "coordinates": [1, 2]})),
            // Built from raw text (NOT json!, which would round through f64 first) so the high-precision
            // coordinate reaches the byte path verbatim.
            bbox: Some(RawJson(
                RawValue::from_string("[-22.650799880000001,1.0,2.0,3.0]".to_string()).unwrap(),
            )),
            assets: Some(RawJson::from_value(&json!({
                "data": {"href": "s3://x", "type": "override", "nested": {"i": 1}},
                "item_only": {"k": [1, 2, 3]}
            }))),
            stac_extensions: Some(json!(["https://ext/v1"])),
            promoted: PromotedProperties {
                temporal: temporal("2020-01-01T00:00:00Z"),
                fields: vec![("platform", json!("l8"))],
                owned_fields: vec![],
            },
            fragment_id: Some(7),
            ..Default::default()
        };
        let frag = FragmentContext {
            content: Some(json!({
                "assets": {
                    "data": {"type": "image/tiff", "nested": {"j": 2}},
                    "thumbnail": {"href": "t", "big": [0, 0, 0]}
                },
                "stac_version": "1.0.0"
            })),
            links_template: None,
        };

        let h = Hydrator::new(HydrationModel::Fragment);
        let value = h.hydrate(make_item(), &CollectionContext::default(), Some(&frag));

        let mut buf: Vec<u8> = Vec::new();
        write_fragment_feature(make_item(), Some(&frag), &mut buf).unwrap();
        let byte_value: Value = serde_json::from_slice(&buf).unwrap();

        assert_eq!(byte_value, value, "byte path != value path");
        assert_eq!(byte_value["assets"]["data"]["type"], "override"); // item wins on leaf
        assert_eq!(
            byte_value["assets"]["data"]["nested"],
            json!({"j": 2, "i": 1})
        ); // disjoint concat
        assert!(byte_value["assets"]["thumbnail"]["big"].is_array()); // frag-only, verbatim
        // bbox numeric precision preserved verbatim in the byte output.
        assert!(
            String::from_utf8_lossy(&buf).contains("-22.650799880000001"),
            "bbox precision lost"
        );
    }
}
