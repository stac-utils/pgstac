//! Root-metadata fetchers: queryables.json, settings.json, collection.json.
//!
//! These shapes are the cross-PR contract the ingest side reads.
//! Generated columns (`queryables.id`, `collections.base_item`/geometry/…) are
//! never dumped — the target recomputes them.

use crate::Result;
use crate::export::plan::CollectionPlan;
use serde_json::{Value, json};
use tokio_postgres::GenericClient;

/// Builds the `queryables.json` body: all queryables rows verbatim, minus the
/// GENERATED `id`. Each row carries its own `collection_ids` (null = global), so
/// global + per-collection queryables share one file.
pub async fn queryables_json<C: GenericClient>(client: &C) -> Result<(Value, u64)> {
    let rows = client
        .query(
            "SELECT name, collection_ids, definition, property_path, \
                    property_wrapper, property_index_type \
             FROM queryables ORDER BY name, collection_ids NULLS FIRST",
            &[],
        )
        .await?;
    let queryables: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "name": r.get::<_, String>("name"),
                "collection_ids": r.get::<_, Option<Vec<String>>>("collection_ids"),
                "definition": r.get::<_, Option<Value>>("definition"),
                "property_path": r.get::<_, Option<String>>("property_path"),
                "property_wrapper": r.get::<_, Option<String>>("property_wrapper"),
                "property_index_type": r.get::<_, Option<String>>("property_index_type"),
            })
        })
        .collect();
    let count = queryables.len() as u64;
    Ok((json!({ "queryables": queryables }), count))
}

/// Builds the `settings.json` body: all `pgstac_settings` rows verbatim. Import
/// decides what to apply (decision §10).
pub async fn settings_json<C: GenericClient>(client: &C) -> Result<(Value, u64)> {
    let rows = client
        .query("SELECT name, value FROM pgstac_settings ORDER BY name", &[])
        .await?;
    let settings: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "name": r.get::<_, String>("name"),
                "value": r.get::<_, Option<String>>("value"),
            })
        })
        .collect();
    let count = settings.len() as u64;
    Ok((json!({ "settings": settings }), count))
}

/// Builds the `collection.json` body for a collection: the STAC content plus the
/// pgstac restore metadata (partition_trunc, private, fragment_config).
pub fn collection_json(plan: &CollectionPlan) -> Value {
    json!({
        "stac": plan.content,
        "pgstac": {
            "partition_trunc": plan.partition_trunc.as_manifest_str(),
            "private": plan.private,
            "fragment_config": plan.fragment_config,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::plan::PartitionTrunc;

    #[test]
    fn collection_json_shape() {
        let plan = CollectionPlan {
            id: "c".into(),
            partition_trunc: PartitionTrunc::Month,
            content: json!({"id": "c", "type": "Collection"}),
            private: Some(json!({"secret": 1})),
            fragment_config: Some(vec!["[\"assets\"]".into()]),
            partitions: vec![],
        };
        let j = collection_json(&plan);
        assert_eq!(j["stac"]["id"], "c");
        assert_eq!(j["pgstac"]["partition_trunc"], "month");
        assert_eq!(j["pgstac"]["private"]["secret"], 1);
        assert_eq!(j["pgstac"]["fragment_config"][0], "[\"assets\"]");
    }

    #[test]
    fn collection_json_null_trunc() {
        let plan = CollectionPlan {
            id: "c".into(),
            partition_trunc: PartitionTrunc::None,
            content: json!({"id": "c"}),
            private: None,
            fragment_config: None,
            partitions: vec![],
        };
        let j = collection_json(&plan);
        assert!(j["pgstac"]["partition_trunc"].is_null());
        assert!(j["pgstac"]["fragment_config"].is_null());
    }
}
