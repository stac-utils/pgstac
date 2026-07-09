//! Loading queryables from a JSON-schema document into `pgstac.queryables`.
//!
//! Ports pypgstac's queryables loader to the 0.10 model: for each non-core property in the document's
//! `properties`, map its JSON type to a `property_wrapper` and, for a property named in `index_fields`,
//! set `property_index_type = 'BTREE'`; then upsert the row scoped to `collection_ids` (or the shared
//! NULL scope). The `queryables` trigger marks `indexes_pending`; the async index sweep builds any
//! indexes, so there is no synchronous index build here.

use crate::{Error, Result};
use serde_json::Value;
use tokio_postgres::Client;

/// Fields pgstac indexes directly; queryables never override them.
const CORE_FIELDS: [&str; 5] = ["id", "geometry", "datetime", "end_datetime", "collection"];

/// The `property_wrapper` for a queryable definition (JSON-Schema type/format → pgstac wrapper).
fn property_wrapper(definition: &Value) -> &'static str {
    match definition.get("type").and_then(Value::as_str) {
        Some("number") => "to_float",
        Some("integer") => "to_int",
        Some("array") => "to_text_array",
        _ if definition.get("format").and_then(Value::as_str) == Some("date-time") => "to_tstz",
        _ => "to_text",
    }
}

/// Loads the `properties` of a queryables JSON-schema document into `pgstac.queryables`.
///
/// Non-core properties are upserted, scoped to `collection_ids` (or the shared NULL scope when `None`); a
/// property named in `index_fields` gets `property_index_type = 'BTREE'`. With `delete_missing`, queryables
/// in the same scope whose name is absent from the document are removed. Returns the number loaded.
///
/// Errors when the document has no non-empty `properties` object.
pub(crate) async fn load_queryables(
    client: &mut Client,
    queryables: &Value,
    collection_ids: Option<&[String]>,
    delete_missing: bool,
    index_fields: Option<&[String]>,
) -> Result<u64> {
    let properties = match queryables.get("properties").and_then(Value::as_object) {
        Some(properties) if !properties.is_empty() => properties,
        _ => {
            return Err(Error::Queryables(
                "queryables document has no `properties`".to_string(),
            ));
        }
    };

    let tx = client.transaction().await?;
    let mut loaded = 0u64;
    let mut names: Vec<String> = Vec::new();

    for (name, definition) in properties {
        if CORE_FIELDS.contains(&name.as_str()) {
            continue;
        }
        names.push(name.clone());
        let wrapper = property_wrapper(definition);
        let index_type: Option<&str> = index_fields
            .is_some_and(|fields| fields.iter().any(|field| field == name))
            .then_some("BTREE");

        // Replace any existing row for this name in the target scope (and the shared NULL scope when scoped).
        match collection_ids {
            None => {
                let _ = tx.execute(
                    "DELETE FROM queryables WHERE name = $1 AND collection_ids IS NULL",
                    &[name],
                )
                .await?;
            }
            Some(cids) => {
                let _ = tx.execute(
                    "DELETE FROM queryables WHERE name = $1 AND collection_ids = $2",
                    &[name, &cids],
                )
                .await?;
                let _ = tx.execute(
                    "DELETE FROM queryables WHERE name = $1 AND collection_ids IS NULL",
                    &[name],
                )
                .await?;
            }
        }

        let _ = tx.execute(
            "INSERT INTO queryables \
             (name, collection_ids, definition, property_wrapper, property_index_type) \
             VALUES ($1, $2, $3, $4, $5)",
            &[name, &collection_ids, definition, &wrapper, &index_type],
        )
        .await?;
        loaded += 1;
    }

    if delete_missing && !names.is_empty() {
        let core: Vec<&str> = CORE_FIELDS.to_vec();
        match collection_ids {
            None => {
                let _ = tx.execute(
                    "DELETE FROM queryables \
                     WHERE collection_ids IS NULL AND name <> ALL($1) AND name <> ALL($2)",
                    &[&names, &core],
                )
                .await?;
            }
            Some(cids) => {
                let _ = tx.execute(
                    "DELETE FROM queryables \
                     WHERE collection_ids = $1 AND name <> ALL($2) AND name <> ALL($3)",
                    &[&cids, &names, &core],
                )
                .await?;
            }
        }
    }

    tx.commit().await?;
    Ok(loaded)
}
