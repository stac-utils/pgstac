//! Data export (dump) library for pgstac.
//!
//! This module turns a pgstac instance (0.9.11 or 0.10) into a self-describing,
//! restorable dump: fully hydrated items as stac-geoparquet (one file per
//! partition) plus collection/queryables/settings JSON and a manifest.
//!
//! It is library code with no CLI dependencies; the `pgstac` binary (behind the
//! `cli` feature) is a thin wrapper.

pub mod budget;
pub mod format;
pub mod manifest;
pub mod metadata;
pub mod parallel;
pub mod plan;
pub mod planner;
pub mod search;
pub mod sink;

pub use parallel::{ClientFactory, DsnFactory, Snapshot};
pub use planner::{DumpConfig, DumpPlanner, DumpReport};
pub use search::SearchSource;

use crate::Result;
use crate::hydrate::{CollectionContext, HydrationModel};
use serde_json::Value;
use tokio_postgres::GenericClient;

/// Detects the storage model of a pgstac instance from its version string.
///
/// 0.9.x and earlier use the per-collection `base_item` model; 0.10+ uses the
/// fragment model. A version that does not parse as `0.9.x` (e.g. the dev
/// `"unreleased"` build, or `1.x`) is treated as the fragment model.
///
/// Prefer [`detect_hydration_model`], which inspects the actual schema and is
/// robust to non-numeric version strings; this helper is exposed for callers
/// that already have a release version in hand.
pub fn hydration_model_for_version(version: &str) -> HydrationModel {
    // Versions are `MAJOR.MINOR.PATCH`. Only a clean `0.<10` is base_item.
    let mut parts = version.split('.');
    let major: Option<u64> = parts.next().and_then(|p| p.parse().ok());
    let minor: Option<u64> = parts.next().and_then(|p| p.parse().ok());
    match (major, minor) {
        (Some(0), Some(minor)) if minor < 10 => HydrationModel::BaseItem,
        _ => HydrationModel::Fragment,
    }
}

/// Detects the storage model by inspecting the live schema (robust to dev
/// version strings like `"unreleased"`).
///
/// The fragment model is identified by the presence of the `items.fragment_id`
/// column (0.10); otherwise the `collections.base_item` column (0.9.11) means the
/// base_item model.
pub async fn detect_hydration_model<C: GenericClient>(client: &C) -> Result<HydrationModel> {
    let has_fragment_id: bool = client
        .query_one(
            "SELECT EXISTS (\
                SELECT 1 FROM information_schema.columns \
                WHERE table_schema = 'pgstac' \
                  AND table_name = 'items' \
                  AND column_name = 'fragment_id'\
             ) AS present",
            &[],
        )
        .await?
        .get("present");
    if has_fragment_id {
        Ok(HydrationModel::Fragment)
    } else {
        Ok(HydrationModel::BaseItem)
    }
}

/// Loads the [`CollectionContext`] (base_item for 0.9.11) for a collection.
///
/// For the fragment model the context carries no per-collection data, but we
/// still confirm the collection exists.
pub async fn load_collection_context<C: GenericClient>(
    client: &C,
    model: HydrationModel,
    collection_id: &str,
) -> Result<CollectionContext> {
    match model {
        HydrationModel::BaseItem => {
            let rows = client
                .query(
                    "SELECT base_item FROM collections WHERE id = $1",
                    &[&collection_id],
                )
                .await?;
            let base_item = rows
                .first()
                .and_then(|row| row.get::<_, Option<Value>>("base_item"));
            Ok(CollectionContext { base_item })
        }
        HydrationModel::Fragment => Ok(CollectionContext::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_detection() {
        assert_eq!(
            hydration_model_for_version("0.9.11"),
            HydrationModel::BaseItem
        );
        assert_eq!(
            hydration_model_for_version("0.9.15"),
            HydrationModel::BaseItem
        );
        assert_eq!(
            hydration_model_for_version("0.10.0"),
            HydrationModel::Fragment
        );
        assert_eq!(
            hydration_model_for_version("1.0.0"),
            HydrationModel::Fragment
        );
    }
}
