//! Work-unit enumeration for a dump: collections and their partitions, with
//! datetime/bbox prefilter pruning (no keyset search engine, so this runs on
//! both 0.9.11 and 0.10).

use crate::Result;
use serde_json::Value;
use tokio_postgres::GenericClient;

/// The partitioning granularity of a collection (`collections.partition_trunc`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionTrunc {
    /// One partition per year-month; file `<YYYYMM>.parquet`.
    Month,
    /// One partition per year; file `<YYYY>.parquet`.
    Year,
    /// No datetime sub-partitioning; one file `items.parquet`.
    None,
}

impl PartitionTrunc {
    /// Parses the `collections.partition_trunc` text value.
    pub fn parse(value: Option<&str>) -> PartitionTrunc {
        match value {
            Some("month") => PartitionTrunc::Month,
            Some("year") => PartitionTrunc::Year,
            _ => PartitionTrunc::None,
        }
    }

    /// The text form for the manifest (`"month"`/`"year"`/`null`).
    pub fn as_manifest_str(&self) -> Option<&'static str> {
        match self {
            PartitionTrunc::Month => Some("month"),
            PartitionTrunc::Year => Some("year"),
            PartitionTrunc::None => None,
        }
    }
}

/// One partition to dump: its source name + temporal footprint.
#[derive(Debug, Clone)]
pub struct PartitionPlan {
    /// Source partition relation name (e.g. `_items_2_202401`).
    pub name: String,
    /// Constraint datetime range `[start, end)` as STAC text, if bounded.
    pub dtrange: Option<(String, String)>,
    /// Estimated row count (`reltuples`).
    pub reltuples: f32,
    /// The parquet file name within the collection dir, per MANIFEST.md naming.
    pub file_name: String,
}

/// One collection to dump: its id, partition_trunc, and partitions.
#[derive(Debug, Clone)]
pub struct CollectionPlan {
    /// Collection id (authoritative).
    pub id: String,
    /// Partitioning granularity.
    pub partition_trunc: PartitionTrunc,
    /// The collection's STAC content (`collections.content`).
    pub content: Value,
    /// The collection's `private` jsonb, if any.
    pub private: Option<Value>,
    /// The collection's `fragment_config` (0.10), if any.
    pub fragment_config: Option<Vec<String>>,
    /// Partitions to dump (already pruned by any prefilter).
    pub partitions: Vec<PartitionPlan>,
}

/// A datetime/bbox prefilter for a partial dump.
#[derive(Debug, Clone, Default)]
pub struct Prefilter {
    /// Inclusive-start, exclusive-end datetime window (STAC text).
    pub datetime: Option<(String, String)>,
    /// Spatial bbox `[w, s, e, n]`.
    pub bbox: Option<[f64; 4]>,
}

/// Derives the partition file name from the truncation and the partition's
/// datetime range start (per MANIFEST.md naming table).
fn file_name_for(trunc: PartitionTrunc, dtrange_start: Option<&str>) -> String {
    match trunc {
        PartitionTrunc::None => "items.parquet".to_string(),
        PartitionTrunc::Month => match dtrange_start {
            // `2024-01-01...` -> `202401.parquet`
            Some(s) if s.len() >= 7 => {
                format!("{}{}.parquet", &s[0..4], &s[5..7])
            }
            _ => "items.parquet".to_string(),
        },
        PartitionTrunc::Year => match dtrange_start {
            Some(s) if s.len() >= 4 => format!("{}.parquet", &s[0..4]),
            _ => "items.parquet".to_string(),
        },
    }
}

/// Lower bound text of a `tstzrange`, or `None` for `-infinity`/empty.
fn range_bound_text(range_lower: Option<&str>) -> Option<String> {
    match range_lower {
        Some(s) if s != "-infinity" && s != "infinity" && !s.is_empty() => {
            // Postgres renders tstz as `2024-01-01 00:00:00+00`; normalize to a
            // sortable date prefix for naming. Keep the full text for the range.
            Some(s.to_string())
        }
        _ => None,
    }
}

/// Plans the collections + partitions to dump.
///
/// `collection_ids` restricts to a subset (partial dump); `None` = all
/// collections. `prefilter` prunes partitions whose datetime footprint does not
/// overlap the requested window. bbox pruning happens per-item at scan time (the
/// partition footprint is temporal only), so a bbox-only filter does not prune
/// partitions here.
pub async fn plan_collections<C: GenericClient>(
    client: &C,
    collection_ids: Option<&[String]>,
    prefilter: &Prefilter,
) -> Result<Vec<CollectionPlan>> {
    // `fragment_config` exists only on 0.10. Detect it once and build the
    // projection accordingly (a literal column reference is parsed even inside an
    // unreached CASE branch, so it must be conditionally omitted, not guarded).
    let has_fragment_config: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                WHERE table_schema='pgstac' AND table_name='collections' \
                AND column_name='fragment_config') AS present",
            &[],
        )
        .await?
        .get("present");
    let fragment_expr = if has_fragment_config {
        "to_jsonb(fragment_config)"
    } else {
        "NULL::jsonb"
    };

    let collection_rows = match collection_ids {
        Some(ids) => {
            let q = format!(
                "SELECT id, content, private, partition_trunc, {fragment_expr} AS fragment_config \
                 FROM collections WHERE id = ANY($1) ORDER BY id"
            );
            client.query(&q, &[&ids]).await?
        }
        None => {
            let q = format!(
                "SELECT id, content, private, partition_trunc, {fragment_expr} AS fragment_config \
                 FROM collections ORDER BY id"
            );
            client.query(&q, &[]).await?
        }
    };

    let mut plans = Vec::with_capacity(collection_rows.len());
    for crow in &collection_rows {
        let id: String = crow.get("id");
        let content: Value = crow.get("content");
        let private: Option<Value> = crow.get("private");
        let trunc =
            PartitionTrunc::parse(crow.get::<_, Option<String>>("partition_trunc").as_deref());
        let fragment_config: Option<Vec<String>> = crow
            .get::<_, Option<Value>>("fragment_config")
            .and_then(|v| serde_json::from_value(v).ok());

        // Partition metadata + bounds rendered as text for naming/manifest.
        let prows = client
            .query(
                "SELECT partition, reltuples, \
                        lower(constraint_dtrange)::text AS dt_lower, \
                        upper(constraint_dtrange)::text AS dt_upper \
                 FROM partition_sys_meta WHERE collection = $1 ORDER BY partition",
                &[&id],
            )
            .await?;

        let mut partitions = Vec::with_capacity(prows.len());
        for prow in &prows {
            let name: String = prow.get("partition");
            let reltuples: f32 = prow.get("reltuples");
            let dt_lower: Option<String> = prow.get("dt_lower");
            let dt_upper: Option<String> = prow.get("dt_upper");
            let dtrange = match (range_bound_text(dt_lower.as_deref()), &dt_upper) {
                (Some(lo), Some(up)) if up != "infinity" => Some((lo, up.clone())),
                _ => None,
            };

            // Prune by datetime prefilter: skip a partition whose bounded
            // footprint does not overlap the requested window. Unbounded
            // (NULL-trunc / infinite) partitions are never pruned (scanned with
            // a per-item predicate instead).
            if let Some((want_lo, want_hi)) = &prefilter.datetime
                && let Some((p_lo, p_hi)) = &dtrange
                && (p_hi.as_str() <= want_lo.as_str() || p_lo.as_str() >= want_hi.as_str())
            {
                continue;
            }

            let file_name = file_name_for(trunc, range_bound_text(dt_lower.as_deref()).as_deref());
            partitions.push(PartitionPlan {
                name,
                dtrange,
                reltuples,
                file_name,
            });
        }

        plans.push(CollectionPlan {
            id,
            partition_trunc: trunc,
            content,
            private,
            fragment_config,
            partitions,
        });
    }
    Ok(plans)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trunc_parse_and_str() {
        assert_eq!(PartitionTrunc::parse(Some("month")), PartitionTrunc::Month);
        assert_eq!(PartitionTrunc::parse(Some("year")), PartitionTrunc::Year);
        assert_eq!(PartitionTrunc::parse(None), PartitionTrunc::None);
        assert_eq!(PartitionTrunc::Month.as_manifest_str(), Some("month"));
        assert_eq!(PartitionTrunc::None.as_manifest_str(), None);
    }

    #[test]
    fn file_naming() {
        assert_eq!(
            file_name_for(PartitionTrunc::Month, Some("2024-01-01 00:00:00+00")),
            "202401.parquet"
        );
        assert_eq!(
            file_name_for(PartitionTrunc::Year, Some("2021-01-01 00:00:00+00")),
            "2021.parquet"
        );
        assert_eq!(file_name_for(PartitionTrunc::None, None), "items.parquet");
        // Month with no bound start -> falls back.
        assert_eq!(file_name_for(PartitionTrunc::Month, None), "items.parquet");
    }
}
