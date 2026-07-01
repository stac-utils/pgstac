//! Row sources: fetch dehydrated item rows from a partition (or arbitrary
//! `WHERE`) and turn them into [`DehydratedItem`]s ready for the [`Hydrator`].
//!
//! Columns are read **raw/binary** and converted in Rust: `geometry` arrives as
//! EWKB (the column is selected unwrapped) and is turned into GeoJSON via
//! [`RawGeometry`](crate::geom::RawGeometry); `timestamptz` columns are rendered
//! to STAC text with [`tstz_to_stac_text`](crate::temporal::tstz_to_stac_text).
//! Nothing is pre-formatted server-side.
//!
//! [`Hydrator`]: crate::hydrate::Hydrator

use crate::Result;
use crate::geom::RawGeometry;
use crate::hydrate::HydrationModel;
use crate::hydrate::{
    CollectionContext, DehydratedItem, FragmentContext, Hydrator, PromotedProperties,
};
use crate::temporal::tstz_to_stac_text;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::pin_mut;
use serde_json::{Map, Value};
use std::collections::HashMap;
use tokio_postgres::GenericClient;
use tokio_postgres::types::ToSql;

/// Reads the raw `geometry` column (EWKB) into a GeoJSON [`Value`], or `None` when null.
fn read_geometry(row: &tokio_postgres::Row, col: &str) -> Result<Option<Value>> {
    row.try_get::<_, Option<RawGeometry>>(col)?
        .map(|g| g.to_geojson())
        .transpose()
}

/// How a promoted column maps to a JSON value, derived from its Postgres type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromotedKind {
    Text,
    Float,
    Int,
    BigInt,
    TextArray,
    Jsonb,
    /// `timestamptz` rendered to STAC text by the source query.
    TstzText,
}

impl PromotedKind {
    /// Maps a Postgres `udt_name`/`data_type` to a kind.
    fn from_pg_type(udt_name: &str) -> PromotedKind {
        match udt_name {
            "text" | "varchar" | "bpchar" => PromotedKind::Text,
            "float8" | "float4" | "numeric" => PromotedKind::Float,
            "int4" | "int2" => PromotedKind::Int,
            "int8" => PromotedKind::BigInt,
            "_text" | "_varchar" => PromotedKind::TextArray,
            "jsonb" | "json" => PromotedKind::Jsonb,
            "timestamptz" | "timestamp" => PromotedKind::TstzText,
            // Unknown -> jsonb is the safest passthrough (PostGIS geom etc. don't
            // appear in the promoted set).
            _ => PromotedKind::Jsonb,
        }
    }
}

/// One promoted column: its STAC property name, SQL column, and value kind.
#[derive(Debug, Clone)]
struct PromotedColumn {
    stac_name: String,
    column: String,
    kind: PromotedKind,
}

/// The promoted-column schema for an instance, derived at runtime from
/// `promoted_item_property_defs()` (STAC name + ordered column list) joined with
/// the items table column types.
///
/// Deriving this at runtime — rather than hardcoding — keeps the source portable
/// across pgstac schema revisions, which differ in their promoted-column set
/// (e.g. `eo:bands` vs `bands`, `proj:epsg` vs `proj:code`). See
/// `findings/EXPORT-hydrate-schema-versions.md`.
#[derive(Debug, Clone)]
pub struct PromotedSchema {
    columns: Vec<PromotedColumn>,
}

/// Hydration invariants for a pgstac database: the storage model + the fragment-model promoted schema.
/// These don't change between calls, so a stateful client/pool detects them once and caches them,
/// saving the catalog round trips (`detect_hydration_model` + `PromotedSchema::load`) on every search.
#[derive(Debug, Clone)]
pub(crate) struct CachedHydration {
    pub(crate) model: HydrationModel,
    pub(crate) schema: Option<std::sync::Arc<PromotedSchema>>,
}

impl CachedHydration {
    /// Detects the storage model and loads the promoted schema (fragment model only).
    pub(crate) async fn detect<C: GenericClient>(client: &C) -> Result<Self> {
        let model = detect_hydration_model(client).await?;
        let schema = match model {
            HydrationModel::Fragment => {
                Some(std::sync::Arc::new(PromotedSchema::load(client).await?))
            }
            HydrationModel::BaseItem => None,
        };
        Ok(CachedHydration { model, schema })
    }
}

impl PromotedSchema {
    /// Loads the promoted-column schema from the live instance.
    ///
    /// Uses `promoted_item_property_defs()` for the ordered `(stac_name, column)`
    /// mapping (the order is load-bearing for byte-parity) and
    /// `information_schema.columns` for each column's type.
    pub async fn load<C: GenericClient>(client: &C) -> Result<PromotedSchema> {
        // Column -> udt_name for the items table.
        let type_rows = client
            .query(
                "SELECT column_name, udt_name FROM information_schema.columns \
                 WHERE table_schema = 'pgstac' AND table_name = 'items'",
                &[],
            )
            .await?;
        let mut types = HashMap::new();
        for row in &type_rows {
            let name: String = row.get("column_name");
            let udt: String = row.get("udt_name");
            let _ = types.insert(name, udt);
        }

        // Ordered (stac_name, column) from the metadata function. Its row type is
        // (name, definition, property_path) where property_path is the column.
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
            let udt = types.get(&column).map(String::as_str).unwrap_or("jsonb");
            columns.push(PromotedColumn {
                stac_name,
                kind: PromotedKind::from_pg_type(udt),
                column,
            });
        }
        Ok(PromotedSchema { columns })
    }

    /// SELECT expressions for the promoted columns — the raw column names. The
    /// `timestamptz` columns are rendered to STAC text in Rust, not the server.
    fn select_exprs(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.column.clone()).collect()
    }
}

/// Builds the `SELECT` projection for the 0.10 (fragment) model.
///
/// Columns are raw: `geometry` is the unwrapped EWKB column and `datetime`/`end_datetime` are raw
/// `timestamptz` (converted in Rust). This is used by the export/partition-scan path; the search path
/// runs `search_plan`'s own (equivalent, fields-aware) projection.
pub(crate) fn fragment_select_columns(schema: &PromotedSchema) -> Vec<String> {
    let mut cols = vec![
        "id".to_string(),
        "collection".to_string(),
        "geometry".to_string(),
        "datetime_is_range".to_string(),
        "datetime".to_string(),
        "end_datetime".to_string(),
        "bbox".to_string(),
        "links".to_string(),
        "assets".to_string(),
        "properties".to_string(),
        "extra".to_string(),
        "stac_version".to_string(),
        "stac_extensions".to_string(),
        "link_hrefs".to_string(),
        "fragment_id".to_string(),
    ];
    cols.extend(schema.select_exprs());
    cols
}

/// Reads one [`tokio_postgres::Row`] (fragment model) into a [`DehydratedItem`].
///
/// Reads the raw columns and does the conversions in Rust: EWKB→GeoJSON geometry and
/// `timestamptz`→STAC text.
pub(crate) fn read_fragment_row(
    row: &tokio_postgres::Row,
    schema: &PromotedSchema,
) -> Result<DehydratedItem> {
    let datetime_is_range: bool = row.try_get("datetime_is_range")?;
    let dt_text: Option<String> = row
        .try_get::<_, Option<DateTime<Utc>>>("datetime")?
        .map(tstz_to_stac_text);
    let edt_text: Option<String> = row
        .try_get::<_, Option<DateTime<Utc>>>("end_datetime")?
        .map(tstz_to_stac_text);

    let mut promoted = PromotedProperties {
        temporal: build_temporal(datetime_is_range, dt_text, edt_text),
        fields: Vec::new(),
        owned_fields: Vec::with_capacity(schema.columns.len()),
    };
    for col in &schema.columns {
        let value = read_promoted(row, &col.column, col.kind)?;
        promoted.owned_fields.push((col.stac_name.clone(), value));
    }

    Ok(DehydratedItem {
        id: row.try_get("id")?,
        collection: row.try_get("collection")?,
        geometry: read_geometry(row, "geometry")?,
        content: None,
        bbox: row.try_get("bbox")?,
        links: row.try_get("links")?,
        assets: row.try_get("assets")?,
        properties: row.try_get("properties")?,
        extra: row.try_get("extra")?,
        stac_version: row.try_get("stac_version")?,
        stac_extensions: row.try_get("stac_extensions")?,
        link_hrefs: row.try_get("link_hrefs")?,
        fragment_id: row.try_get("fragment_id")?,
        promoted,
    })
}

/// Reads a single promoted column value into a JSON-ready [`Value`].
fn read_promoted(row: &tokio_postgres::Row, col: &str, kind: PromotedKind) -> Result<Value> {
    let value = match kind {
        PromotedKind::Text => row
            .try_get::<_, Option<String>>(col)?
            .map(Value::String)
            .unwrap_or(Value::Null),
        PromotedKind::TstzText => row
            .try_get::<_, Option<DateTime<Utc>>>(col)?
            .map(|dt| Value::String(tstz_to_stac_text(dt)))
            .unwrap_or(Value::Null),
        PromotedKind::Float => row
            .try_get::<_, Option<f64>>(col)?
            .map(float8_to_json)
            .unwrap_or(Value::Null),
        PromotedKind::Int => row
            .try_get::<_, Option<i32>>(col)?
            .map(|v| Value::Number(v.into()))
            .unwrap_or(Value::Null),
        PromotedKind::BigInt => row
            .try_get::<_, Option<i64>>(col)?
            .map(|v| Value::Number(v.into()))
            .unwrap_or(Value::Null),
        PromotedKind::TextArray => match row.try_get::<_, Option<Vec<String>>>(col)? {
            Some(items) => Value::Array(items.into_iter().map(Value::String).collect()),
            None => Value::Null,
        },
        PromotedKind::Jsonb => row.try_get::<_, Option<Value>>(col)?.unwrap_or(Value::Null),
    };
    Ok(value)
}

/// Converts a `float8` to a JSON number matching Postgres `to_jsonb(float8)`.
///
/// Postgres jsonb stores numbers as `numeric`, so an **integral** float renders
/// without a decimal point (`10.0` -> `10`); non-integral floats keep their
/// fractional part. Mirroring that keeps the promoted float columns byte-equal
/// to the SQL `content_hydrate` output (whose values come through jsonb).
fn float8_to_json(v: f64) -> Value {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < (i64::MAX as f64) {
        // Integral and i64-representable -> integer, matching jsonb.
        Value::Number((v as i64).into())
    } else {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

/// Builds the temporal block matching `temporal_properties_from_item`.
fn build_temporal(
    datetime_is_range: bool,
    dt_text: Option<String>,
    edt_text: Option<String>,
) -> Map<String, Value> {
    let mut m = Map::new();
    if datetime_is_range {
        // jsonb_build_object('datetime', NULL) || strip_nulls({start, end})
        let _ = m.insert("datetime".to_string(), Value::Null);
        if let Some(dt) = dt_text {
            let _ = m.insert("start_datetime".to_string(), Value::String(dt));
        }
        if let Some(edt) = edt_text {
            let _ = m.insert("end_datetime".to_string(), Value::String(edt));
        }
    } else {
        // jsonb_build_object('datetime', tstz_to_stac_text(datetime))
        let _ = m.insert(
            "datetime".to_string(),
            dt_text.map(Value::String).unwrap_or(Value::Null),
        );
    }
    m
}

/// Reads one [`tokio_postgres::Row`] (base_item model, 0.9.11) into a
/// [`DehydratedItem`].
pub(crate) fn read_base_item_row(row: &tokio_postgres::Row) -> Result<DehydratedItem> {
    Ok(DehydratedItem {
        id: row.try_get("id")?,
        collection: row.try_get("collection")?,
        geometry: read_geometry(row, "geometry")?,
        content: row.try_get("content")?,
        ..Default::default()
    })
}

/// The `SELECT` projection for the 0.9.11 (base_item) model. Geometry is the raw EWKB column.
pub(crate) fn base_item_select_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "collection".to_string(),
        "geometry".to_string(),
        "content".to_string(),
    ]
}

/// Fetches dehydrated rows for the given model from `from_clause` with an
/// optional `where_clause`, applying `params`. Used by parity tests and the
/// partition source.
///
/// `from_clause` is a table or partition name (or subquery). `where_clause` is
/// the predicate body (without `WHERE`); pass `None` for all rows. `limit`
/// caps the number of rows (None = no limit). Results are ordered
/// `(datetime, id)` to match the dump contract.
pub async fn fetch_dehydrated<C: GenericClient>(
    client: &C,
    model: HydrationModel,
    from_clause: &str,
    where_clause: Option<&str>,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<DehydratedItem>> {
    fetch_dehydrated_limited(client, model, from_clause, where_clause, params, None).await
}

/// Like [`fetch_dehydrated`] but with an optional row `limit`.
pub async fn fetch_dehydrated_limited<C: GenericClient>(
    client: &C,
    model: HydrationModel,
    from_clause: &str,
    where_clause: Option<&str>,
    params: &[&(dyn ToSql + Sync)],
    limit: Option<i64>,
) -> Result<Vec<DehydratedItem>> {
    // The fragment model needs the runtime-derived promoted-column schema.
    let promoted_schema = match model {
        HydrationModel::Fragment => Some(PromotedSchema::load(client).await?),
        HydrationModel::BaseItem => None,
    };
    let columns = match model {
        HydrationModel::BaseItem => base_item_select_columns(),
        HydrationModel::Fragment => fragment_select_columns(
            promoted_schema
                .as_ref()
                .expect("fragment model loads a promoted schema"),
        ),
    };
    let where_sql = match where_clause {
        Some(w) => format!("WHERE {w}"),
        None => String::new(),
    };
    let limit_sql = match limit {
        Some(n) => format!("LIMIT {n}"),
        None => String::new(),
    };
    let query = format!(
        "SELECT {} FROM {} {} ORDER BY datetime, id {}",
        columns.join(", "),
        from_clause,
        where_sql,
        limit_sql,
    );
    let rows = client.query(&query, params).await?;
    match model {
        HydrationModel::BaseItem => rows.iter().map(read_base_item_row).collect(),
        HydrationModel::Fragment => {
            let schema = promoted_schema
                .as_ref()
                .expect("fragment model loads a promoted schema");
            rows.iter()
                .map(|row| read_fragment_row(row, schema))
                .collect()
        }
    }
}

/// Loads a single fragment by id (0.10) — used by the streaming iterator's per-new-id parallel fetch.
pub async fn fetch_fragment<C: GenericClient>(
    client: &C,
    id: i64,
) -> Result<Option<FragmentContext>> {
    let rows = client
        .query(
            "SELECT content, links_template FROM item_fragments WHERE id = $1",
            &[&id],
        )
        .await?;
    match rows.first() {
        Some(row) => Ok(Some(FragmentContext {
            content: row.try_get("content")?,
            links_template: row.try_get("links_template")?,
        })),
        None => Ok(None),
    }
}

/// Loads the shared fragment content for a set of fragment ids (0.10).
pub async fn fetch_fragments<C: GenericClient>(
    client: &C,
    ids: &[i64],
) -> Result<HashMap<i64, FragmentContext>> {
    let mut map = HashMap::new();
    if ids.is_empty() {
        return Ok(map);
    }
    let rows = client
        .query(
            "SELECT id, content, links_template FROM item_fragments WHERE id = ANY($1)",
            &[&ids],
        )
        .await?;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let _ = map.insert(
            id,
            FragmentContext {
                content: row.try_get("content")?,
                links_template: row.try_get("links_template")?,
            },
        );
    }
    Ok(map)
}

/// Loads **all** of a collection's fragments into a cache keyed by fragment id. Fragments are the
/// deduplicated shared part of item content, so a collection has only a handful regardless of item
/// count — cheap to load whole. Used to pre-populate the fragment cache before a streaming partition
/// scan so hydration never issues a query while the scan's portal is open (see [`scan_partition`]).
pub async fn fetch_collection_fragments<C: GenericClient>(
    client: &C,
    collection: &str,
) -> Result<HashMap<i64, FragmentContext>> {
    let mut map = HashMap::new();
    let rows = client
        .query(
            "SELECT id, content, links_template FROM item_fragments WHERE collection = $1",
            &[&collection],
        )
        .await?;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let _ = map.insert(
            id,
            FragmentContext {
                content: row.try_get("content")?,
                links_template: row.try_get("links_template")?,
            },
        );
    }
    Ok(map)
}

/// A per-item prefilter applied during a partition scan.
#[derive(Debug, Clone, Default)]
pub struct ScanFilter {
    /// Inclusive-start, exclusive-end datetime window (STAC text). Applied as
    /// `datetime >= start AND datetime < end` (datetime = pgstac `datetime`
    /// column = start_datetime for ranges, matching the dump ordering key).
    pub datetime: Option<(String, String)>,
    /// Spatial bbox `[w, s, e, n]`; applied as `ST_Intersects(geometry, env)`.
    pub bbox: Option<[f64; 4]>,
}

/// Builds the `WHERE` body and bound params for a [`ScanFilter`].
///
/// Returns `(where_body, owned_param_values)`. Bbox is inlined as numeric
/// literals (finite f64 -> safe), datetime as bound `$N` params.
fn scan_where(filter: &ScanFilter) -> (String, Vec<String>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<String> = Vec::new();
    if let Some((lo, hi)) = &filter.datetime {
        // Bind as text, then cast in SQL, so the param's Rust type (String)
        // matches what the driver sends ($N::text::timestamptz).
        params.push(lo.clone());
        clauses.push(format!("datetime >= ${}::text::timestamptz", params.len()));
        params.push(hi.clone());
        clauses.push(format!("datetime < ${}::text::timestamptz", params.len()));
    }
    if let Some([w, s, e, n]) = &filter.bbox {
        // Coordinates are finite f64; format as plain decimals (no injection).
        clauses.push(format!(
            "ST_Intersects(geometry, ST_MakeEnvelope({w}, {s}, {e}, {n}, 4326))"
        ));
    }
    (clauses.join(" AND "), params)
}

/// Streams a single partition's items, hydrated, in `(datetime, id)` order,
/// using a server-side cursor (`query_raw` portal) so server memory stays flat.
///
/// `callback` is invoked once per batch of hydrated items (batch size chosen by
/// the driver's portal); returning `Err` aborts the scan. Fragments referenced
/// by the batch are looked up lazily and cached across batches.
///
/// Works on both 0.9.11 (base_item) and 0.10 (fragment); the only server-side
/// state is the cursor, so it does not need the keyset search engine.
pub async fn scan_partition<C, F>(
    client: &C,
    model: HydrationModel,
    partition: &str,
    collection: &str,
    ctx: &CollectionContext,
    filter: &ScanFilter,
    mut callback: F,
) -> Result<u64>
where
    C: GenericClient,
    F: FnMut(Vec<Value>) -> Result<()>,
{
    let hydrator = Hydrator::new(model);
    let promoted_schema = match model {
        HydrationModel::Fragment => Some(PromotedSchema::load(client).await?),
        HydrationModel::BaseItem => None,
    };
    let columns = match model {
        HydrationModel::BaseItem => base_item_select_columns(),
        HydrationModel::Fragment => fragment_select_columns(
            promoted_schema
                .as_ref()
                .expect("fragment model loads a promoted schema"),
        ),
    };
    let (where_body, where_params) = scan_where(filter);
    let where_sql = if where_body.is_empty() {
        String::new()
    } else {
        format!("WHERE {where_body}")
    };
    let query = format!(
        "SELECT {} FROM {} {} ORDER BY datetime, id",
        columns.join(", "),
        partition,
        where_sql,
    );

    // Bind params as &dyn ToSql (coercion, not a cast — keeps trivial_casts happy).
    let params: Vec<&(dyn ToSql + Sync)> = where_params
        .iter()
        .map(|p| {
            let r: &(dyn ToSql + Sync) = p;
            r
        })
        .collect();

    // Pre-load ALL of this collection's (deduplicated, few) fragments BEFORE opening the streaming
    // portal below. Fetching fragments lazily inside the scan loop issued a query on this same
    // connection while the portal's unread rows filled the connection buffer, deadlocking on any
    // non-trivial partition (the server blocks sending the fragment result, the client blocks not
    // draining the portal). Pre-loading keeps the portal scan query-free. `flush_batch` keeps a lazy
    // fetch only as a fallback for the rare case of a fragment committed after this load.
    let mut frag_cache: HashMap<i64, FragmentContext> = match model {
        HydrationModel::Fragment => fetch_collection_fragments(client, collection).await?,
        HydrationModel::BaseItem => HashMap::new(),
    };

    // query_raw streams via a portal (flat server memory).
    let stream = client.query_raw(&query, params).await?;
    pin_mut!(stream);

    // Buffer rows into modest batches so the callback amortizes work while
    // memory stays bounded.
    const BATCH: usize = 1024;
    let mut row_batch: Vec<tokio_postgres::Row> = Vec::with_capacity(BATCH);
    let mut total: u64 = 0;

    while let Some(row) = stream.next().await {
        row_batch.push(row?);
        if row_batch.len() >= BATCH {
            total += flush_batch(
                client,
                &hydrator,
                model,
                promoted_schema.as_ref(),
                ctx,
                &mut frag_cache,
                &mut row_batch,
                &mut callback,
            )
            .await?;
        }
    }
    if !row_batch.is_empty() {
        total += flush_batch(
            client,
            &hydrator,
            model,
            promoted_schema.as_ref(),
            ctx,
            &mut frag_cache,
            &mut row_batch,
            &mut callback,
        )
        .await?;
    }
    Ok(total)
}

/// Hydrates a batch of raw rows and hands the items to the callback.
#[allow(clippy::too_many_arguments)]
async fn flush_batch<C, F>(
    client: &C,
    hydrator: &Hydrator,
    model: HydrationModel,
    promoted_schema: Option<&PromotedSchema>,
    ctx: &CollectionContext,
    frag_cache: &mut HashMap<i64, FragmentContext>,
    row_batch: &mut Vec<tokio_postgres::Row>,
    callback: &mut F,
) -> Result<u64>
where
    C: GenericClient,
    F: FnMut(Vec<Value>) -> Result<()>,
{
    // Parse rows into dehydrated items.
    let mut items: Vec<DehydratedItem> = Vec::with_capacity(row_batch.len());
    for row in row_batch.iter() {
        let item = match model {
            HydrationModel::BaseItem => read_base_item_row(row)?,
            HydrationModel::Fragment => read_fragment_row(
                row,
                promoted_schema.expect("fragment model loads a promoted schema"),
            )?,
        };
        items.push(item);
    }
    row_batch.clear();

    // Resolve any new fragment ids not yet cached.
    if model == HydrationModel::Fragment {
        let missing: Vec<i64> = items
            .iter()
            .filter_map(|i| i.fragment_id)
            .filter(|id| !frag_cache.contains_key(id))
            .collect();
        if !missing.is_empty() {
            let fetched = fetch_fragments(client, &missing).await?;
            frag_cache.extend(fetched);
        }
    }

    let count = items.len() as u64;
    let hydrated: Vec<Value> = items
        .into_iter()
        .map(|item| {
            let fragment = item.fragment_id.and_then(|id| frag_cache.get(&id));
            hydrator.hydrate(item, ctx, fragment)
        })
        .collect();
    callback(hydrated)?;
    Ok(count)
}

/// Detects which storage [`HydrationModel`] a database uses.
///
/// A `pgstac.items.fragment_id` column means the 0.10 fragment model; its absence means the 0.9.11
/// `base_item` model.
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

/// Loads the per-collection hydration context (the `base_item` for the 0.9.11 model; empty for the
/// 0.10 fragment model, whose shared content lives in `item_fragments`).
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
