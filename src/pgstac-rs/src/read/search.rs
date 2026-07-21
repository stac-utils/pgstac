//! Client-side search driven by `search_plan`.
//!
//! `search_plan` returns a ready-to-prepare SELECT (projection, `WHERE`, keyset seek and `ORDER BY`
//! all baked in), the per-band datetime histogram, the lead direction, and the context count/query.
//! This module does the rest in Rust:
//!
//! 1. call `search_plan` **once** and `prepare()` its query, so stepping multiple histogram bands
//!    reuses one plan (no replanning);
//! 2. step the bands, binding `$1`/`$2` (band low/high) and `$3` (limit) per band, until the page is
//!    full — or, for a non-datetime-leading sort, run the single `$1`=limit query;
//! 3. read each row's **raw** columns (EWKB geometry, raw `timestamptz`) and **hydrate in Rust** with
//!    the [`Hydrator`](crate::hydrate::Hydrator);
//! 4. **mint the keyset tokens in Rust** ([`crate::keyset`]) from the boundary features — no extra
//!    round trip. Tokens are validated by page-equivalence (the server casts each value back).

use crate::Result;
use crate::hydrate::{HydrationModel, Hydrator};
use crate::keyset;
use crate::source::{
    PromotedSchema, detect_hydration_model, fetch_fragments, load_collection_context,
    read_base_item_row, read_fragment_row,
};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, Row};

/// One page of a search: hydrated features plus the keyset pagination tokens and context.
#[derive(Debug, Clone, Default)]
pub struct SearchPage {
    /// The hydrated STAC items.
    pub features: Vec<Value>,
    /// The `next:`-prefixed continuation token, if there is a next page.
    pub next_token: Option<String>,
    /// The `prev:`-prefixed continuation token, if there is a previous page.
    pub prev_token: Option<String>,
    /// The number of features returned in this page.
    pub number_returned: usize,
    /// The total number of matches, when context counting is enabled.
    pub number_matched: Option<i64>,
}

/// The pieces of `search_plan` the client needs to step + tokenize a page.
pub(crate) struct Plan {
    /// The ready-to-prepare SELECT.
    pub(crate) query: String,
    /// The per-month `[{m, n}]` histogram, when the sort is datetime-leading.
    pub(crate) histogram: Option<Value>,
    /// Whether the effective lead direction is descending (already accounts for `prev`).
    pub(crate) lead_desc: bool,
    /// Whether the sort leads with datetime (so the query is `$1,$2,$3` band-stepped).
    pub(crate) datetime_leading: bool,
    /// The inlined context count, when fresh.
    pub(crate) context_count: Option<i64>,
    /// The query to run for the context count on a cache miss.
    pub(crate) ctx_query: Option<String>,
    /// The search carries a property filter (`filter`/`query`), so its matches can be sparse across
    /// datetime bands. [`step_bands`] then scans the whole range in one query (like SQL `search()`)
    /// instead of geometric-doubling through sparse leading bands (many round trips).
    pub(crate) selective: bool,
}

/// A high sentinel for the most recent band's exclusive upper bound (no items exist past the last
/// histogram month, so any value above them works).
fn far_future() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap()
}

/// Fetches the plan for a search in a single round trip.
pub(crate) async fn fetch_plan<C: GenericClient>(
    client: &C,
    search: &Value,
    token: Option<&str>,
    limit: i64,
) -> Result<Plan> {
    let limit = i32::try_from(limit)?;
    let row = client
        .query_one(
            "SELECT query, histogram, lead_desc, datetime_leading, context_count, ctx_query \
             FROM search_plan($1::jsonb, $2, $3)",
            &[search, &token, &limit],
        )
        .await?;
    Ok(Plan {
        query: row.try_get("query")?,
        histogram: row.try_get("histogram")?,
        lead_desc: row.try_get("lead_desc")?,
        datetime_leading: row.try_get("datetime_leading")?,
        context_count: row.try_get("context_count")?,
        ctx_query: row.try_get("ctx_query")?,
        selective: search.get("filter").is_some() || search.get("query").is_some(),
    })
}

/// Parses the histogram into contiguous `(low, high)` datetime bands (ascending), one per month, that
/// together cover the whole histogram range up to [`far_future`].
///
/// Every month is kept — bands must NOT be dropped by count. The histogram counts are an estimate
/// (`partition_bounds` prorates a partition's row count uniformly across its dtrange months), so a sparse
/// collection — few items over a long span — rounds every month to 0 even though the data is there.
/// Dropping zero-count months would leave datetime gaps and silently lose those rows (SQL `search()`
/// scans the whole range regardless). [`step_bands`] sizes its scan windows by geometric doubling, not by
/// these counts, so keeping the empty months costs only a few extra (cheap) empty-window probes.
pub(crate) fn histogram_bands(histogram: &Value) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let entries: Vec<(DateTime<Utc>, i64)> = histogram
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .map(|e| {
            let m = e["m"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc));
            let n = e["n"].as_i64().unwrap_or(0);
            (m, n)
        })
        .filter_map(|(m, n)| m.map(|m| (m, n)))
        .collect();

    let mut bands = Vec::new();
    for i in 0..entries.len() {
        let (low, _n) = entries[i];
        let high = entries.get(i + 1).map(|e| e.0).unwrap_or_else(far_future);
        bands.push((low, high));
    }
    Ok(bands)
}

/// The ordered band ranges to walk for a plan: the datetime histogram bands in lead order, or a single
/// `None` band for a non-datetime sort (the query has no band parameters). Used by the streaming
/// iterator (the buffered [`step_bands`] inlines an equivalent walk with its geometric windowing).
#[cfg(feature = "pool")]
#[allow(clippy::type_complexity)]
pub(crate) fn band_ranges(plan: &Plan) -> Result<Vec<Option<(DateTime<Utc>, DateTime<Utc>)>>> {
    if !plan.datetime_leading {
        return Ok(vec![None]);
    }
    let mut bands = match &plan.histogram {
        Some(h) => histogram_bands(h)?,
        None => Vec::new(),
    };
    if plan.lead_desc {
        bands.reverse();
    }
    Ok(bands.into_iter().map(Some).collect())
}

/// Runs the prepared query across the datetime histogram bands until `want` rows are collected.
async fn step_bands<C: GenericClient>(client: &C, plan: &Plan, want: i64) -> Result<Vec<Row>> {
    let stmt = client.prepare(&plan.query).await?;

    if !plan.datetime_leading {
        // Non-datetime sort: a single `$1`=limit query covers the whole range.
        return Ok(client.query(&stmt, &[&want]).await?);
    }

    let mut bands = match &plan.histogram {
        Some(h) => histogram_bands(h)?,
        None => Vec::new(),
    };
    if plan.lead_desc {
        bands.reverse();
    }

    // Geometric-window band walk (non-lossy): query the
    // next `window` contiguous bands as ONE statement with `LIMIT = remaining`, doubling the window each
    // step. A window that returns `< remaining` rows is exhausted (advance past it); one that fills
    // `remaining` ends the walk. Starting at a single band keeps time-to-first-row low and avoids the
    // wide-`Append` planning tax of scanning every partition up front; the doubling keeps a deep page (or
    // a token that has consumed the leading bands) down to ~log(bands) round trips instead of one per band.
    let mut rows: Vec<Row> = Vec::new();
    let mut remaining = want;
    let mut i = 0usize;
    // Unfiltered datetime sorts fill from the first band(s), so start narrow (low time-to-first-row) and
    // double. A property filter can be sparse across bands, so scan the whole range in one query (like
    // SQL `search()`) rather than walking sparse leading bands one round trip at a time.
    let mut window = if plan.selective {
        bands.len().max(1)
    } else {
        1
    };
    while i < bands.len() && remaining > 0 {
        let end = (i + window).min(bands.len());
        // The window spans contiguous bands, so its datetime range is [min low, max high).
        let low = bands[i..end]
            .iter()
            .map(|b| b.0)
            .min()
            .expect("non-empty window");
        let high = bands[i..end]
            .iter()
            .map(|b| b.1)
            .max()
            .expect("non-empty window");
        let params: [&(dyn ToSql + Sync); 3] = [&low, &high, &remaining];
        let band_rows = client.query(&stmt, &params).await?;
        let n = band_rows.len() as i64;
        rows.extend(band_rows);
        if n >= remaining {
            break; // filled the page from this window
        }
        remaining -= n;
        i = end;
        window *= 2;
    }
    Ok(rows)
}

/// The total match count (`numberMatched`), or `None` when context counting is off.
async fn resolve_context<C: GenericClient>(client: &C, plan: &Plan) -> Result<Option<i64>> {
    if let Some(count) = plan.context_count {
        return Ok(Some(count));
    }
    let Some(ctx_query) = &plan.ctx_query else {
        return Ok(None);
    };
    let row = client.query_one(ctx_query, &[]).await?;
    Ok(row.try_get::<_, Option<i64>>(0)?)
}

/// Runs one page of a search: prepares `search_plan`'s query, steps the histogram bands, hydrates the
/// rows in Rust, and mints the pagination tokens in Rust.
///
/// Detects the hydration model (and loads the promoted schema) on every call; callers that already
/// know these — e.g. [`PgstacPool`](crate::PgstacPool), which caches them — use
/// [`search_page_with`] to skip those catalog round trips.
pub async fn search_page<C: GenericClient>(
    client: &C,
    search: &Value,
    token: Option<&str>,
    limit: i64,
) -> Result<SearchPage> {
    let model = detect_hydration_model(client).await?;
    let promoted = match model {
        HydrationModel::Fragment => Some(PromotedSchema::load(client).await?),
        HydrationModel::BaseItem => None,
    };
    search_page_with(client, model, promoted.as_ref(), search, token, limit).await
}

/// Like [`search_page`] but with the hydration model + promoted schema supplied (so they aren't
/// re-detected per call).
pub async fn search_page_with<C: GenericClient>(
    client: &C,
    model: HydrationModel,
    promoted: Option<&PromotedSchema>,
    search: &Value,
    token: Option<&str>,
    limit: i64,
) -> Result<SearchPage> {
    let is_prev = token.is_some_and(|t| t.starts_with("prev:"));
    let plan = fetch_plan(client, search, token, limit).await?;

    // Fetch one extra row across the bands to detect whether a further page exists.
    let rows = step_bands(client, &plan, limit + 1).await?;
    let mut items = match (&model, &promoted) {
        (HydrationModel::Fragment, Some(schema)) => rows
            .iter()
            .map(|row| read_fragment_row(row, schema))
            .collect::<Result<Vec<_>>>()?,
        _ => rows
            .iter()
            .map(read_base_item_row)
            .collect::<Result<Vec<_>>>()?,
    };

    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);

    // When paginating backwards the rows came back in reverse order; flip them to forward order.
    if is_prev {
        items.reverse();
    }

    // Hydrate: load each collection's context once and the referenced fragments.
    let mut contexts: HashMap<String, _> = HashMap::new();
    for item in &items {
        if !contexts.contains_key(&item.collection) {
            let ctx = load_collection_context(client, model, &item.collection).await?;
            let _ = contexts.insert(item.collection.clone(), ctx);
        }
    }
    let frag_ids: Vec<i64> = items.iter().filter_map(|i| i.fragment_id).collect();
    let fragments = fetch_fragments(client, &frag_ids).await?;
    let hydrator = Hydrator::new(model);
    let features: Vec<Value> = items
        .into_iter()
        .map(|item| {
            let ctx = &contexts[&item.collection];
            let fragment = item.fragment_id.and_then(|id| fragments.get(&id));
            hydrator.hydrate(item, ctx, fragment)
        })
        .collect();

    // Mint tokens in Rust from the boundary features' sort-key values.
    let fields = keyset::sort_key_fields(search);
    let (mut next_token, mut prev_token) = (None, None);
    let next_present = if is_prev { token.is_some() } else { has_more };
    let prev_present = if is_prev { has_more } else { token.is_some() };
    if let Some(last) = features.last()
        && next_present
    {
        next_token = Some(format!("next:{}", keyset::mint_token(last, &fields)));
    }
    if let Some(first) = features.first()
        && prev_present
    {
        prev_token = Some(format!("prev:{}", keyset::mint_token(first, &fields)));
    }

    let number_matched = resolve_context(client, &plan).await?;

    // Apply the STAC `fields` projection last, so token minting above still saw the full sort-key
    // values even when `fields` excludes them.
    let features = project_fields(features, search);

    Ok(SearchPage {
        number_returned: features.len(),
        features,
        next_token,
        prev_token,
        number_matched,
    })
}

/// Applies the search's STAC `fields` include/exclude projection to each feature (a no-op when the
/// search has no `fields`).
pub(crate) fn project_fields(features: Vec<Value>, search: &Value) -> Vec<Value> {
    match search.get("fields") {
        Some(fields) if fields.is_object() => features
            .into_iter()
            .map(|feature| crate::fields::apply_fields(feature, fields))
            .collect(),
        _ => features,
    }
}

/// The page size used to paginate `search_collect` / `search_stream` when the search body has no
/// explicit `limit`.
const DEFAULT_BATCH: i64 = 1000;

/// The per-page batch size for a search, bounded by `max_items` so the last fetch doesn't overshoot.
fn batch_size(search: &Value, collected: usize, max_items: Option<usize>) -> i64 {
    let base = search
        .get("limit")
        .and_then(Value::as_i64)
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_BATCH);
    match max_items {
        Some(max) => {
            let remaining = max.saturating_sub(collected) as i64;
            remaining.min(base)
        }
        None => base,
    }
}

/// Collects every matching item (up to `max_items`) into one page by following the keyset `next`
/// tokens. The returned [`SearchPage`] carries the full set of features (and the match count); it has
/// no continuation tokens.
pub async fn search_collect<C: GenericClient>(
    client: &C,
    search: &Value,
    max_items: Option<usize>,
) -> Result<SearchPage> {
    let mut features: Vec<Value> = Vec::new();
    let mut token: Option<String> = None;
    let mut number_matched = None;
    loop {
        let limit = batch_size(search, features.len(), max_items);
        if limit <= 0 {
            break;
        }
        let page = search_page(client, search, token.as_deref(), limit).await?;
        if number_matched.is_none() {
            number_matched = page.number_matched;
        }
        let next = page.next_token;
        let empty = page.features.is_empty();
        let take = match max_items {
            Some(max) => max - features.len(),
            None => usize::MAX,
        };
        features.extend(page.features.into_iter().take(take));
        match next {
            Some(next) if !empty => token = Some(next),
            _ => break,
        }
    }
    Ok(SearchPage {
        number_returned: features.len(),
        features,
        next_token: None,
        prev_token: None,
        number_matched,
    })
}

/// Streams every matching item (up to `max_items`) as newline-delimited JSON to `write`, following
/// the keyset `next` tokens. Memory stays flat: one page is held at a time. Returns the number of
/// items written.
pub async fn search_stream<C, W>(
    client: &C,
    search: &Value,
    write: &mut W,
    max_items: Option<usize>,
) -> Result<usize>
where
    C: GenericClient,
    W: std::io::Write,
{
    let mut written = 0usize;
    let mut token: Option<String> = None;
    loop {
        let limit = batch_size(search, written, max_items);
        if limit <= 0 {
            break;
        }
        let page = search_page(client, search, token.as_deref(), limit).await?;
        let next = page.next_token;
        for feature in &page.features {
            serde_json::to_writer(&mut *write, feature)?;
            write.write_all(b"\n")?;
            written += 1;
            if max_items.is_some_and(|max| written >= max) {
                return Ok(written);
            }
        }
        match next {
            Some(next) if !page.features.is_empty() => token = Some(next),
            _ => break,
        }
    }
    Ok(written)
}

/// Fetches a single item by id from a collection.
///
/// A getter is just a one-row search with the collection and id pinned, so it shares the same
/// hydration path as [`search_page`].
pub async fn get_item<C: GenericClient>(
    client: &C,
    collection_id: &str,
    item_id: &str,
) -> Result<Option<Value>> {
    let body = json!({"collections": [collection_id], "ids": [item_id], "limit": 1});
    let page = search_page(client, &body, None, 1).await?;
    Ok(page.features.into_iter().next())
}
