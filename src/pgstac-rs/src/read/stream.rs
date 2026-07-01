//! A true streaming search iterator with flat memory.
//!
//! Item rows stream from a server **portal** (`query_raw`) on one pooled connection — a whole page is
//! never buffered. As each row surfaces a `fragment_id` not yet seen, that **single** fragment is
//! fetched synchronously on a **second, parallel** pooled connection and cached. With high fragment
//! sharing (the split-storage design point) the shared fragment crosses the wire once and peak memory
//! stays at ~one row plus the small fragment cache, independent of result size.

#[cfg(feature = "export")]
use crate::export::format::{
    Format, GeoparquetMode, GeoparquetStreamWriter, ParquetCompression, encode_all,
};
use crate::feature::write_fragment_feature;
use crate::hydrate::{CollectionContext, FragmentContext, HydrationModel, Hydrator};
use crate::search::{band_ranges, fetch_plan};
use crate::source::{
    fetch_fragment, load_collection_context, read_base_item_row, read_fragment_row,
};
use crate::{PgstacPool, Result};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::GenericClient;
use tokio_postgres::types::ToSql;

/// The `_limit` passed to `search_plan` for an unbounded stream — affects only the histogram/context
/// estimate, never the streamed result.
const PLAN_LIMIT_HINT: i64 = 10_000;

/// Fetches `fid` once: returns the cached `Arc` if present, otherwise fetches it on `conn` (the
/// parallel connection) and caches it. The `Arc` is cloned out before any await so no borrow of the
/// cache is held across the fetch.
async fn cached_fragment<C: GenericClient>(
    cache: &mut HashMap<i64, Arc<FragmentContext>>,
    conn: &C,
    fid: i64,
) -> Result<Option<Arc<FragmentContext>>> {
    if let Some(fragment) = cache.get(&fid) {
        return Ok(Some(Arc::clone(fragment)));
    }
    let fetched = fetch_fragment(conn, fid).await?.map(Arc::new);
    if let Some(fragment) = &fetched {
        let _ = cache.insert(fid, Arc::clone(fragment));
    }
    Ok(fetched)
}

impl PgstacPool {
    /// Streams hydrated STAC items one at a time with flat memory.
    ///
    /// `limit` caps the number of items (`None` = unbounded). The returned stream owns two pooled
    /// connections for its lifetime: one streams the row portal, the other serves the per-new-fragment
    /// fetches in parallel.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pgstac::{ConnectConfig, PgstacPool};
    /// use futures::StreamExt;
    /// use serde_json::json;
    ///
    /// # tokio_test::block_on(async {
    /// let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();
    /// let mut items = Box::pin(pool.search_items(json!({}), None, Some(5)));
    /// while let Some(item) = items.next().await {
    ///     println!("{}", item.unwrap()["id"]);
    /// }
    /// # })
    /// ```
    pub fn search_items(
        &self,
        search: Value,
        token: Option<String>,
        limit: Option<i64>,
    ) -> impl Stream<Item = Result<Value>> + 'static {
        let pool = self.clone();
        try_stream! {
            // Model + promoted schema come from the pool's shared cache (loaded once, not per stream).
            let hydration = pool.cached_hydration().await?;
            let model = hydration.model;
            let schema = hydration.schema;
            let hydrator = Hydrator::new(model);

            let data = pool.get().await?;
            let frag_conn = pool.get().await?;

            let plan = fetch_plan(&**data, &search, token.as_deref(), limit.unwrap_or(PLAN_LIMIT_HINT)).await?;
            let stmt = data.prepare(&plan.query).await?;
            let bands = band_ranges(&plan)?;
            let fields = search.get("fields").filter(|f| f.is_object()).cloned();

            let mut contexts: HashMap<String, CollectionContext> = HashMap::new();
            let mut frag_cache: HashMap<i64, Arc<FragmentContext>> = HashMap::new();
            let mut remaining = limit;

            'bands: for band in bands {
                if remaining == Some(0) {
                    break;
                }
                let lim = remaining.unwrap_or(i64::MAX);
                let row_stream = match &band {
                    Some((low, high)) => {
                        let params: Vec<&(dyn ToSql + Sync)> = vec![low, high, &lim];
                        data.query_raw(&stmt, params).await?
                    }
                    None => {
                        let params: Vec<&(dyn ToSql + Sync)> = vec![&lim];
                        data.query_raw(&stmt, params).await?
                    }
                };
                futures::pin_mut!(row_stream);

                while let Some(row) = row_stream.next().await {
                    let row = row?;
                    let item = match (&model, &schema) {
                        (HydrationModel::Fragment, Some(s)) => read_fragment_row(&row, s)?,
                        _ => read_base_item_row(&row)?,
                    };

                    // Per-collection context: fragment model is empty (no query); base_item queries on
                    // the parallel connection so the data portal is never interrupted.
                    if !contexts.contains_key(&item.collection) {
                        let ctx = load_collection_context(&**frag_conn, model, &item.collection).await?;
                        let _ = contexts.insert(item.collection.clone(), ctx);
                    }

                    let fragment = match item.fragment_id {
                        None => None,
                        Some(fid) => cached_fragment(&mut frag_cache, &**frag_conn, fid).await?,
                    };

                    let ctx = &contexts[&item.collection];
                    let feature = hydrator.hydrate(item, ctx, fragment.as_deref());
                    let feature = match &fields {
                        Some(f) => crate::fields::apply_fields(feature, f),
                        None => feature,
                    };
                    yield feature;

                    if let Some(ref mut r) = remaining {
                        *r -= 1;
                        if *r == 0 {
                            break 'bands;
                        }
                    }
                }
            }
        }
    }

    /// Collects every matching item (up to `max_items`) by draining [`search_items`](Self::search_items).
    pub async fn search_collect_items(
        &self,
        search: Value,
        max_items: Option<i64>,
    ) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let stream = self.search_items(search, None, max_items);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// Streams every matching item (up to `max_items`) into a stac-geoparquet file written to `sink`.
    ///
    /// On the 0.10 fragment model the schema is registry-complete, so items are written in row-group
    /// batches as they stream — the item working set is one batch, never the whole result. The legacy
    /// base-item model has no registry, so its schema must be widened over the full set; that case
    /// buffers (the same split the dump makes). Returns the number of items written.
    #[cfg(feature = "export")]
    pub async fn stream_geoparquet<W: std::io::Write + Send>(
        &self,
        search: Value,
        max_items: Option<i64>,
        compression: ParquetCompression,
        row_group_size: Option<usize>,
        sink: W,
    ) -> Result<usize> {
        const BATCH: usize = 1024;
        let model = self.cached_hydration().await?.model;
        match model {
            HydrationModel::Fragment => {
                let mut writer = GeoparquetStreamWriter::new(sink, compression);
                if let Some(n) = row_group_size {
                    writer = writer.with_max_row_group_row_count(n);
                }
                let mut written = 0usize;
                let mut batch: Vec<Value> = Vec::with_capacity(BATCH);
                let stream = self.search_items(search, None, max_items);
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    batch.push(item?);
                    written += 1;
                    if batch.len() >= BATCH {
                        writer.write_batch(std::mem::take(&mut batch))?;
                    }
                }
                if !batch.is_empty() {
                    writer.write_batch(batch)?;
                }
                let _ = writer.finish()?;
                Ok(written)
            }
            HydrationModel::BaseItem => {
                // No registry to complete the schema, so widen over the full set: buffer + encode once.
                let mut sink = sink;
                let items = self.search_collect_items(search, max_items).await?;
                let written = items.len();
                let bytes = encode_all(
                    Format::Geoparquet {
                        compression,
                        mode: GeoparquetMode::Buffered,
                        max_row_group_row_count: row_group_size,
                    },
                    items,
                )?;
                sink.write_all(&bytes)?;
                Ok(written)
            }
        }
    }

    /// Streams every matching item (up to `max_items`) as newline-delimited JSON to `write`, holding
    /// only one item at a time. Begins at `token` (`None` streams from the start). Returns the number
    /// of items written.
    ///
    /// For the 0.10 fragment model this uses the byte path
    /// ([`write_fragment_feature`](crate::feature::write_fragment_feature)): each item is serialized
    /// straight to `write` with the shared fragment merged at serialize time and `bbox` emitted from raw
    /// bytes — no per-item [`Value`] is ever materialized. A `fields` projection runs on the value
    /// path instead (it needs the structured feature).
    pub async fn stream_ndjson<W: std::io::Write>(
        &self,
        search: Value,
        token: Option<&str>,
        max_items: Option<i64>,
        write: &mut W,
    ) -> Result<usize> {
        let hydration = self.cached_hydration().await?;
        let model = hydration.model;
        let schema = hydration.schema;
        let hydrator = Hydrator::new(model);

        let data = self.get().await?;
        let frag_conn = self.get().await?;
        let plan = fetch_plan(
            &**data,
            &search,
            token,
            max_items.unwrap_or(PLAN_LIMIT_HINT),
        )
        .await?;
        let stmt = data.prepare(&plan.query).await?;
        let bands = band_ranges(&plan)?;
        let fields = search.get("fields").filter(|f| f.is_object()).cloned();

        let mut contexts: HashMap<String, CollectionContext> = HashMap::new();
        let mut frag_cache: HashMap<i64, Arc<FragmentContext>> = HashMap::new();
        let mut written = 0usize;
        let mut remaining = max_items;

        'bands: for band in bands {
            if remaining == Some(0) {
                break;
            }
            let lim = remaining.unwrap_or(i64::MAX);
            let row_stream = match &band {
                Some((low, high)) => {
                    let params: Vec<&(dyn ToSql + Sync)> = vec![low, high, &lim];
                    data.query_raw(&stmt, params).await?
                }
                None => {
                    let params: Vec<&(dyn ToSql + Sync)> = vec![&lim];
                    data.query_raw(&stmt, params).await?
                }
            };
            futures::pin_mut!(row_stream);

            while let Some(row) = row_stream.next().await {
                let row = row?;
                let item = match (&model, &schema) {
                    (HydrationModel::Fragment, Some(s)) => read_fragment_row(&row, s)?,
                    _ => read_base_item_row(&row)?,
                };

                if !contexts.contains_key(&item.collection) {
                    let ctx =
                        load_collection_context(&**frag_conn, model, &item.collection).await?;
                    let _ = contexts.insert(item.collection.clone(), ctx);
                }
                let fragment = match item.fragment_id {
                    None => None,
                    Some(fid) => cached_fragment(&mut frag_cache, &**frag_conn, fid).await?,
                };

                match model {
                    // Byte path (no per-item Value materialized) — only when no field projection is
                    // requested; `fields` needs the structured feature, so fall back to the value path.
                    HydrationModel::Fragment if fields.is_none() => {
                        write_fragment_feature(item, fragment.as_deref(), &mut *write)?;
                    }
                    _ => {
                        let ctx = &contexts[&item.collection];
                        let feature = hydrator.hydrate(item, ctx, fragment.as_deref());
                        let feature = match &fields {
                            Some(f) => crate::fields::apply_fields(feature, f),
                            None => feature,
                        };
                        serde_json::to_writer(&mut *write, &feature)?;
                    }
                }
                write.write_all(b"\n")?;
                written += 1;

                if let Some(ref mut r) = remaining {
                    *r -= 1;
                    if *r == 0 {
                        break 'bands;
                    }
                }
            }
        }
        Ok(written)
    }
}
