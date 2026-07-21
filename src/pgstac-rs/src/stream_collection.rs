//! `PgstacPool`'s [`stac_io::StreamSearch`] impl: stream items from the keyset portal, mint the
//! pagination links from the first/last item, and count matches concurrently.

use crate::{PgstacPool, keyset};
use futures::StreamExt as _;
use stac::api::{ItemCollection, Search};
use stac_io::{Finalize, ItemStream, StreamSearch, StreamedSearch};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Adapts a pgstac-side error into the [`stac_io::Error`] the shared streaming writer returns.
fn backend(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> stac_io::Error {
    stac_io::Error::from(error.into())
}

impl PgstacPool {
    /// Whether the database has the v0.10 streaming layout: the `search_plan(jsonb, text, integer)`
    /// planner and the `items.fragment_id` column. Sniffed rather than read from a version string, which
    /// can diverge from the actual layout (a fork, a partial migration, a renamed schema).
    pub async fn has_streaming_layout(&self) -> crate::Result<bool> {
        let client = self.get().await?;
        let row = client
            .query_one(
                "SELECT to_regprocedure('pgstac.search_plan(jsonb, text, integer)') IS NOT NULL \
                 AND EXISTS ( \
                     SELECT 1 FROM information_schema.columns \
                     WHERE table_schema = 'pgstac' \
                       AND table_name = 'items' \
                       AND column_name = 'fragment_id' \
                 ) AS has_layout",
                &[],
            )
            .await?;
        Ok(row.get("has_layout"))
    }
}

/// A keyset pagination link: `<self_href>?token=<token>` when a base URL is given, else relative
/// `?token=<token>`. The token is the raw `next:<keyset>` / `prev:<keyset>` value.
fn page_link(self_href: Option<&str>, rel: &str, token: &str) -> stac::Link {
    let href = match self_href {
        Some(base) => format!("{base}?token={token}"),
        None => format!("?token={token}"),
    };
    stac::Link::new(href, rel)
}

/// Fallback for databases without the streaming layout: run the SQL `search()` function (present in any
/// pgstac version), stream its `features`, and deserialize the rest of the response into the collection.
/// The response already carries STAC links (root/self/next), so no minting is needed.
async fn sql_search_fallback(
    pool: &PgstacPool,
    mut search: serde_json::Value,
    token: Option<String>,
    context: bool,
) -> stac_io::Result<StreamedSearch> {
    if let serde_json::Value::Object(map) = &mut search {
        if let Some(token) = token {
            let _ = map.insert("token".into(), serde_json::Value::String(token));
        }
        if context {
            let conf = map.entry("conf").or_insert_with(|| serde_json::json!({}));
            if let serde_json::Value::Object(conf_map) = conf {
                let _ = conf_map.insert("context".into(), serde_json::json!("on"));
            }
        }
    }

    let client = pool.get().await.map_err(backend)?;
    let response: serde_json::Value = client
        .query_one("SELECT pgstac.search($1::jsonb)", &[&search])
        .await
        .map_err(backend)?
        .get(0);
    let mut object = match response {
        serde_json::Value::Object(object) => object,
        _ => return Err(backend("pgstac.search did not return an object")),
    };
    let features = match object.remove("features") {
        Some(serde_json::Value::Array(features)) => features,
        _ => Vec::new(),
    };

    let items: ItemStream = Box::pin(futures::stream::iter(features.into_iter().map(Ok)));
    let finalize: Finalize = Box::new(move |_first, _last, _count| {
        Box::pin(async move {
            let _ = object.insert("features".into(), serde_json::Value::Array(Vec::new()));
            let collection: ItemCollection =
                serde_json::from_value(serde_json::Value::Object(object))?;
            Ok(collection)
        })
    });
    Ok(StreamedSearch { items, finalize })
}

impl StreamSearch for PgstacPool {
    fn stream_search(
        &self,
        search: Search,
        max_items: Option<usize>,
        context: bool,
        self_href: Option<String>,
    ) -> impl Future<Output = stac_io::Result<StreamedSearch>> + Send {
        let pool = self.clone();
        async move {
            let mut search = serde_json::to_value(&search)?;
            // A continuation token rides in the search body; take it out for the keyset portal.
            let token = search
                .as_object_mut()
                .and_then(|map| map.remove("token"))
                .and_then(|value| value.as_str().map(str::to_string));

            // Older databases lack the streaming layout; fall back to the SQL `search()` function.
            if !pool.has_streaming_layout().await.map_err(backend)? {
                return sql_search_fallback(&pool, search, token, context).await;
            }

            // Honor max_items (total cap), else the search body's `limit`, else stream all.
            let cap = max_items
                .map(|max| max as i64)
                .or_else(|| search.get("limit").and_then(serde_json::Value::as_i64));
            let sort_fields = keyset::sort_key_fields(&search);
            let is_prev = token.as_deref().is_some_and(|t| t.starts_with("prev:"));
            let had_token = token.is_some();

            // pgstac requests the count via conf.context — the STAC `context` flag translated here, not by
            // the caller — on its own pooled connection, concurrent with the item stream.
            let count_handle = if context {
                let pool = pool.clone();
                let mut count_search = search.clone();
                if let serde_json::Value::Object(map) = &mut count_search {
                    let conf = map.entry("conf").or_insert_with(|| serde_json::json!({}));
                    if let serde_json::Value::Object(conf_map) = conf {
                        let _ = conf_map.insert("context".into(), serde_json::json!("on"));
                    }
                }
                Some(tokio::spawn(async move {
                    pool.search_matched(&count_search).await
                }))
            } else {
                None
            };

            // Fetch one past the cap so `has_more` is exact (fetch-N+1 peek); the extra row is never yielded.
            let has_more = Arc::new(AtomicBool::new(false));
            let has_more_stream = has_more.clone();
            let portal = pool.search_items(search, token, cap.map(|c| c + 1));
            let items: ItemStream = Box::pin(async_stream::stream! {
                futures::pin_mut!(portal);
                let mut yielded: i64 = 0;
                while let Some(item) = portal.next().await {
                    match item {
                        Ok(value) => {
                            if cap.is_some_and(|c| yielded >= c) {
                                has_more_stream.store(true, Ordering::Relaxed);
                                break;
                            }
                            yielded += 1;
                            yield Ok(value);
                        }
                        Err(error) => {
                            yield Err(backend(error));
                            break;
                        }
                    }
                }
            });

            // next when rows remain past the cap; prev when the request carried a token (swapped for a
            // backward prev: token).
            let finalize: Finalize = Box::new(move |first, last, _count| {
                Box::pin(async move {
                    let more = has_more.load(Ordering::Relaxed);
                    let next_present = if is_prev { had_token } else { more };
                    let prev_present = if is_prev { more } else { had_token };
                    let mut links = Vec::new();
                    if let Some(last) = last.filter(|_| next_present) {
                        let token = format!("next:{}", keyset::mint_token(&last, &sort_fields));
                        links.push(page_link(self_href.as_deref(), "next", &token));
                    }
                    if let Some(first) = first.filter(|_| prev_present) {
                        let token = format!("prev:{}", keyset::mint_token(&first, &sort_fields));
                        links.push(page_link(self_href.as_deref(), "prev", &token));
                    }
                    let number_matched = match count_handle {
                        Some(handle) => handle
                            .await?
                            .map_err(backend)?
                            .map(|matched| matched as u64),
                        None => None,
                    };
                    let mut collection = ItemCollection::new(Vec::new())?;
                    collection.links = links;
                    collection.number_matched = number_matched;
                    Ok(collection)
                })
            });

            Ok(StreamedSearch { items, finalize })
        }
    }
}
