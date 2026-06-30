//! Search-driven export — **0.10 only**.
//!
//! Built on the Rust search path: a page comes from [`Pgstac::search`], which drives `search_plan`,
//! steps + hydrates the bands in Rust, and mints the keyset `next`/`prev` tokens client-side — the SQL
//! `search()` function is never called. This source produces a single **ItemCollection page**: a STAC
//! `FeatureCollection` assembled in Rust with its `next`/`prev` tokens, links, and
//! `context`/`numberReturned`/`numberMatched`.
//!
//! Bulk NDJSON + geoparquet streaming with flat memory are [`crate::PgstacPool::stream_ndjson`] and
//! [`crate::PgstacPool::stream_geoparquet`] (server-side row portals), not this source.
//!
//! A malformed token surfaces as an error (the SQL `keyset_decode` raises; [`SearchSource::page`] maps
//! it to [`crate::Error::Export`]).

use crate::page::Page;
use crate::{Error, Pgstac, Result};
use serde_json::Value;
use stac::api::Search;
use tokio_postgres::GenericClient;

/// Drives the v0.10 keyset search engine for export (0.10 only).
#[derive(Debug, Clone)]
pub struct SearchSource {
    search: Search,
}

impl SearchSource {
    /// Creates a source for the given search/CQL2 query.
    pub fn new(search: Search) -> Self {
        SearchSource { search }
    }

    /// Sets the keyset token (`next:<keyset>` / `prev:<keyset>`) for the page to
    /// fetch. A malformed token will error when the page is fetched.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        let _ = self
            .search
            .additional_fields
            .insert("token".into(), Value::String(token.into()));
        self
    }

    /// Fetches one keyset page via [`Pgstac::search`] (`search_plan` + Rust band-stepping); the
    /// features, tokens, and context are all derived in Rust.
    ///
    /// A malformed token raises in the SQL `keyset_decode`; that surfaces here as
    /// a database error, which is mapped to [`Error::Export`] with context so the
    /// caller sees a clear "bad token" failure (never silent, never offset-style).
    pub async fn page<C: GenericClient>(&self, client: &C) -> Result<Page> {
        match Pgstac::search(client, self.search.clone()).await {
            Ok(page) => Ok(page),
            Err(Error::TokioPostgres(e)) => {
                // Distinguish a token/keyset decode failure from other DB errors
                // so the caller gets a clear "bad token" message, but still
                // error (never an offset-style fallback). The keyset decode runs
                // inside `keyset_decode`, named in the DbError `where_` context.
                let in_keyset = e
                    .as_db_error()
                    .and_then(|db| db.where_())
                    .is_some_and(|w| w.contains("keyset_decode") || w.contains("keyset_where"));
                let msg = e.to_string();
                if in_keyset
                    || msg.contains("token")
                    || msg.contains("keyset")
                    || msg.contains("base64")
                {
                    Err(Error::Export(format!("invalid search token: {e}")))
                } else {
                    Err(Error::TokioPostgres(e))
                }
            }
            Err(other) => Err(other),
        }
    }

    /// Builds a single STAC `FeatureCollection` page with SQL-faithful tokens +
    /// context. The returned JSON mirrors `search()`: `features`, `links`
    /// (incl. next/prev), `numberReturned`, and `numberMatched` (when context is
    /// on). The page's tokens round-trip through the keyset engine.
    pub async fn item_collection_page<C: GenericClient>(&self, client: &C) -> Result<Value> {
        let page = self.page(client).await?;
        page_to_feature_collection(&page)
    }
}

/// Serializes a [`Page`] back into a STAC `FeatureCollection` JSON object,
/// preserving the SQL-minted tokens, links, context, and counts.
pub fn page_to_feature_collection(page: &Page) -> Result<Value> {
    let mut fc = serde_json::Map::new();
    let _ = fc.insert("type".into(), Value::String("FeatureCollection".into()));
    let features: Vec<Value> = page
        .features
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<_, _>>()?;
    let _ = fc.insert("features".into(), Value::Array(features));
    if !page.links.is_empty() {
        let _ = fc.insert("links".into(), serde_json::to_value(&page.links)?);
    }
    if let Some(n) = page.number_returned {
        let _ = fc.insert("numberReturned".into(), Value::from(n));
    }
    if let Some(ctx) = &page.context {
        // numberMatched comes from context.matched in the SQL output.
        let ctx_val = serde_json::to_value(ctx)?;
        if let Some(matched) = ctx_val.get("matched").cloned() {
            let _ = fc.insert("numberMatched".into(), matched);
        }
        let _ = fc.insert("context".into(), ctx_val);
    }
    // Surface next/prev tokens as top-level fields too (mirrors Page) so callers
    // that key off them do not have to re-parse links.
    if let Some(t) = page.next_token() {
        let _ = fc.insert("next".into(), Value::String(t));
    }
    if let Some(t) = page.prev_token() {
        let _ = fc.insert("prev".into(), Value::String(t));
    }
    // Preserve any extra top-level fields search() emitted (e.g. numberMatched
    // when context is on but no Context struct was deserialized).
    for (k, v) in &page.additional_fields {
        if !fc.contains_key(k) {
            let _ = fc.insert(k.clone(), v.clone());
        }
    }
    Ok(Value::Object(fc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use stac::api::Search;

    #[test]
    fn with_token_sets_field() {
        let src = SearchSource::new(Search::default()).with_token("next:abc");
        assert_eq!(
            src.search.additional_fields.get("token"),
            Some(&Value::String("next:abc".into()))
        );
    }
}
