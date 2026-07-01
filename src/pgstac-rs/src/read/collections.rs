//! Client-side collection search and lookups, driven by `collection_search_plan`.
//!
//! Collections are not split-stored, so there is no hydration and no datetime band engine: the plan
//! returns a query yielding `(content, keys)` rows. This walks that query, applies keyset pagination,
//! and mints tokens **in Rust** ([`crate::keyset::keyset_encode`]) from the per-row `keys`.

use crate::Result;
use crate::keyset::keyset_encode;
use crate::search::SearchPage;
use serde_json::{Value, json};
use tokio_postgres::GenericClient;

/// Runs one page of a collection search and returns the matching collections plus keyset tokens.
///
/// Mirrors the SQL `collection_search` paging contract (including the `prev` reversal) but does the
/// work client-side off `collection_search_plan`. The returned [`SearchPage`] holds the collections in
/// `features`.
pub async fn collection_search<C: GenericClient>(
    client: &C,
    search: &Value,
    token: Option<&str>,
) -> Result<SearchPage> {
    let is_prev = token.is_some_and(|t| t.starts_with("prev:"));
    // A non-empty keyset (the token minus its next:/prev: prefix) means this is not the first page.
    let has_keyset = token
        .map(|t| t.trim_start_matches("next:").trim_start_matches("prev:"))
        .is_some_and(|k| !k.is_empty());
    let limit = search
        .get("limit")
        .and_then(Value::as_i64)
        .filter(|&n| n > 0)
        .unwrap_or(10);

    let plan = client
        .query_one(
            "SELECT query, ctx_query FROM collection_search_plan($1::jsonb, $2)",
            &[search, &token],
        )
        .await?;
    let query: String = plan.try_get("query")?;
    let ctx_query: Option<String> = plan.try_get("ctx_query")?;

    let number_matched = match ctx_query {
        Some(ctx) if !ctx.is_empty() => {
            Some(client.query_one(&ctx, &[]).await?.try_get::<_, i64>(0)?)
        }
        _ => None,
    };

    // The plan query takes the page size (+1 to detect a further page) as a bound parameter and
    // returns (content jsonb, keys text[]). `LIMIT $1` infers $1 as bigint.
    let over_limit: i64 = limit + 1;
    let rows = client.query(&query, &[&over_limit]).await?;
    let has_more = rows.len() as i64 > limit;

    let mut features: Vec<Value> = Vec::new();
    let mut first_keys: Option<Vec<Option<String>>> = None;
    let mut last_keys: Option<Vec<Option<String>>> = None;
    for (i, row) in rows.iter().enumerate() {
        let keys: Vec<Option<String>> = row.try_get(1)?;
        if i == 0 {
            first_keys = Some(keys.clone());
        }
        if (i as i64) < limit {
            features.push(row.try_get(0)?);
            last_keys = Some(keys);
        }
    }

    let (fwd_first, fwd_last, next_present, prev_present) = if is_prev {
        features.reverse();
        (last_keys, first_keys, has_keyset, has_more)
    } else {
        (first_keys, last_keys, has_more, has_keyset)
    };

    let mut next_token = None;
    let mut prev_token = None;
    if !features.is_empty() {
        if let (true, Some(keys)) = (next_present, &fwd_last) {
            next_token = Some(format!("next:{}", keyset_encode(keys)));
        }
        if let (true, Some(keys)) = (prev_present, &fwd_first) {
            prev_token = Some(format!("prev:{}", keyset_encode(keys)));
        }
    }

    Ok(SearchPage {
        number_returned: features.len(),
        features,
        next_token,
        prev_token,
        number_matched,
    })
}

/// Fetches a single collection by id (wraps [`collection_search`] with an id filter).
pub async fn get_collection<C: GenericClient>(
    client: &C,
    collection_id: &str,
) -> Result<Option<Value>> {
    let body = json!({"ids": [collection_id], "limit": 1});
    let page = collection_search(client, &body, None).await?;
    Ok(page.features.into_iter().next())
}

/// Fetches the queryables document for a collection, or the catalog-wide queryables when
/// `collection_id` is `None`.
pub async fn get_queryables<C: GenericClient>(
    client: &C,
    collection_id: Option<&str>,
) -> Result<Value> {
    let row = match collection_id {
        Some(id) => {
            client
                .query_one("SELECT get_queryables($1::text) AS q", &[&id])
                .await?
        }
        None => {
            // `get_queryables()` with no args is ambiguous between the text[] and text overloads;
            // pin the text overload with an explicit NULL for the catalog-wide queryables.
            client
                .query_one("SELECT get_queryables(NULL::text) AS q", &[])
                .await?
        }
    };
    row.try_get("q").map_err(Into::into)
}
