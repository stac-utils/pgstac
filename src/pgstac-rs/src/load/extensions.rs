//! Loading STAC extension JSON-schema documents into `pgstac.stac_extensions`.
//!
//! Ports pypgstac's `loadextensions` in two steps: (1) record every distinct `stac_extensions` URL any
//! collection references (dropping any `#fragment`) in `stac_extensions`, then (2) fetch the JSON schema at
//! every extension whose `content` is still NULL and store it. Unlike pypgstac (local files only), the
//! fetch handles both `http(s)://` URLs and local paths (`file://…` or a bare path). A URL that cannot be
//! fetched or parsed is logged (WARNING) and skipped — its `content` stays NULL and the rest still load.

use crate::Result;
use serde_json::Value;
use tokio_postgres::GenericClient;

/// Collects the distinct `stac_extensions` URLs referenced by collections, then fetches and stores the
/// JSON schema for every extension whose `content` is still NULL. Returns the number newly populated.
///
/// A single URL that cannot be fetched or parsed is logged (WARNING) and skipped, leaving its `content`
/// NULL; the load still succeeds. Database errors abort.
pub(crate) async fn load_extensions<C: GenericClient>(client: &C) -> Result<u64> {
    // Step 1: record every distinct extension URL any collection references, stripping any `#fragment`.
    let _ = client
        .execute(
            "INSERT INTO stac_extensions (url) \
             SELECT DISTINCT \
                 substring(jsonb_array_elements_text(content->'stac_extensions') FROM '^[^#]*') \
             FROM collections \
             ON CONFLICT DO NOTHING",
            &[],
        )
        .await?;

    // Step 2: fetch and store the schema for every extension not yet populated.
    let rows = client
        .query("SELECT url FROM stac_extensions WHERE content IS NULL", &[])
        .await?;

    let mut loaded = 0u64;
    for row in &rows {
        let url: String = row.get("url");
        match fetch_extension(&url).await {
            Ok(content) => {
                let _ = client
                    .execute(
                        "UPDATE stac_extensions SET content = $1 WHERE url = $2",
                        &[&content, &url],
                    )
                    .await?;
                loaded += 1;
            }
            // Skip one unreachable/invalid extension rather than aborting the whole load: warn (naming the
            // URL and the cause) and leave its `content` NULL so it can be retried later.
            Err(error) => tracing::warn!("skipping stac extension {url}: {error}"),
        }
    }
    Ok(loaded)
}

/// Fetches and JSON-parses the extension schema at `url`. Supports `http(s)://` (a network GET, with the
/// `store` feature), `file://…`, and bare local paths. The returned error is a human-readable string for
/// the skip-with-warning log.
async fn fetch_extension(url: &str) -> std::result::Result<Value, String> {
    if url.starts_with("http://") || url.starts_with("https://") {
        fetch_http(url).await
    } else {
        // A local path, given either as a `file://` URL or bare.
        let path = url.strip_prefix("file://").unwrap_or(url);
        let bytes = std::fs::read(path).map_err(|error| format!("read {path}: {error}"))?;
        serde_json::from_slice(&bytes).map_err(|error| format!("parse {path}: {error}"))
    }
}

/// Fetches an `http(s)://` schema over the network via `object_store` and returns the parsed JSON.
#[cfg(feature = "store")]
async fn fetch_http(url: &str) -> std::result::Result<Value, String> {
    use object_store::ObjectStore;

    let parsed = url::Url::parse(url).map_err(|error| format!("parse url {url}: {error}"))?;
    let (store, path) =
        object_store::parse_url(&parsed).map_err(|error| format!("resolve {url}: {error}"))?;
    let bytes = store
        .get(&path)
        .await
        .map_err(|error| format!("fetch {url}: {error}"))?
        .bytes()
        .await
        .map_err(|error| format!("read {url}: {error}"))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("parse {url}: {error}"))
}

/// Without the `store` feature there is no HTTP client, so `http(s)://` extensions are skipped.
#[cfg(not(feature = "store"))]
async fn fetch_http(url: &str) -> std::result::Result<Value, String> {
    Err(format!(
        "cannot fetch {url}: this build has no http support (enable the `store` feature)"
    ))
}
