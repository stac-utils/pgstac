//! Python extension module (built with maturin) exposing the pgstac **read + write** API over a
//! connection pool, for use in `stac-fastapi-pgstac` and similar.
//!
//! The pool is created once (`Pgstac.connect`) at application startup and shared. Every method is
//! `async` (a Python awaitable via `pyo3-async-runtimes`). Reads return JSON strings (the caller does
//! `orjson.loads`); writes go through the **Rust loader** (dehydration + fragment splitting + the
//! binary COPY all in Rust), driving the same engine as the rest of the crate.

#![cfg(feature = "python")]

use crate::ingest::ConflictPolicy;
use crate::{ConnectConfig, DEFAULT_SEARCH_PATH, PgstacPool};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use serde_json::{Value, json};

/// Serializes a value to a JSON string for the Python boundary.
fn encode<T: serde::Serialize>(value: &T) -> PyResult<String> {
    serde_json::to_string(value).map_err(pyerr)
}

fn pyerr<E: std::fmt::Display>(error: E) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}

fn parse_search(search: &str) -> PyResult<Value> {
    serde_json::from_str(search).map_err(pyerr)
}

/// Parse the write input into a list of item values: a JSON array, a
/// FeatureCollection (`features`), or a single item object.
fn parse_items(items: &str) -> PyResult<Vec<Value>> {
    match serde_json::from_str(items).map_err(pyerr)? {
        Value::Array(items) => Ok(items),
        Value::Object(mut map) => match map.remove("features") {
            Some(Value::Array(features)) => Ok(features),
            _ => Ok(vec![Value::Object(map)]),
        },
        other => Ok(vec![other]),
    }
}

/// Map the conflict-policy string ("upsert" default / "ignore" / "error").
fn parse_policy(policy: Option<&str>) -> PyResult<ConflictPolicy> {
    Ok(match policy.unwrap_or("upsert") {
        "upsert" => ConflictPolicy::Upsert,
        "ignore" => ConflictPolicy::Ignore,
        "error" => ConflictPolicy::Error,
        other => return Err(pyerr(format!("unknown conflict policy: {other}"))),
    })
}

/// Map the parquet compression string ("zstd" default / "snappy" / "uncompressed").
fn parse_compression(compression: &str) -> PyResult<crate::export::format::ParquetCompression> {
    use crate::export::format::ParquetCompression;
    Ok(match compression.to_ascii_lowercase().as_str() {
        "zstd" => ParquetCompression::Zstd,
        "snappy" => ParquetCompression::Snappy,
        "uncompressed" | "none" => ParquetCompression::Uncompressed,
        other => return Err(pyerr(format!("unknown compression: {other}"))),
    })
}

/// A pooled pgstac client exposing the read API to Python.
#[pyclass]
struct Pgstac {
    pool: PgstacPool,
}

#[pymethods]
impl Pgstac {
    /// Connects a pool. `dsn` defaults to the standard libpq environment (`PGHOST`, `PGDATABASE`, …).
    ///
    /// Returns an awaitable resolving to a `Pgstac`. Call once at startup and reuse.
    #[staticmethod]
    #[pyo3(signature = (dsn=None))]
    fn connect(py: Python<'_>, dsn: Option<String>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let config = match dsn {
                Some(dsn) => ConnectConfig {
                    dsn: Some(dsn),
                    search_path: DEFAULT_SEARCH_PATH.to_string(),
                    ..Default::default()
                },
                None => ConnectConfig::from_env(),
            };
            let pool = PgstacPool::connect(config).await.map_err(pyerr)?;
            Ok(Pgstac { pool })
        })
    }

    /// One page of an item search. Returns a FeatureCollection (with `next`/`prev` tokens,
    /// `numberReturned`, `numberMatched`) as JSON bytes.
    #[pyo3(signature = (search, token=None, limit=10))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        search: &str,
        token: Option<String>,
        limit: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let search = parse_search(search)?;
        // The search body's `limit` (the STAC contract) wins over the method default.
        let limit = search.get("limit").and_then(Value::as_i64).unwrap_or(limit);
        future_into_py(py, async move {
            let page = pool
                .search_page(&search, token.as_deref(), limit)
                .await
                .map_err(pyerr)?;
            encode(&json!({
                "type": "FeatureCollection",
                "features": page.features,
                "numberReturned": page.number_returned,
                "numberMatched": page.number_matched,
                "next": page.next_token,
                "prev": page.prev_token,
            }))
        })
    }

    /// Collects every matching item (up to `max_items`) into one FeatureCollection (JSON bytes),
    /// draining the flat-memory streaming iterator.
    #[pyo3(signature = (search, max_items=None))]
    fn search_collect<'py>(
        &self,
        py: Python<'py>,
        search: &str,
        max_items: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let search = parse_search(search)?;
        future_into_py(py, async move {
            let items = pool
                .search_collect_items(search, max_items)
                .await
                .map_err(pyerr)?;
            let number_returned = items.len();
            encode(&json!({
                "type": "FeatureCollection",
                "features": items,
                "numberReturned": number_returned,
            }))
        })
    }

    /// The total match count (`numberMatched`) for a search, run independently (use concurrently with
    /// `search` for the parallel-context pattern). `None` when context counting is off.
    fn search_matched<'py>(&self, py: Python<'py>, search: &str) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let search = parse_search(search)?;
        future_into_py(py, async move {
            pool.search_matched(&search).await.map_err(pyerr)
        })
    }

    /// Streams every matching item (up to `max_items`) into stac-geoparquet, returning the file bytes.
    ///
    /// `row_group_size` caps rows per parquet row-group (smaller = lower peak memory while encoding;
    /// `None` = the parquet default). `compression` is "zstd" (default), "snappy", or "uncompressed".
    #[pyo3(signature = (search, max_items=None, row_group_size=None, compression="zstd"))]
    fn search_to_geoparquet<'py>(
        &self,
        py: Python<'py>,
        search: &str,
        max_items: Option<i64>,
        row_group_size: Option<usize>,
        compression: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let search = parse_search(search)?;
        let compression = parse_compression(compression)?;
        future_into_py(py, async move {
            let mut buf: Vec<u8> = Vec::new();
            let _ = pool
                .stream_geoparquet(search, max_items, compression, row_group_size, &mut buf)
                .await
                .map_err(pyerr)?;
            Ok(buf)
        })
    }

    /// One page of a collection search. Returns the collections + tokens as JSON bytes.
    #[pyo3(signature = (search, token=None))]
    fn collection_search<'py>(
        &self,
        py: Python<'py>,
        search: &str,
        token: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let search = parse_search(search)?;
        future_into_py(py, async move {
            let page = pool
                .collection_search(&search, token.as_deref())
                .await
                .map_err(pyerr)?;
            encode(&json!({
                "collections": page.features,
                "numberReturned": page.number_returned,
                "numberMatched": page.number_matched,
                "next": page.next_token,
                "prev": page.prev_token,
            }))
        })
    }

    /// Fetches one item by id; JSON bytes, or `None` if it does not exist.
    fn get_item<'py>(
        &self,
        py: Python<'py>,
        collection_id: String,
        item_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let item = pool
                .get_item(&collection_id, &item_id)
                .await
                .map_err(pyerr)?;
            item.map(|v| encode(&v)).transpose()
        })
    }

    /// Fetches one collection by id; JSON bytes, or `None` if it does not exist.
    fn get_collection<'py>(
        &self,
        py: Python<'py>,
        collection_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let collection = pool.get_collection(&collection_id).await.map_err(pyerr)?;
            collection.map(|v| encode(&v)).transpose()
        })
    }

    /// The queryables document for a collection (or catalog-wide when `collection_id` is `None`).
    #[pyo3(signature = (collection_id=None))]
    fn get_queryables<'py>(
        &self,
        py: Python<'py>,
        collection_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let queryables = pool
                .get_queryables(collection_id.as_deref())
                .await
                .map_err(pyerr)?;
            encode(&queryables)
        })
    }

    /// Loads items through the Rust loader (dehydrate + fragment split + binary COPY). `items` is a
    /// JSON array, a FeatureCollection, or a single item. `policy` is "upsert" (default), "ignore", or
    /// "error". Returns the number of rows written. Use for both single-item (stac-fastapi POST) and
    /// bulk ingest — the same pipeline at different batch sizes.
    #[pyo3(signature = (items, policy=None))]
    fn create_items<'py>(
        &self,
        py: Python<'py>,
        items: &str,
        policy: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let items = parse_items(items)?;
        let policy = parse_policy(policy.as_deref())?;
        future_into_py(py, async move {
            pool.create_items(items, policy).await.map_err(pyerr)
        })
    }

    /// Creates a collection from its STAC JSON (deriving `fragment_config` from `item_assets`).
    fn create_collection<'py>(
        &self,
        py: Python<'py>,
        collection: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let collection = parse_search(collection)?;
        future_into_py(py, async move {
            pool.create_collection(&collection).await.map_err(pyerr)
        })
    }

    /// Replaces a collection's content (preserving its operator-configured partitioning/fragmenting).
    fn update_collection<'py>(
        &self,
        py: Python<'py>,
        collection: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let collection = parse_search(collection)?;
        future_into_py(py, async move {
            pool.update_collection(&collection).await.map_err(pyerr)
        })
    }

    /// Deletes a collection (and its items) by id.
    fn delete_collection<'py>(
        &self,
        py: Python<'py>,
        collection_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            pool.delete_collection(&collection_id).await.map_err(pyerr)
        })
    }

    /// Sets a collection's partition truncation: "year", "month", or `None` for a single partition.
    /// On a collection with items this repartitions; set it before loading to choose the layout.
    #[pyo3(signature = (collection_id, partition_trunc=None))]
    fn set_partition_trunc<'py>(
        &self,
        py: Python<'py>,
        collection_id: String,
        partition_trunc: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            pool.set_partition_trunc(&collection_id, partition_trunc.as_deref())
                .await
                .map_err(pyerr)
        })
    }

    /// Deletes an item by id from a collection.
    fn delete_item<'py>(
        &self,
        py: Python<'py>,
        collection_id: String,
        item_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            pool.delete_item(&collection_id, &item_id)
                .await
                .map_err(pyerr)
        })
    }

    /// Runs the async partition-stats maintenance: recompute exact
    /// bounds + row counts for partitions ingest left dirty. `limit` caps the batch (oldest first);
    /// `None` tightens all. Returns the number of partitions tightened. Schedule off-hours.
    #[pyo3(signature = (limit=None))]
    fn maintain<'py>(&self, py: Python<'py>, limit: Option<i32>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            pool.tighten_dirty_stats(limit).await.map_err(pyerr)
        })
    }
}

/// The `pypgstac_rs` extension module.
#[pymodule]
fn pypgstac_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Pgstac>()?;
    Ok(())
}
