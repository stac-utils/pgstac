//! Write methods on [`PgstacPool`]: the Rust-optimized ingest path.
//!
//! Every write — even a single item from a stac-fastapi transaction — runs the full loader: load the
//! per-collection promoted schema, dehydrate in Rust, then binary-COPY through the SECURITY DEFINER
//! staging/flush functions (direct writes to `items` are revoked from `pgstac_ingest`; see [`load_items`]).
//! There are no per-item SQL ingest functions and no thin pool wrappers: the single-item and bulk paths are
//! the same pipeline at different batch sizes. Collection writes go through the SQL
//! `create_collection`/`update_collection`/`delete_collection` functions (not subject to that restriction).

use crate::PgstacPool;
use crate::Result;
use crate::ingest::{ConflictPolicy, load_items};
use serde_json::Value;

impl PgstacPool {
    /// Loads a batch of full STAC items via the Rust dehydrate + binary-COPY pipeline, resolving id
    /// collisions by `policy`. Returns the number of rows flushed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pgstac::{ConnectConfig, PgstacPool};
    /// use pgstac::ingest::ConflictPolicy;
    /// use serde_json::json;
    ///
    /// # tokio_test::block_on(async {
    /// let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();
    /// let item = json!({"type": "Feature", "id": "a", "collection": "c1", /* ... */ });
    /// pool.create_items(vec![item], ConflictPolicy::Upsert).await.unwrap();
    /// # })
    /// ```
    pub async fn create_items(&self, items: Vec<Value>, policy: ConflictPolicy) -> Result<u64> {
        let schema = self.cached_dehydrate_schema().await?;
        let mut client = self.get().await?;
        load_items(&mut client, items, &schema, policy).await
    }

    /// EXPERIMENTAL precheck-driven upsert/ignore. Skips unchanged items on re-ingest (no transfer,
    /// dehydrate, or write — the sync win) and loads only new + changed via `create_items(policy)`.
    /// Per-partition, parallel, adaptive (skip empty / pull small partition / probe via temp-table + JOIN),
    /// with no per-item SQL-function arguments — see [`crate::ingest::precheck_upsert`] for the full
    /// classification + move semantics. Returns `(unchanged_skipped, loaded)`.
    pub async fn upsert_items_precheck(
        &self,
        items: Vec<Value>,
        policy: ConflictPolicy,
    ) -> Result<(u64, u64)> {
        crate::ingest::precheck_upsert(self, items, policy).await
    }

    /// Inserts a single item, erroring if its id already exists in the collection.
    pub async fn create_item(&self, item: Value) -> Result<u64> {
        self.create_items(vec![item], ConflictPolicy::Error).await
    }

    /// Upserts a single item, replacing it if its content changed.
    pub async fn upsert_item(&self, item: Value) -> Result<u64> {
        self.create_items(vec![item], ConflictPolicy::Upsert).await
    }

    /// Deletes an item by id from a collection.
    pub async fn delete_item(&self, collection_id: &str, item_id: &str) -> Result<()> {
        let client = self.get().await?;
        let _ = client
            .execute("SELECT delete_item($1, $2)", &[&item_id, &collection_id])
            .await?;
        Ok(())
    }

    /// Creates a collection from its STAC JSON (deriving `fragment_config` from `item_assets`).
    pub async fn create_collection(&self, collection: &Value) -> Result<()> {
        let client = self.get().await?;
        let _ = client
            .execute("SELECT create_collection($1::jsonb)", &[collection])
            .await?;
        Ok(())
    }

    /// Inserts or updates a collection (idempotent), setting `partition_trunc` on insert and preserving it
    /// (same value) on conflict. This is what makes `load`/`restore` re-runnable: a second pass updates the
    /// collection content instead of failing on the duplicate id, so an interrupted restore resumes cleanly.
    pub async fn upsert_collection(
        &self,
        collection: &Value,
        partition_trunc: Option<&str>,
    ) -> Result<()> {
        let client = self.get().await?;
        let _ = client
            .execute(
                "SELECT upsert_collection($1::jsonb, $2::text)",
                &[collection, &partition_trunc],
            )
            .await?;
        Ok(())
    }

    /// Runs the async partition-stats maintenance: recompute exact
    /// bounds + row counts for partitions ingest left dirty (oldest first). `limit` caps the batch
    /// (`None` = all dirty). Returns the number of partitions tightened. Safe + optional — a generous
    /// (un-tightened) envelope only over-includes a partition in search, never loses rows.
    pub async fn tighten_dirty_stats(&self, limit: Option<i32>) -> Result<i32> {
        let client = self.get().await?;
        let row = client
            .query_one("SELECT tighten_dirty_partition_stats($1::int)", &[&limit])
            .await?;
        Ok(row.get(0))
    }

    /// Sets a collection's `partition_trunc` (`Some("year")`/`Some("month")`, or `None` for a single
    /// partition). The collections trigger repartitions existing items; on a freshly-created (empty)
    /// collection it just records the setting, so set it before loading items (e.g. on restore, to
    /// recreate the dumped partition layout).
    pub async fn set_partition_trunc(
        &self,
        collection_id: &str,
        partition_trunc: Option<&str>,
    ) -> Result<()> {
        let client = self.get().await?;
        let _ = client
            .execute(
                "UPDATE collections SET partition_trunc = $2 WHERE id = $1",
                &[&collection_id, &partition_trunc],
            )
            .await?;
        Ok(())
    }

    /// Replaces a collection's content (preserving its operator-configured partitioning/fragmenting).
    pub async fn update_collection(&self, collection: &Value) -> Result<()> {
        let client = self.get().await?;
        let _ = client
            .execute("SELECT update_collection($1::jsonb)", &[collection])
            .await?;
        Ok(())
    }

    /// Deletes a collection (and its items) by id.
    pub async fn delete_collection(&self, collection_id: &str) -> Result<()> {
        let client = self.get().await?;
        let _ = client
            .execute("SELECT delete_collection($1)", &[&collection_id])
            .await?;
        Ok(())
    }
}

// The rustac `stac::api::TransactionClient` (+ `ItemsClient` / `CollectionsClient`) live on [`crate::Client`]:
// call `pool.client().await?` for the idiomatic per-request client (it shares the pool's hydration cache).
// The pool keeps the native `create_items` / `create_collection` / `upsert_collection` write methods above,
// which are what those traits delegate to.
