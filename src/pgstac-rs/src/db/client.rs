use crate::dehydrate::DehydrateSchema;
use crate::ingest::{ConflictPolicy, load_items};
use crate::db::call;
use crate::read::collections;
use crate::search::{SearchPage, search_page_with};
use crate::source::CachedHydration;
use crate::Error;
use serde::Serialize;
use serde_json::Value;
use stac::api::{CollectionsClient, ItemCollection, ItemsClient, Search, TransactionClient};
use stac::{Collection, Item};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Bridges the concrete connection types the engine runs on to a `&tokio_postgres::Client`. Implemented for
/// a bare [`tokio_postgres::Client`] and (with the `pool` feature) a pooled `deadpool_postgres::Client`, so a
/// single [`Client<C>`] serves both direct and pooled connections.
pub trait PgConn {
    /// The underlying tokio-postgres client, for reads.
    fn pg(&self) -> &tokio_postgres::Client;
    /// The underlying tokio-postgres client, for the loader's `&mut` transaction.
    fn pg_mut(&mut self) -> &mut tokio_postgres::Client;
}

impl PgConn for tokio_postgres::Client {
    fn pg(&self) -> &tokio_postgres::Client {
        self
    }
    fn pg_mut(&mut self) -> &mut tokio_postgres::Client {
        self
    }
}

#[cfg(feature = "pool")]
impl PgConn for deadpool_postgres::Client {
    fn pg(&self) -> &tokio_postgres::Client {
        self
    }
    fn pg_mut(&mut self) -> &mut tokio_postgres::Client {
        self
    }
}

/// A wrapper around a connection (a bare [`tokio_postgres::Client`] or, with the `pool` feature, a pooled
/// `deadpool_postgres::Client`) that implements the STAC client traits ([`ItemsClient`],
/// [`CollectionsClient`], and [`TransactionClient`]) on the Rust pgstac engine.
///
/// `Client` caches the database's hydration invariants (storage model + promoted schema) on first use and
/// reuses them across every read on this connection, saving the catalog round trips a fresh detection would
/// cost per call. The pgstac-specific operations that are not STAC CRUD — version, settings, and collection
/// maintenance — are inherent methods; [`Deref`] reaches the underlying [`tokio_postgres::Client`] for raw
/// SQL.
///
/// # Examples
///
/// ```no_run
/// use pgstac::Client;
/// use stac::api::ItemsClient;
/// use tokio_postgres::NoTls;
///
/// # tokio_test::block_on(async {
/// let (pg_client, connection) = tokio_postgres::connect(
///     "postgresql://username:password@localhost:5432/postgis",
///     NoTls,
/// ).await.unwrap();
/// tokio::spawn(async move {
///     if let Err(e) = connection.await {
///         eprintln!("connection error: {}", e);
///     }
/// });
/// let client = Client::new(pg_client);
/// let item_collection = client.search(Default::default()).await.unwrap();
/// # })
/// ```
#[derive(Debug)]
pub struct Client<C> {
    client: C,
    hydration: Arc<OnceCell<CachedHydration>>,
}

impl<C> Client<C> {
    /// Wraps a connection, caching the hydration invariants across calls.
    pub fn new(client: C) -> Self {
        Client::with_cache(client, Arc::new(OnceCell::new()))
    }

    /// Wraps a connection with a hydration cache shared with other clients (every client a
    /// [`crate::PgstacPool`] hands out shares the pool's cache, so detection happens once pool-wide).
    pub(crate) fn with_cache(client: C, hydration: Arc<OnceCell<CachedHydration>>) -> Self {
        Client { client, hydration }
    }
}

impl<C: PgConn> Deref for Client<C> {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &tokio_postgres::Client {
        self.client.pg()
    }
}

impl<C: PgConn> DerefMut for Client<C> {
    fn deref_mut(&mut self) -> &mut tokio_postgres::Client {
        self.client.pg_mut()
    }
}

impl<C: PgConn + Send + Sync> Client<C> {
    /// The hydration invariants for this connection, detected once and cached.
    async fn cached_hydration(&self) -> Result<CachedHydration, Error> {
        self.hydration
            .get_or_try_init(|| CachedHydration::detect(self.client.pg()))
            .await
            .cloned()
    }

    /// Returns the **pgstac** version.
    pub async fn pgstac_version(&self) -> Result<String, Error> {
        call::string(self.client.pg(), "get_version", &[]).await
    }

    /// Returns whether the **pgstac** database is readonly.
    pub async fn readonly(&self) -> Result<bool, Error> {
        call::boolean(self.client.pg(), "readonly", &[]).await
    }

    /// Returns the value of the `context` **pgstac** setting (defaults to "off").
    pub async fn context(&self) -> Result<bool, Error> {
        call::string(self.client.pg(), "get_setting", &[&"context"])
            .await
            .map(|value| value == "on")
    }

    /// Sets the value of a **pgstac** setting.
    pub async fn set_pgstac_setting(&self, key: &str, value: &str) -> Result<(), Error> {
        let _ = self
            .client
            .pg()
            .execute(
                "INSERT INTO pgstac_settings (name, value) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT pgstac_settings_pkey DO UPDATE SET value = excluded.value;",
                &[&key, &value],
            )
            .await?;
        Ok(())
    }

    /// Adds or updates a collection.
    pub async fn upsert_collection<T: Serialize>(&self, collection: T) -> Result<(), Error> {
        let collection = serde_json::to_value(collection)?;
        call::void(self.client.pg(), "upsert_collection", &[&collection]).await
    }

    /// Updates a collection.
    pub async fn update_collection<T: Serialize>(&self, collection: T) -> Result<(), Error> {
        let collection = serde_json::to_value(collection)?;
        call::void(self.client.pg(), "update_collection", &[&collection]).await
    }

    /// Updates all collection extents.
    pub async fn update_collection_extents(&self) -> Result<(), Error> {
        call::void(self.client.pg(), "update_collection_extents", &[]).await
    }

    /// Deletes a collection.
    pub async fn delete_collection(&self, id: &str) -> Result<(), Error> {
        call::void(self.client.pg(), "delete_collection", &[&id]).await
    }

    /// Deletes an item.
    pub async fn delete_item(&self, id: &str, collection: Option<&str>) -> Result<(), Error> {
        call::void(self.client.pg(), "delete_item", &[&id, &collection]).await
    }
}

impl<C: PgConn + Send + Sync> ItemsClient for Client<C> {
    type Error = Error;

    async fn search(&self, search: Search) -> Result<ItemCollection, Error> {
        let hydration = self.cached_hydration().await?;
        let search = search.into_cql2_json()?;
        let body = serde_json::to_value(search)?;
        let token = body
            .get("token")
            .and_then(Value::as_str)
            .map(str::to_string);
        let limit = body.get("limit").and_then(Value::as_i64).unwrap_or(10);
        let page: SearchPage = match search_page_with(
            self.client.pg(),
            hydration.model,
            hydration.schema.as_deref(),
            &body,
            token.as_deref(),
            limit,
        )
        .await
        {
            Ok(page) => page,
            // A malformed token surfaces as a `keyset_decode` error in SQL; map it to a clear
            // InvalidToken rather than a raw DB error (and never an offset-style fallback).
            Err(Error::TokioPostgres(e)) => {
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
                    return Err(Error::InvalidToken(msg));
                }
                return Err(Error::TokioPostgres(e));
            }
            Err(other) => return Err(other),
        };
        ItemCollection::try_from(page)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>, Error> {
        let hydration = self.cached_hydration().await?;
        let body =
            serde_json::json!({"collections": [collection_id], "ids": [item_id], "limit": 1});
        let value = search_page_with(
            self.client.pg(),
            hydration.model,
            hydration.schema.as_deref(),
            &body,
            None,
            1,
        )
        .await?
        .features
        .into_iter()
        .next();
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }
}

impl<C: PgConn + Send + Sync> CollectionsClient for Client<C> {
    type Error = Error;

    async fn collections(&self) -> Result<Vec<Collection>, Error> {
        collections::all(self.client.pg())
            .await?
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Error::from))
            .collect()
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>, Error> {
        collections::get_collection(self.client.pg(), id)
            .await?
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }
}

/// Writes go through the **Rust loader**, not the SQL `create_item`/`create_items` functions:
/// `add_items` dehydrates, splits fragments, and binary-COPYs entirely in Rust (see [`load_items`]).
/// The loader needs a `&mut tokio_postgres::Client` to open its own transaction for the binary COPY; it
/// reaches it via `pg_mut()`, so this works for any `PgConn` — a direct or a pooled connection.
impl<C: PgConn + Send + Sync> TransactionClient for Client<C> {
    type Error = Error;

    /// Creates a collection via the SQL `create_collection` (which derives the `fragment_config` from
    /// `item_assets`). Collections are not subject to the item-write restriction and are not a bulk path.
    async fn add_collection(&mut self, collection: Collection) -> Result<(), Error> {
        let collection = serde_json::to_value(collection)?;
        call::void(self.client.pg(), "create_collection", &[&collection]).await
    }

    /// Creates a single item through the Rust loader, erroring if its id already exists.
    async fn add_item(&mut self, item: Item) -> Result<(), Error> {
        self.add_items(vec![item]).await
    }

    /// Creates items through the Rust loader (dehydrate + fragment split + binary COPY, all in Rust),
    /// erroring if any id already exists. Use [`crate::PgstacPool::create_items`] to choose an
    /// upsert/ignore [`ConflictPolicy`].
    async fn add_items(&mut self, items: Vec<Item>) -> Result<(), Error> {
        let values = items
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<Value>, _>>()?;
        let schema = DehydrateSchema::load(self.client.pg()).await?;
        let _ = load_items(self.client.pg_mut(), values, &schema, ConflictPolicy::Error).await?;
        Ok(())
    }
}
