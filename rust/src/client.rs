use crate::{Error, Pgstac};
use serde_json::Map;
use stac::api::{CollectionsClient, ItemCollection, ItemsClient, Search, TransactionClient};
use stac::{Collection, Item};
use std::ops::{Deref, DerefMut};
use tokio_postgres::GenericClient;

/// A newtype wrapper around a [`GenericClient`] that implements the STAC
/// client traits ([`ItemsClient`], [`CollectionsClient`], and
/// [`TransactionClient`]).
///
/// This wrapper allows any [`tokio_postgres`] client or transaction to be used
/// with the STAC client trait abstractions. Use [`Deref`] to access the
/// underlying [`Pgstac`] methods directly.
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
/// let client = Client(pg_client);
/// let item_collection = client.search(Default::default()).await.unwrap();
/// # })
/// ```
#[derive(Debug)]
pub struct Client<C>(pub C);

impl<C> Deref for Client<C> {
    type Target = C;

    fn deref(&self) -> &C {
        &self.0
    }
}

impl<C> DerefMut for Client<C> {
    fn deref_mut(&mut self) -> &mut C {
        &mut self.0
    }
}

impl<C: GenericClient + Send + Sync> ItemsClient for Client<C> {
    type Error = Error;

    async fn search(&self, search: Search) -> Result<ItemCollection, Error> {
        let page = Pgstac::search(&self.0, search).await?;
        let next_token = page.next_token();
        let prev_token = page.prev_token();
        let mut item_collection = ItemCollection::new(page.features)?;
        if let Some(next_token) = next_token {
            let mut next = Map::new();
            let _ = next.insert("token".into(), next_token.into());
            item_collection.next = Some(next);
        }
        if let Some(prev_token) = prev_token {
            let mut prev = Map::new();
            let _ = prev.insert("token".into(), prev_token.into());
            item_collection.prev = Some(prev);
        }
        item_collection.context = page.context;
        Ok(item_collection)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>, Error> {
        let value = Pgstac::item(&self.0, item_id, Some(collection_id)).await?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }
}

impl<C: GenericClient + Send + Sync> CollectionsClient for Client<C> {
    type Error = Error;

    async fn collections(&self) -> Result<Vec<Collection>, Error> {
        let values = Pgstac::collections(&self.0).await?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Error::from))
            .collect()
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>, Error> {
        let value = Pgstac::collection(&self.0, id).await?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }
}

impl<C: GenericClient + Send + Sync> TransactionClient for Client<C> {
    type Error = Error;

    async fn add_collection(&mut self, collection: Collection) -> Result<(), Error> {
        Pgstac::add_collection(&self.0, collection).await
    }

    async fn add_item(&mut self, item: Item) -> Result<(), Error> {
        Pgstac::add_item(&self.0, item).await
    }

    async fn add_items(&mut self, items: Vec<Item>) -> Result<(), Error> {
        Pgstac::add_items(&self.0, &items).await
    }
}
