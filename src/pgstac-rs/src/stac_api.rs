//! Native [`stac::api`] client-trait impls for [`PgstacPool`], so a pgstac database is a first-class
//! rustac backend (search / collections / transaction). Every method routes through the same engine
//! the rest of the crate uses — the keyset search in [`crate::search`] and the Rust loader for writes —
//! so these traits are the rustac-native API surface over that engine, not a second implementation.

use crate::{Error, Pgstac, PgstacPool};
use futures::{Stream, StreamExt};
use stac::api::{
    CollectionsClient, ItemCollection, ItemsClient, Search, StreamItemsClient, TransactionClient,
};
use stac::{Collection, Item};

impl ItemsClient for PgstacPool {
    type Error = Error;

    async fn search(&self, search: Search) -> Result<ItemCollection, Error> {
        let client = self.get().await?;
        let page = Pgstac::search(&**client, search).await?;
        ItemCollection::try_from(page)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>, Error> {
        match self.get_item(collection_id, item_id).await? {
            Some(value) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }
}

impl CollectionsClient for PgstacPool {
    type Error = Error;

    async fn collections(&self) -> Result<Vec<Collection>, Error> {
        let client = self.get().await?;
        Pgstac::collections(&**client)
            .await?
            .into_iter()
            .map(|value| serde_json::from_value(value).map_err(Error::from))
            .collect()
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>, Error> {
        match self.get_collection(id).await? {
            Some(value) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }
}

impl TransactionClient for PgstacPool {
    type Error = Error;

    async fn add_collection(&mut self, collection: Collection) -> Result<(), Error> {
        let value = serde_json::to_value(collection)?;
        self.create_collection(&value).await
    }

    async fn add_item(&mut self, item: Item) -> Result<(), Error> {
        let value = serde_json::to_value(item)?;
        self.create_item(value).await.map(|_| ())
    }
}

impl StreamItemsClient for PgstacPool {
    type Error = Error;

    /// Streams every matching item across all keyset pages with flat memory, by mapping the crate's
    /// portal-based [`search_items`](PgstacPool::search_items) stream into `stac::api::Item`s. This is
    /// the rustac-native, constant-memory alternative to page-by-page pagination.
    async fn search_stream(
        &self,
        search: Search,
    ) -> Result<impl Stream<Item = Result<stac::api::Item, Error>> + Send, Error> {
        let search = serde_json::to_value(search)?;
        let items = self.search_items(search, None, None);
        Ok(items
            .map(|result| result.and_then(|value| serde_json::from_value(value).map_err(Error::from))))
    }
}
