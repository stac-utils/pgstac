//! The cached [`Client`] wrapper: the upstream `stac::api` traits (ItemsClient / CollectionsClient /
//! TransactionClient) backed by the Rust engine, caching the hydration invariants across calls.
//! Targets the rich fragment instance (default `pgstac_rs_test_rich`); skips if unreachable.

#![allow(unused_crate_dependencies)]

use pgstac::Client;
use stac::api::{CollectionsClient, ItemsClient, Search};
use tokio_postgres::NoTls;

async fn connect() -> Option<Client<tokio_postgres::Client>> {
    let dsn = std::env::var("PGSTAC_RS_TEST_RICH_DB").unwrap_or_else(|_| {
        "postgresql://username:password@localhost:5439/pgstac_rs_test_rich".into()
    });
    match tokio_postgres::connect(&dsn, NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(connection);
            client
                .batch_execute("SET search_path = pgstac, public;")
                .await
                .ok()?;
            Some(Client::new(client))
        }
        Err(e) => {
            eprintln!("[client] skipping: {e}");
            None
        }
    }
}

#[tokio::test]
async fn cached_client_implements_stac_traits() {
    let Some(client) = connect().await else {
        return;
    };

    // A (collection, id) sample, fetched via Deref to the wrapped connection.
    let row = client
        .query_one(
            "SELECT collection, id FROM items WHERE collection = 'landsat-c2-l2' LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    let (collection, id): (String, String) = (row.get("collection"), row.get("id"));

    // ItemsClient::item — the first read detects + caches the hydration invariants.
    let item = ItemsClient::item(&client, &collection, &id).await.unwrap();
    assert_eq!(item.map(|i| i.id), Some(id));

    // ItemsClient::search — reuses the cached hydration (no re-detection round trips).
    let mut search = Search::default();
    search.items.limit = Some(5);
    let items = ItemsClient::search(&client, search).await.unwrap();
    assert_eq!(items.items.len(), 5);

    // CollectionsClient, also on the engine.
    let collections = CollectionsClient::collections(&client).await.unwrap();
    assert!(!collections.is_empty());
    let one = CollectionsClient::collection(&client, &collection)
        .await
        .unwrap();
    assert_eq!(one.map(|c| c.id), Some(collection));
}
