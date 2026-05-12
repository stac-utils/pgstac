//! Rust interface for [pgstac](https://github.com/stac-utils/pgstac).
//!
//! # Examples
//!
//! [Pgstac] is a trait to query a **pgstac** database.
//! It is implemented for anything that implements [tokio_postgres::GenericClient]:
//!
//! ```no_run
//! use pgstac::Pgstac;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (client, connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move {
//!     if let Err(e) = connection.await {
//!      eprintln!("connection error: {}", e);
//!     }
//! });
//! println!("{}", client.pgstac_version().await.unwrap());
//! # })
//! ```
//!
//! If you want to work in a transaction, you can do that too:
//!
//! ```no_run
//! use pgstac::Pgstac;
//! use stac::Collection;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (mut client, connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move {
//!     if let Err(e) = connection.await {
//!      eprintln!("connection error: {}", e);
//!     }
//! });
//! let transaction = client.transaction().await.unwrap();
//! transaction.add_collection(Collection::new("an-id", "a description")).await.unwrap();
//! transaction.commit().await.unwrap();
//! # })
//! ```
//!
//! # Features
//!
//! - `tls`: provide a function to create an unverified tls provider, which can be useful in some circumstances (see <https://github.com/stac-utils/rustac/issues/375>)

#![deny(
    elided_lifetimes_in_paths,
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    missing_abi,
    missing_debug_implementations,
    non_ascii_idents,
    noop_method_call,
    rust_2021_incompatible_closure_captures,
    rust_2021_incompatible_or_patterns,
    rust_2021_prefixes_incompatible_syntax,
    rust_2021_prelude_collisions,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]
#![warn(missing_docs)]

mod client;
mod page;

pub use client::Client;
pub use page::Page;
use serde::{Serialize, de::DeserializeOwned};
use stac::api::{ItemCollection, Search};
use tokio_postgres::{GenericClient, NoTls, Row, types::ToSql};

/// Crate-specific error enum.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [stac::Error]
    #[error(transparent)]
    Stac(#[from] stac::Error),

    /// [tokio_postgres::Error]
    #[error(transparent)]
    TokioPostgres(#[from] tokio_postgres::Error),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
}

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A [serde_json::Value].
pub type JsonValue = serde_json::Value;

/// Searches a pgstac database.
///
/// This function establishes a connection to the pgstac database, performs the search
/// with pagination support, and collects all results up to `max_items` if specified.
///
/// # Examples
///
/// ```no_run
/// # tokio_test::block_on(async {
/// let connection_string = "postgresql://username:password@localhost:5432/postgis";
/// let search = stac::api::Search::default();
/// let item_collection = pgstac::search(connection_string, search, None).await.unwrap();
/// # })
/// ```
pub async fn search(
    connection_string: &str,
    mut search: Search,
    max_items: Option<usize>,
) -> Result<ItemCollection> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
    let task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("pgstac connection error: {}", e);
        }
    });

    let mut all_items = if let Some(max_items) = max_items {
        if max_items == 0 {
            return Ok(ItemCollection::new(Vec::new())?);
        }
        Vec::with_capacity(max_items)
    } else {
        Vec::new()
    };

    if search.items.limit.is_none()
        && let Some(max_items) = max_items
    {
        search.items.limit = Some(max_items.try_into()?);
    }

    loop {
        tracing::info!("Fetching page");
        let page = client.search(search.clone()).await?;
        let next_token = page.next_token();
        let has_next_token = next_token.is_some();
        if let Some(token) = next_token {
            let _ = search
                .additional_fields
                .insert("token".into(), token.into());
        }
        for item in page.features {
            all_items.push(item);
            if let Some(max_items) = max_items
                && all_items.len() >= max_items
            {
                break;
            }
        }
        let should_continue = if let Some(max_items) = max_items {
            all_items.len() < max_items && has_next_token
        } else {
            has_next_token
        };
        if !should_continue {
            break;
        }
        tracing::debug!("Found {} item(s), continuing...", all_items.len());
    }

    drop(task);

    Ok(ItemCollection::new(all_items)?)
}

/// Methods for working with **pgstac**.
#[allow(async_fn_in_trait)]
pub trait Pgstac: GenericClient {
    /// Returns the **pgstac** version.
    async fn pgstac_version(&self) -> Result<String> {
        self.pgstac_string("get_version", &[]).await
    }

    /// Returns whether the **pgstac** database is readonly.
    async fn readonly(&self) -> Result<bool> {
        self.pgstac_bool("readonly", &[]).await
    }

    /// Returns the value of the `context` **pgstac** setting.
    ///
    /// This setting defaults to "off".  See [the **pgstac**
    /// docs](https://github.com/stac-utils/pgstac/blob/main/docs/src/pgstac.md#pgstac-settings)
    /// for more information on the settings and their meaning.
    async fn context(&self) -> Result<bool> {
        self.pgstac_string("get_setting", &[&"context"])
            .await
            .map(|value| value == "on")
    }

    /// Sets the value of a **pgstac** setting.
    async fn set_pgstac_setting(&self, key: &str, value: &str) -> Result<()> {
        self.execute(
            "INSERT INTO pgstac_settings (name, value) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT pgstac_settings_pkey DO UPDATE SET value = excluded.value;",
            &[&key, &value],
        ).await.map(|_| ()).map_err(Error::from)
    }

    /// Fetches all collections.
    async fn collections(&self) -> Result<Vec<JsonValue>> {
        self.pgstac_vec("all_collections", &[]).await
    }

    /// Fetches a collection by id.
    async fn collection(&self, id: &str) -> Result<Option<JsonValue>> {
        self.pgstac_opt("get_collection", &[&id]).await
    }

    /// Adds a collection.
    async fn add_collection<T>(&self, collection: T) -> Result<()>
    where
        T: Serialize,
    {
        let collection = serde_json::to_value(collection)?;
        self.pgstac_void("create_collection", &[&collection]).await
    }

    /// Adds or updates a collection.
    async fn upsert_collection<T>(&self, collection: T) -> Result<()>
    where
        T: Serialize,
    {
        let collection = serde_json::to_value(collection)?;
        self.pgstac_void("upsert_collection", &[&collection]).await
    }

    /// Updates all collection extents.
    async fn update_collection_extents(&self) -> Result<()> {
        self.pgstac_void("update_collection_extents", &[]).await
    }

    /// Updates a collection.
    async fn update_collection<T>(&self, collection: T) -> Result<()>
    where
        T: Serialize,
    {
        let collection = serde_json::to_value(collection)?;
        self.pgstac_void("update_collection", &[&collection]).await
    }

    /// Deletes a collection.
    async fn delete_collection(&self, id: &str) -> Result<()> {
        self.pgstac_void("delete_collection", &[&id]).await
    }

    /// Fetches an item.
    async fn item(&self, id: &str, collection: Option<&str>) -> Result<Option<JsonValue>> {
        self.pgstac_opt("get_item", &[&id, &collection]).await
    }

    /// Adds an item.
    async fn add_item<T>(&self, item: T) -> Result<()>
    where
        T: Serialize,
    {
        let item = serde_json::to_value(item)?;
        self.pgstac_void("create_item", &[&item]).await
    }

    /// Adds items.
    async fn add_items<T>(&self, items: &[T]) -> Result<()>
    where
        T: Serialize,
    {
        let items = serde_json::to_value(items)?;
        self.pgstac_void("create_items", &[&items]).await
    }

    /// Updates an item.
    async fn update_item<T>(&self, item: T) -> Result<()>
    where
        T: Serialize,
    {
        let item = serde_json::to_value(item)?;
        self.pgstac_void("update_item", &[&item]).await
    }

    /// Upserts an item.
    async fn upsert_item<T>(&self, item: T) -> Result<()>
    where
        T: Serialize,
    {
        let item = serde_json::to_value(item)?;
        self.pgstac_void("upsert_item", &[&item]).await
    }

    /// Upserts items.
    ///
    /// To avoid having to iterate the entire slice to serialize, these items
    /// must all be [serde_json::Value].
    async fn upsert_items<T>(&self, items: &[T]) -> Result<()>
    where
        T: Serialize,
    {
        let items = serde_json::to_value(items)?;
        self.pgstac_void("upsert_items", &[&items]).await
    }

    /// Deletes an item.
    async fn delete_item(&self, id: &str, collection: Option<&str>) -> Result<()> {
        self.pgstac_void("delete_item", &[&id, &collection]).await
    }

    /// Searches for items.
    async fn search(&self, search: Search) -> Result<Page> {
        let search = search.into_cql2_json()?;
        let search = serde_json::to_value(search)?;
        self.pgstac_value("search", &[&search]).await
    }

    /// Runs a pgstac function.
    async fn pgstac(
        &self,
        function: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> std::result::Result<Row, tokio_postgres::Error> {
        let param_string = (0..params.len())
            .map(|i| format!("${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("SELECT * from pgstac.{function}({param_string})");
        self.query_one(&query, params).await
    }

    /// Returns a string result from a pgstac function.
    async fn pgstac_string(
        &self,
        function: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<String> {
        let row = self.pgstac(function, params).await?;
        row.try_get(function).map_err(Error::from)
    }

    /// Returns a bool result from a pgstac function.
    async fn pgstac_bool(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<bool> {
        let row = self.pgstac(function, params).await?;
        row.try_get(function).map_err(Error::from)
    }

    /// Returns a vector from a pgstac function.
    async fn pgstac_vec<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        if let Some(value) = self.pgstac_opt(function, params).await? {
            Ok(value)
        } else {
            Ok(Vec::new())
        }
    }

    /// Returns an optional value from a pgstac function.
    async fn pgstac_opt<T>(
        &self,
        function: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let row = self.pgstac(function, params).await?;
        let option: Option<JsonValue> = row.try_get(function)?;
        let option = option.map(|v| serde_json::from_value(v)).transpose()?;
        Ok(option)
    }

    /// Returns a deserializable value from a pgstac function.
    async fn pgstac_value<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let row = self.pgstac(function, params).await?;
        let value = row.try_get(function)?;
        serde_json::from_value(value).map_err(Error::from)
    }

    /// Returns nothing from a pgstac function.
    async fn pgstac_void(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<()> {
        let _ = self.pgstac(function, params).await?;
        Ok(())
    }
}

impl<T> Pgstac for T where T: GenericClient {}

#[cfg(test)]
pub(crate) mod tests {
    use super::Pgstac;
    use geojson::Geometry;
    use rstest::{fixture, rstest};
    use serde_json::{Map, json};
    use stac::api::{Fields, Filter, Search, Sortby};
    use stac::{Collection, Item};
    use std::{
        ops::Deref,
        sync::{LazyLock, atomic::AtomicU16},
    };
    use tokio::sync::Mutex;
    use tokio_postgres::{Client, Config, NoTls};
    use tokio_test as _;

    static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct TestClient {
        client: Client,
        config: Config,
        dbname: String,
    }

    pub(crate) fn config() -> Config {
        std::env::var("PGSTAC_RS_TEST_DB")
            .unwrap_or("postgresql://username:password@localhost:5439/postgis".to_string())
            .parse()
            .unwrap()
    }

    impl TestClient {
        async fn new(id: u16) -> TestClient {
            let dbname = format!("pgstac_test_{id}");
            let config = config();
            {
                let _mutex = MUTEX.lock().await;
                let (client, connection) = config.connect(NoTls).await.unwrap();
                let _handle = tokio::spawn(async move { connection.await.unwrap() });
                let _ = client
                    .execute(
                        &format!(
                            "CREATE DATABASE {} TEMPLATE {}",
                            dbname,
                            config.get_dbname().unwrap()
                        ),
                        &[],
                    )
                    .await
                    .unwrap();
            }
            let mut test_config = config.clone();
            let (client, connection) = test_config.dbname(&dbname).connect(NoTls).await.unwrap();
            let _handle = tokio::spawn(async move { connection.await.unwrap() });
            TestClient {
                client,
                config,
                dbname,
            }
        }

        async fn terminate(&mut self) {
            let (client, connection) = self.config.connect(NoTls).await.unwrap();
            let _handle = tokio::spawn(async move { connection.await.unwrap() });
            let _ = client
                .execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                    &[&self.dbname],
                )
                .await
                .unwrap();
            let _ = client
                .execute(&format!("DROP DATABASE {}", self.dbname), &[])
                .await
                .unwrap();
        }
    }

    impl Drop for TestClient {
        fn drop(&mut self) {
            std::thread::scope(|s| {
                let _ = s.spawn(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    runtime.block_on(self.terminate());
                });
            });
        }
    }

    impl Deref for TestClient {
        type Target = Client;
        fn deref(&self) -> &Self::Target {
            &self.client
        }
    }

    fn longmont() -> Geometry {
        Geometry::new_point(vec![-105.1019, 40.1672])
    }

    #[fixture]
    fn id() -> u16 {
        static COUNTER: AtomicU16 = AtomicU16::new(0);
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    #[fixture]
    async fn client(id: u16) -> TestClient {
        TestClient::new(id).await
    }

    #[rstest]
    #[tokio::test]
    async fn pgstac_version(#[future(awt)] client: TestClient) {
        let _ = client.pgstac_version().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn readonly(#[future(awt)] client: TestClient) {
        assert!(!client.readonly().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn context(#[future(awt)] client: TestClient) {
        assert!(!client.context().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn set_context(#[future(awt)] client: TestClient) {
        client.set_pgstac_setting("context", "on").await.unwrap();
        assert!(client.context().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn collections(#[future(awt)] client: TestClient) {
        assert!(client.collections().await.unwrap().is_empty());
        client
            .add_collection(Collection::new("an-id", "a description"))
            .await
            .unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn add_collection_duplicate(#[future(awt)] client: TestClient) {
        assert!(client.collections().await.unwrap().is_empty());
        let collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client.add_collection(collection).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn upsert_collection(#[future(awt)] client: TestClient) {
        assert!(client.collections().await.unwrap().is_empty());
        let mut collection = Collection::new("an-id", "a description");
        client.upsert_collection(collection.clone()).await.unwrap();
        collection.title = Some("a title".to_string());
        client.upsert_collection(collection).await.unwrap();
        assert_eq!(
            client.collection("an-id").await.unwrap().unwrap()["title"],
            "a title"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn update_collection(#[future(awt)] client: TestClient) {
        let mut collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(
            client
                .collection("an-id")
                .await
                .unwrap()
                .unwrap()
                .get("title")
                .is_none()
        );
        collection.title = Some("a title".to_string());
        client.update_collection(collection).await.unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
        assert_eq!(
            client.collection("an-id").await.unwrap().unwrap()["title"],
            "a title"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn update_collection_does_not_exit(#[future(awt)] client: TestClient) {
        let collection = Collection::new("an-id", "a description");
        assert!(client.update_collection(collection).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn collection_not_found(#[future(awt)] client: TestClient) {
        assert!(client.collection("not-an-id").await.unwrap().is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn delete_collection(#[future(awt)] client: TestClient) {
        let collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client.collection("an-id").await.unwrap().is_some());
        client.delete_collection("an-id").await.unwrap();
        assert!(client.collection("an-id").await.unwrap().is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn delete_collection_does_not_exist(#[future(awt)] client: TestClient) {
        assert!(client.delete_collection("not-an-id").await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn item(#[future(awt)] client: TestClient) {
        assert!(
            client
                .item("an-id", Some("collection-id"))
                .await
                .unwrap()
                .is_none()
        );
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .additional_fields
            .insert("type".into(), "Feature".into());
        client.add_item(item.clone()).await.unwrap();
        assert_eq!(
            client
                .item("an-id", Some("collection-id"))
                .await
                .unwrap()
                .unwrap(),
            serde_json::to_value(item).unwrap(),
        );
        client.update_collection_extents().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn item_without_collection(#[future(awt)] client: TestClient) {
        let item = Item::new("an-id");
        assert!(client.add_item(item.clone()).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn update_item(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), "bar".into());
        client.update_item(item).await.unwrap();
        assert_eq!(
            client
                .item("an-id", Some("collection-id"))
                .await
                .unwrap()
                .unwrap()["properties"]["foo"],
            "bar"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn delete_item(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        client
            .delete_item(&item.id, Some("collection-id"))
            .await
            .unwrap();
        assert_eq!(
            client.item("an-id", Some("collection-id")).await.unwrap(),
            None,
        );
    }

    #[rstest]
    #[tokio::test]
    async fn upsert_item(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.upsert_item(item.clone()).await.unwrap();
        client.upsert_item(item).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn add_items(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let mut other_item = item.clone();
        other_item.id = "other-id".to_string();
        client.add_items(&[item, other_item]).await.unwrap();
        assert!(
            client
                .item("an-id", Some("collection-id"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            client
                .item("other-id", Some("collection-id"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn upsert_items(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let mut other_item = item.clone();
        other_item.id = "other-id".to_string();
        let items = vec![item, other_item];
        client.upsert_items(&items).await.unwrap();
        client.upsert_items(&items).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn search_everything(#[future(awt)] client: TestClient) {
        assert!(
            client
                .search(Search::default())
                .await
                .unwrap()
                .features
                .is_empty()
        );
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        assert_eq!(
            client.search(Search::default()).await.unwrap().features[0],
            *serde_json::to_value(item).unwrap().as_object().unwrap()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn search_ids(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let search = Search {
            ids: vec!["an-id".to_string()],
            ..Default::default()
        };
        assert_eq!(client.search(search).await.unwrap().features.len(), 1);
        let search = Search {
            ids: vec!["not-an-id".to_string()],
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_collections(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let search = Search {
            collections: vec!["collection-id".to_string()],
            ..Default::default()
        };
        assert_eq!(client.search(search).await.unwrap().features.len(), 1);
        let search = Search {
            collections: vec!["not-an-id".to_string()],
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_limit(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "another-id".to_string();
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.limit = Some(1);
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features.len(), 1);
        if let Some(context) = page.context {
            // v0.8
            assert_eq!(context.limit.unwrap(), 1);
        } else {
            // v0.9
            assert_eq!(page.number_returned.unwrap(), 1);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn search_bbox(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let mut search = Search::default();
        search.items.bbox = Some(vec![-106., 40., -105., 41.].try_into().unwrap());
        assert_eq!(
            client.search(search.clone()).await.unwrap().features.len(),
            1
        );
        search.items.bbox = Some(vec![-106., 41., -105., 42.].try_into().unwrap());
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_datetime(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        item.properties.datetime = Some("2023-01-07T00:00:00Z".parse().unwrap());
        client.add_item(item.clone()).await.unwrap();
        let mut search = Search::default();
        search.items.datetime = Some("2023-01-07T00:00:00Z".to_string());
        assert_eq!(
            client.search(search.clone()).await.unwrap().features.len(),
            1
        );
        search.items.datetime = Some("2023-01-08T00:00:00Z".to_string());
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_intersects(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let search = Search {
            intersects: Some(
                serde_json::from_value(
                    serde_json::to_value(Geometry::new_polygon(vec![vec![
                        vec![-106., 40.],
                        vec![-106., 41.],
                        vec![-105., 41.],
                        vec![-105., 40.],
                        vec![-106., 40.],
                    ]]))
                    .unwrap(),
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        assert_eq!(client.search(search).await.unwrap().features.len(), 1);
        let search = Search {
            intersects: Some(
                serde_json::from_value(
                    serde_json::to_value(Geometry::new_polygon(vec![vec![
                        vec![-104., 40.],
                        vec![-104., 41.],
                        vec![-103., 41.],
                        vec![-103., 40.],
                        vec![-104., 40.],
                    ]]))
                    .unwrap(),
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn pagination(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.properties.datetime = Some("2023-01-08T00:00:00Z".parse().unwrap());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "another-id".to_string();
        item.properties.datetime = Some("2023-01-07T00:00:00Z".parse().unwrap());
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.limit = Some(1);
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(page.features[0]["id"], "an-id");
        let _ = search
            .additional_fields
            .insert("token".to_string(), "next:collection-id:an-id".into());
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(page.features[0]["id"], "another-id");
        let _ = search
            .additional_fields
            .insert("token".to_string(), "prev:collection-id:another-id".into());
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features[0]["id"], "an-id");
    }

    #[rstest]
    #[tokio::test]
    async fn base_url(#[future(awt)] client: TestClient) {
        client
            .set_pgstac_setting("base_url", "http://pgstac.test")
            .await
            .unwrap();
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.properties.datetime = Some("2023-01-08T00:00:00Z".parse().unwrap());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "another-id".to_string();
        item.properties.datetime = Some("2023-01-07T00:00:00Z".parse().unwrap());
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.limit = Some(1);
        let page = client.search(search.clone()).await.unwrap();
        if client.pgstac_version().await.unwrap().starts_with("0.9") {
            let _ = page.links.iter().find(|link| link.rel == "next").unwrap();
        }
    }

    #[rstest]
    #[tokio::test]
    async fn fields(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 42.into());
        let _ = item
            .properties
            .additional_fields
            .insert("bar".into(), 43.into());
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.fields = Some(Fields {
            include: vec!["properties.foo".to_string()],
            exclude: vec!["properties.bar".to_string()],
        });
        let page = client.search(search).await.unwrap();
        let item = &page.features[0];
        assert!(item["properties"].as_object().unwrap().get("foo").is_some());
        assert!(item["properties"].as_object().unwrap().get("bar").is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn sortby(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("a");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "b".to_string();
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.sortby = vec![Sortby::asc("id")];
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(page.features[0]["id"], "a");
        assert_eq!(page.features[1]["id"], "b");

        search.items.sortby = vec![Sortby::desc("id")];
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features[0]["id"], "b");
        assert_eq!(page.features[1]["id"], "a");
    }

    #[rstest]
    #[tokio::test]
    async fn filter(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("a");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 42.into());
        client.add_item(item.clone()).await.unwrap();
        item.id = "b".to_string();
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 43.into());
        client.add_item(item).await.unwrap();
        let mut filter = Map::new();
        let _ = filter.insert("op".into(), "=".into());
        let _ = filter.insert("args".into(), json!([{"property": "foo"}, 42]));
        let mut search = Search::default();
        search.items.filter = Some(Filter::Cql2Json(filter));
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn query(#[future(awt)] client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("a");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 42.into());
        client.add_item(item.clone()).await.unwrap();
        item.id = "b".to_string();
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 43.into());
        client.add_item(item).await.unwrap();
        let mut query = Map::new();
        let _ = query.insert("foo".into(), json!({"eq": 42}));
        let mut search = Search::default();
        search.items.query = Some(query);
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features.len(), 1);
    }
}
