#![deny(rustdoc::broken_intra_doc_links)]
//! Rust interface for [pgstac](https://github.com/stac-utils/pgstac).
//!
//! # Examples
//!
//! [Client] wraps a connection and talks to a **pgstac** database, caching the database's hydration
//! invariants across calls:
//!
//! ```no_run
//! use pgstac::Client;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (connection, conn) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move {
//!     if let Err(e) = conn.await {
//!      eprintln!("connection error: {}", e);
//!     }
//! });
//! let client = Client::new(connection);
//! println!("{}", client.pgstac_version().await.unwrap());
//! # })
//! ```
//!
//! Reads and writes both go through the same `Client` via the [`stac::api`] client traits:
//!
//! ```no_run
//! use pgstac::Client;
//! use stac::Collection;
//! use stac::api::{ItemsClient, TransactionClient};
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (connection, conn) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move {
//!     if let Err(e) = conn.await {
//!      eprintln!("connection error: {}", e);
//!     }
//! });
//! let mut client = Client::new(connection);
//! client.add_collection(Collection::new("an-id", "a description")).await.unwrap();
//! let items = client.search(Default::default()).await.unwrap();
//! # })
//! ```
//!
//! # Features
//!
//! - `pool`: a `deadpool`-backed connection pool (`PgstacPool`) with rustls TLS.
//! - `export`: the stac-geoparquet dump/export library.
//! - `cli`: the `pgstac` binary (implies `export` + `pool`).
//! - `python`: the Python extension module built with maturin (implies `pool` + `export`).

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

mod api;
mod db;
#[cfg(feature = "export")]
pub mod export;
mod json;
mod load;
#[cfg(feature = "python")]
mod python;
mod read;

// The modules above live in api/ db/ json/ load/ python/ read/ subdirectories; they are re-exported at
// the crate root so existing `pgstac::…` and internal `crate::…` paths are unchanged — public modules
// stay public, previously-private ones stay crate-internal.
pub use json::{canonical, geom, rawjson, temporal};
pub use load::{dehydrate, fragment, ingest};
#[cfg(feature = "export")]
pub use load::parquet_decode;
pub use read::{collections, feature, fields, hydrate, keyset, search, source};
pub(crate) use load::field_registry;
#[cfg(feature = "pool")]
pub(crate) use db::tls;

pub use db::client::Client;
pub use db::connect::{ConnectConfig, DEFAULT_APPLICATION_NAME, DEFAULT_SEARCH_PATH};
#[cfg(feature = "pool")]
pub use db::pool::{DEFAULT_POOL_SIZE, PgstacPool, PoolOptions, PoolerMode};
use stac::api::{ItemCollection, ItemsClient, Search};
use tokio_postgres::NoTls;

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

    /// [geozero::error::GeozeroError]
    #[error(transparent)]
    Geozero(#[from] geozero::error::GeozeroError),

    /// A malformed item could not be dehydrated.
    #[error("dehydrate error: {0}")]
    Dehydrate(String),

    /// [std::io::Error]
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// [deadpool_postgres::PoolError]
    #[cfg(feature = "pool")]
    #[error(transparent)]
    Pool(#[from] deadpool_postgres::PoolError),

    /// [rustls::Error]
    #[cfg(feature = "pool")]
    #[error(transparent)]
    Rustls(#[from] rustls::Error),

    /// A TLS configuration error.
    #[cfg(feature = "pool")]
    #[error("tls configuration error: {0}")]
    Tls(String),

    /// [deadpool_postgres::BuildError]
    #[cfg(feature = "pool")]
    #[error(transparent)]
    PoolBuild(#[from] deadpool_postgres::BuildError),

    /// [tokio_postgres::Error]
    #[error(transparent)]
    TokioPostgres(#[from] tokio_postgres::Error),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// A malformed search / keyset pagination token.
    #[error("invalid search token: {0}")]
    InvalidToken(String),

    /// An export/dump error.
    #[cfg(feature = "export")]
    #[error("export error: {0}")]
    Export(String),

    /// [object_store::Error]
    #[cfg(feature = "store")]
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    /// [url::ParseError]
    #[cfg(feature = "store")]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}

// `clap` and `stac-io` are used only by the `pgstac` binary (a separate crate target), so the library
// would otherwise flag them as unused dependencies.
#[cfg(feature = "cli")]
use clap as _;
#[cfg(feature = "cli")]
use stac_io as _;

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
    // Route through ConnectConfig so the pgstac startup search_path (and any PG* env gap-fill) applies,
    // the same as the pool and CLI paths.
    let config = ConnectConfig {
        dsn: Some(connection_string.to_string()),
        ..Default::default()
    };
    let (client, connection) = config.to_pg_config()?.connect(NoTls).await?;
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

    let client = Client::new(client);
    loop {
        tracing::info!("Fetching page");
        let page = client.search(search.clone()).await?;
        let next_token = page
            .next
            .as_ref()
            .and_then(|token_map| token_map.get("token"))
            .and_then(|token| token.as_str())
            .map(str::to_string);
        let has_next_token = next_token.is_some();
        if let Some(token) = next_token {
            let _ = search
                .additional_fields
                .insert("token".into(), token.into());
        }
        for item in page.items {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::Client as PgstacClient;
    use geojson::Geometry;
    use rstest::{fixture, rstest};
    use serde_json::{Map, json};
    use stac::api::{
        CollectionsClient, Fields, Filter, ItemsClient, Search, Sortby, TransactionClient,
    };
    use stac::{Collection, Item};
    use std::{
        ops::{Deref, DerefMut},
        sync::{LazyLock, atomic::AtomicU16},
    };
    use tokio::sync::Mutex;
    use tokio_postgres::{Client, Config, NoTls};
    use tokio_test as _;

    static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct TestClient {
        client: PgstacClient<Client>,
        config: Config,
        dbname: String,
    }

    pub(crate) fn config() -> Config {
        std::env::var("PGSTAC_RS_TEST_DB")
            .unwrap_or("postgresql://username:password@localhost:5439/postgis".to_string())
            .parse()
            .unwrap()
    }

    /// Name of the clean, empty pgstac install used as the clone template for each test.
    ///
    /// Tests assume a *fresh* pgstac install (e.g. zero collections), so they must clone from a
    /// known-clean template rather than from whatever database the maintenance connection
    /// ([`config`]) points at — in local dev that database is often populated. Build the template
    /// once with `scripts/pgstac-rs-test-db`, or override the name via `PGSTAC_RS_TEST_TEMPLATE`.
    fn template() -> String {
        std::env::var("PGSTAC_RS_TEST_TEMPLATE")
            .unwrap_or_else(|_| "pgstac_rs_test_template".to_string())
    }

    impl TestClient {
        async fn new(id: u16) -> TestClient {
            let dbname = format!("pgstac_test_{id}");
            let config = config();
            {
                let _mutex = MUTEX.lock().await;
                let (client, connection) = config.connect(NoTls).await.unwrap();
                let _handle = tokio::spawn(async move { connection.await.unwrap() });
                // Clone from the clean template (not the maintenance db, which may be populated).
                // Connecting via `config` (a different database) means the template has no active
                // session, so CREATE DATABASE ... TEMPLATE succeeds.
                let _ = client
                    .execute(
                        &format!("CREATE DATABASE {} TEMPLATE {}", dbname, template()),
                        &[],
                    )
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to create test db {dbname} from template {}: {e}. \
                             Build the template first with `scripts/pgstac-rs-test-db` \
                             (or set PGSTAC_RS_TEST_TEMPLATE).",
                            template()
                        )
                    });
                // `CREATE DATABASE ... TEMPLATE` copies the schema but NOT the template's per-database
                // settings (pg_db_role_setting), so the clone loses the template's
                // `search_path = pgstac, public`. Re-apply it, or the unqualified refs inside the pgstac
                // functions resolve against `public` only and every query fails.
                let _ = client
                    .execute(
                        &format!("ALTER DATABASE {dbname} SET search_path TO pgstac, public"),
                        &[],
                    )
                    .await
                    .unwrap();
            }
            let mut test_config = config.clone();
            let (client, connection) = test_config.dbname(&dbname).connect(NoTls).await.unwrap();
            let _handle = tokio::spawn(async move { connection.await.unwrap() });
            TestClient {
                client: PgstacClient::new(client),
                config,
                dbname,
            }
        }

        /// Seeds items through the **Rust loader** (the single write path) on a fresh connection —
        /// the test analogue of the removed SQL `create_item`/`upsert_item`. `policy` resolves id
        /// collisions (Error = "add", Upsert = "upsert/update").
        async fn load(
            &self,
            items: Vec<serde_json::Value>,
            policy: crate::ingest::ConflictPolicy,
        ) -> crate::Result<()> {
            let mut config = self.config.clone();
            let (mut client, connection) =
                config.dbname(&self.dbname).connect(NoTls).await.unwrap();
            let handle = tokio::spawn(connection);
            client
                .batch_execute("SET search_path TO pgstac, public")
                .await?;
            let schema = crate::dehydrate::DehydrateSchema::load(&client).await?;
            let _ = crate::ingest::load_items(&mut client, items, &schema, policy).await?;
            handle.abort();
            Ok(())
        }

        async fn add_item<T: serde::Serialize>(&self, item: T) -> crate::Result<()> {
            self.load(
                vec![serde_json::to_value(item)?],
                crate::ingest::ConflictPolicy::Error,
            )
            .await
        }

        async fn add_items<T: serde::Serialize>(&self, items: &[T]) -> crate::Result<()> {
            let values = items
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?;
            self.load(values, crate::ingest::ConflictPolicy::Error)
                .await
        }

        async fn upsert_item<T: serde::Serialize>(&self, item: T) -> crate::Result<()> {
            self.load(
                vec![serde_json::to_value(item)?],
                crate::ingest::ConflictPolicy::Upsert,
            )
            .await
        }

        async fn upsert_items<T: serde::Serialize>(&self, items: &[T]) -> crate::Result<()> {
            let values = items
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?;
            self.load(values, crate::ingest::ConflictPolicy::Upsert)
                .await
        }

        async fn update_item<T: serde::Serialize>(&self, item: T) -> crate::Result<()> {
            self.upsert_item(item).await
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
        type Target = PgstacClient<Client>;
        fn deref(&self) -> &Self::Target {
            &self.client
        }
    }

    impl DerefMut for TestClient {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.client
        }
    }

    fn longmont() -> Geometry {
        Geometry::new_point(vec![-105.1019, 40.1672])
    }

    /// Extracts the keyset token string from an `ItemCollection` `next`/`prev` pagination map.
    fn token(link: &Option<Map<String, serde_json::Value>>) -> Option<String> {
        link.as_ref()
            .and_then(|map| map.get("token"))
            .and_then(|value| value.as_str())
            .map(str::to_string)
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
    async fn collections(#[future(awt)] mut client: TestClient) {
        assert!(client.collections().await.unwrap().is_empty());
        client
            .add_collection(Collection::new("an-id", "a description"))
            .await
            .unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn add_collection_duplicate(#[future(awt)] mut client: TestClient) {
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
            client.collection("an-id").await.unwrap().unwrap().title,
            Some("a title".to_string())
        );
    }

    #[rstest]
    #[tokio::test]
    async fn update_collection(#[future(awt)] mut client: TestClient) {
        let mut collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(
            client
                .collection("an-id")
                .await
                .unwrap()
                .unwrap()
                .title
                .is_none()
        );
        collection.title = Some("a title".to_string());
        client.update_collection(collection).await.unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
        assert_eq!(
            client.collection("an-id").await.unwrap().unwrap().title,
            Some("a title".to_string())
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
    async fn delete_collection(#[future(awt)] mut client: TestClient) {
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
    async fn item(#[future(awt)] mut client: TestClient) {
        assert!(
            client
                .item("collection-id", "an-id")
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
        let got = client
            .item("collection-id", "an-id")
            .await
            .unwrap()
            .expect("item present");
        let got = serde_json::to_value(got).unwrap();
        // v0.10 re-renders the item from its columns rather than echoing the stored JSON: the
        // timestamptz `datetime` is microsecond precision (the input nanoseconds are truncated) and an
        // empty `assets` object is dropped, so assert the round-tripped identity + geometry rather than
        // byte-equality with the input.
        assert_eq!(got["id"], "an-id");
        assert_eq!(got["collection"], "collection-id");
        assert_eq!(got["type"], "Feature");
        assert_eq!(got["geometry"], serde_json::to_value(longmont()).unwrap());
        assert!(got["properties"].get("datetime").is_some());
        client.update_collection_extents().await.unwrap();
    }

    // The Rust loader (now the only write path) errors on an item whose collection does not exist,
    // rather than the legacy SQL `create_item` behavior of silently dropping it.
    #[rstest]
    #[tokio::test]
    async fn item_without_collection(#[future(awt)] client: TestClient) {
        let item = Item::new("an-id");
        assert!(client.add_item(item.clone()).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn update_item(#[future(awt)] mut client: TestClient) {
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
        let got = serde_json::to_value(
            client
                .item("collection-id", "an-id")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(got["properties"]["foo"], "bar");
    }

    #[rstest]
    #[tokio::test]
    async fn delete_item(#[future(awt)] mut client: TestClient) {
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
            client.item("collection-id", "an-id").await.unwrap(),
            None,
        );
    }

    #[rstest]
    #[tokio::test]
    async fn upsert_item(#[future(awt)] mut client: TestClient) {
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
    async fn add_items(#[future(awt)] mut client: TestClient) {
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
                .item("collection-id", "an-id")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            client
                .item("collection-id", "other-id")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[rstest]
    #[tokio::test]
    async fn upsert_items(#[future(awt)] mut client: TestClient) {
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
    async fn search_everything(#[future(awt)] mut client: TestClient) {
        assert!(
            client
                .search(Search::default())
                .await
                .unwrap()
                .items
                .is_empty()
        );
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        // See `item`: v0.10 re-renders from columns, so assert identity + geometry, not byte-equality.
        let page = client.search(Search::default()).await.unwrap();
        let got = serde_json::to_value(&page.items[0]).unwrap();
        assert_eq!(got["id"], "an-id");
        assert_eq!(got["collection"], "collection-id");
        assert_eq!(got["geometry"], serde_json::to_value(longmont()).unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn search_ids(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(client.search(search).await.unwrap().items.len(), 1);
        let search = Search {
            ids: vec!["not-an-id".to_string()],
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().items.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_collections(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(client.search(search).await.unwrap().items.len(), 1);
        let search = Search {
            collections: vec!["not-an-id".to_string()],
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().items.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_limit(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(page.items.len(), 1);
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
    async fn search_bbox(#[future(awt)] mut client: TestClient) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let mut search = Search::default();
        search.items.bbox = Some(vec![-106., 40., -105., 41.].try_into().unwrap());
        assert_eq!(
            client.search(search.clone()).await.unwrap().items.len(),
            1
        );
        search.items.bbox = Some(vec![-106., 41., -105., 42.].try_into().unwrap());
        assert!(client.search(search).await.unwrap().items.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_datetime(#[future(awt)] mut client: TestClient) {
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
            client.search(search.clone()).await.unwrap().items.len(),
            1
        );
        search.items.datetime = Some("2023-01-08T00:00:00Z".to_string());
        assert!(client.search(search).await.unwrap().items.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn search_intersects(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(client.search(search).await.unwrap().items.len(), 1);
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
        assert!(client.search(search).await.unwrap().items.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn pagination(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(serde_json::to_value(&page.items[0]).unwrap()["id"], "an-id");
        // Page with the real keyset token the server minted; the old "collection-id:an-id" offset-style
        // token format no longer exists in v0.10 (keyset_decode rejects it).
        let next = token(&page.next).expect("next token");
        let _ = search
            .additional_fields
            .insert("token".to_string(), next.into());
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(serde_json::to_value(&page.items[0]).unwrap()["id"], "another-id");
        let prev = token(&page.prev).expect("prev token");
        let _ = search
            .additional_fields
            .insert("token".to_string(), prev.into());
        let page = client.search(search).await.unwrap();
        assert_eq!(serde_json::to_value(&page.items[0]).unwrap()["id"], "an-id");
    }

    #[rstest]
    #[tokio::test]
    async fn base_url(#[future(awt)] mut client: TestClient) {
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
    async fn fields(#[future(awt)] mut client: TestClient) {
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
        let item = serde_json::to_value(&page.items[0]).unwrap();
        assert!(item["properties"].as_object().unwrap().get("foo").is_some());
        assert!(item["properties"].as_object().unwrap().get("bar").is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn sortby(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(serde_json::to_value(&page.items[0]).unwrap()["id"], "a");
        assert_eq!(serde_json::to_value(&page.items[1]).unwrap()["id"], "b");

        search.items.sortby = vec![Sortby::desc("id")];
        let page = client.search(search).await.unwrap();
        assert_eq!(serde_json::to_value(&page.items[0]).unwrap()["id"], "b");
        assert_eq!(serde_json::to_value(&page.items[1]).unwrap()["id"], "a");
    }

    #[rstest]
    #[tokio::test]
    async fn filter(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(page.items.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn query(#[future(awt)] mut client: TestClient) {
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
        assert_eq!(page.items.len(), 1);
    }
}
