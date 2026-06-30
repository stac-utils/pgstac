//! A [`deadpool`](deadpool_postgres)-backed connection pool for pgstac.

use crate::source::CachedHydration;
use crate::tls::make_tls_connect;
use crate::{ConnectConfig, Pgstac, Result};
use deadpool_postgres::{Client, Manager, ManagerConfig, Pool, RecyclingMethod};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// The default maximum number of connections in a [`PgstacPool`].
pub const DEFAULT_POOL_SIZE: usize = 4;

/// How the pool talks to the database, to stay compatible with external connection poolers.
///
/// The difference matters only behind a pooler such as PgBouncer. In either mode the pgstac
/// `search_path` is applied at connection startup (see [`ConnectConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolerMode {
    /// Use the extended query protocol with server-side prepared statements (fastest). Safe for a
    /// direct connection or a **session**-mode pooler. The default.
    #[default]
    Session,
    /// Use the simple query protocol with no named prepared statements, so the connection is safe to
    /// reuse across a **transaction**-mode pooler. Slightly slower; argument values are inlined.
    Transaction,
}

/// Options controlling how a [`PgstacPool`] is built.
#[derive(Debug, Clone)]
pub struct PoolOptions {
    /// The maximum number of connections in the pool. Defaults to [`DEFAULT_POOL_SIZE`].
    pub max_size: usize,
    /// The protocol mode used for queries. Defaults to [`PoolerMode::Session`].
    pub pooler_mode: PoolerMode,
}

impl Default for PoolOptions {
    fn default() -> Self {
        PoolOptions {
            max_size: DEFAULT_POOL_SIZE,
            pooler_mode: PoolerMode::default(),
        }
    }
}

/// A pool of connections to a pgstac database.
///
/// Every connection applies the pgstac `search_path` at startup (see [`ConnectConfig`]), so the pool
/// is safe to use behind a transaction-mode connection pooler such as PgBouncer.
///
/// # Examples
///
/// ```no_run
/// use pgstac::{ConnectConfig, PgstacPool};
///
/// # tokio_test::block_on(async {
/// let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();
/// let version = pool.version().await.unwrap();
/// # })
/// ```
#[derive(Debug, Clone)]
pub struct PgstacPool {
    pool: Pool,
    pooler_mode: PoolerMode,
    /// Lazily-loaded, shared-across-clones hydration invariants (model + promoted schema).
    hydration: Arc<OnceCell<CachedHydration>>,
    /// Lazily-loaded, shared-across-clones dehydrate schema (the promoted-column registry) for the
    /// write path — cached so repeated ingests don't re-query the catalog on every `create_items`.
    dehydrate_schema: Arc<OnceCell<Arc<crate::dehydrate::DehydrateSchema>>>,
}

impl PgstacPool {
    /// Builds a pool from a [`ConnectConfig`] with default [`PoolOptions`].
    ///
    /// A connection is acquired during construction to fail fast if the database is unreachable.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pgstac::{ConnectConfig, PgstacPool};
    ///
    /// # tokio_test::block_on(async {
    /// let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();
    /// # })
    /// ```
    pub async fn connect(config: ConnectConfig) -> Result<PgstacPool> {
        PgstacPool::connect_with(config, PoolOptions::default()).await
    }

    /// Builds a pool from a [`ConnectConfig`] and explicit [`PoolOptions`].
    ///
    /// A connection is acquired during construction to fail fast if the database is unreachable.
    pub async fn connect_with(config: ConnectConfig, options: PoolOptions) -> Result<PgstacPool> {
        let tls = make_tls_connect(&config)?;
        let pg_config = config.to_pg_config()?;
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config, tls, manager_config);
        let pool = Pool::builder(manager).max_size(options.max_size).build()?;
        let pgstac_pool = PgstacPool {
            pool,
            pooler_mode: options.pooler_mode,
            hydration: Arc::new(OnceCell::new()),
            dehydrate_schema: Arc::new(OnceCell::new()),
        };
        // Acquire (and immediately release) a connection so a bad DSN or unreachable database fails
        // here, at startup, rather than on the first request.
        let _client = pgstac_pool.get().await?;
        Ok(pgstac_pool)
    }

    /// The cached hydration invariants (storage model + promoted schema), loaded once on first use and
    /// shared across every search on this pool (and its clones).
    pub(crate) async fn cached_hydration(&self) -> Result<CachedHydration> {
        let cached = self
            .hydration
            .get_or_try_init(|| async {
                let client = self.get().await?;
                CachedHydration::detect(&**client).await
            })
            .await?;
        Ok(cached.clone())
    }

    /// The cached dehydrate schema (promoted-column registry) for the write path, loaded once on the
    /// first ingest and reused across `create_items` calls + pool clones — so repeated ingests (e.g.
    /// many small stac-fastapi POSTs) skip the per-call catalog round trip.
    pub(crate) async fn cached_dehydrate_schema(
        &self,
    ) -> Result<Arc<crate::dehydrate::DehydrateSchema>> {
        let cached = self
            .dehydrate_schema
            .get_or_try_init(|| async {
                let client = self.get().await?;
                Ok::<_, crate::Error>(Arc::new(
                    crate::dehydrate::DehydrateSchema::load(&**client).await?,
                ))
            })
            .await?;
        Ok(cached.clone())
    }

    /// The [`PoolerMode`] this pool uses for queries.
    pub fn pooler_mode(&self) -> PoolerMode {
        self.pooler_mode
    }

    /// Acquires a connection from the pool.
    ///
    /// The returned client dereferences to a [`tokio_postgres::Client`] and implements [`Pgstac`].
    pub async fn get(&self) -> Result<Client> {
        self.pool.get().await.map_err(Into::into)
    }

    /// A [`crate::Client`] backed by one pooled connection, sharing the pool's cached hydration invariants
    /// (so the detection round trip happens once pool-wide, not per checked-out client). This is the
    /// idiomatic rustac write/read surface — call the STAC client traits ([`stac::api::ItemsClient`],
    /// [`stac::api::CollectionsClient`], [`stac::api::TransactionClient`]) on the returned `Client`.
    pub async fn client(&self) -> Result<crate::Client<Client>> {
        Ok(crate::Client::with_cache(
            self.get().await?,
            self.hydration.clone(),
        ))
    }

    /// Returns the pgstac version reported by the database.
    pub async fn version(&self) -> Result<String> {
        self.get().await?.pgstac_version().await
    }

    /// Runs one page of a search and returns the hydrated features plus the keyset pagination tokens.
    ///
    /// This drives the search client-side via [`search_page`](crate::search::search_page): it walks
    /// the keyset query, hydrates the rows in Rust, and mints tokens via SQL `keyset_encode` (so they
    /// are byte-identical to the SQL `search()` tokens). `token` is a `next:`/`prev:` continuation
    /// token (or `None` for the first page); `limit` is the page size.
    ///
    /// Currently uses the extended query protocol, so it is safe for a direct connection or a
    /// session-mode pooler. Transaction-mode-safe search (simple-protocol routing per
    /// [`PoolerMode::Transaction`]) is a follow-up.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pgstac::{ConnectConfig, PgstacPool};
    /// use serde_json::json;
    ///
    /// # tokio_test::block_on(async {
    /// let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();
    /// let page = pool.search_page(&json!({"limit": 10}), None, 10).await.unwrap();
    /// # })
    /// ```
    pub async fn search_page(
        &self,
        search: &Value,
        token: Option<&str>,
        limit: i64,
    ) -> Result<crate::search::SearchPage> {
        let hydration = self.cached_hydration().await?;
        let client = self.get().await?;
        crate::search::search_page_with(
            &**client,
            hydration.model,
            hydration.schema.as_deref(),
            search,
            token,
            limit,
        )
        .await
    }

    /// Returns the total match count (`numberMatched`) for a search, independent of fetching a page.
    ///
    /// This is the parallel-context primitive: a caller that needs the count can run this on one pooled
    /// connection **concurrently** with [`search_page`](Self::search_page) (with context disabled in the
    /// search body) on another, overlapping the count latency with the data fetch. Returns `None` when
    /// context counting is off for the search.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pgstac::{ConnectConfig, PgstacPool};
    /// use serde_json::json;
    ///
    /// # tokio_test::block_on(async {
    /// let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();
    /// let body = json!({"collections": ["landsat-c2-l2"]});
    /// // Fetch the page and the count concurrently.
    /// let (page, matched) = tokio::join!(
    ///     pool.search_page(&body, None, 10),
    ///     pool.search_matched(&body),
    /// );
    /// let (_page, _matched) = (page.unwrap(), matched.unwrap());
    /// # })
    /// ```
    pub async fn search_matched(&self, search: &Value) -> Result<Option<i64>> {
        let client = self.get().await?;
        let row = client
            .query_one(
                "SELECT context_count, ctx_query FROM search_plan($1::jsonb, NULL::text, NULL::int)",
                &[search],
            )
            .await?;
        if let Some(count) = row.try_get::<_, Option<i64>>("context_count")? {
            return Ok(Some(count));
        }
        let Some(ctx_query) = row.try_get::<_, Option<String>>("ctx_query")? else {
            return Ok(None);
        };
        let count_row = client.query_one(&ctx_query, &[]).await?;
        Ok(count_row.try_get::<_, Option<i64>>(0)?)
    }

    /// Collects every matching item (up to `max_items`) into one [`SearchPage`](crate::search::SearchPage),
    /// paginating internally over the keyset tokens.
    pub async fn search_collect(
        &self,
        search: &Value,
        max_items: Option<usize>,
    ) -> Result<crate::search::SearchPage> {
        let client = self.get().await?;
        crate::search::search_collect(&**client, search, max_items).await
    }

    /// Streams every matching item (up to `max_items`) as newline-delimited JSON to `write`.
    ///
    /// One connection is held for the whole stream and only one page is buffered at a time, so memory
    /// stays flat regardless of the result size. Returns the number of items written.
    pub async fn search_stream<W: std::io::Write>(
        &self,
        search: &Value,
        write: &mut W,
        max_items: Option<usize>,
    ) -> Result<usize> {
        let client = self.get().await?;
        crate::search::search_stream(&**client, search, write, max_items).await
    }

    /// Fetches a single item by id from a collection.
    pub async fn get_item(&self, collection_id: &str, item_id: &str) -> Result<Option<Value>> {
        let client = self.get().await?;
        crate::search::get_item(&**client, collection_id, item_id).await
    }

    /// Runs one page of a collection search, returning the matching collections + keyset tokens.
    pub async fn collection_search(
        &self,
        search: &Value,
        token: Option<&str>,
    ) -> Result<crate::search::SearchPage> {
        let client = self.get().await?;
        crate::collections::collection_search(&**client, search, token).await
    }

    /// Fetches a single collection by id.
    pub async fn get_collection(&self, collection_id: &str) -> Result<Option<Value>> {
        let client = self.get().await?;
        crate::collections::get_collection(&**client, collection_id).await
    }

    /// Fetches the queryables document for a collection (or catalog-wide when `collection_id` is None).
    pub async fn get_queryables(&self, collection_id: Option<&str>) -> Result<Value> {
        let client = self.get().await?;
        crate::collections::get_queryables(&**client, collection_id).await
    }

    /// Closes the pool, preventing it from handing out further connections.
    pub fn close(&self) {
        self.pool.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio_postgres::NoTls;

    const LOCAL_BASE: &str = "postgresql://username:password@localhost:5439";

    fn test_dsn() -> String {
        std::env::var("PGSTAC_RS_TEST_DB").unwrap_or_else(|_| format!("{LOCAL_BASE}/postgis"))
    }

    /// A disposable database cloned from the clean test template, dropped on `Drop`.
    ///
    /// Search calls need a clean pgstac install (the shared dev database can carry ambiguous
    /// multi-version functions), and they must not hold a connection to the template itself (that
    /// would block other tests from cloning it). Each instance clones an isolated database.
    struct CloneDb {
        name: String,
    }

    impl CloneDb {
        async fn create() -> CloneDb {
            static COUNTER: AtomicU32 = AtomicU32::new(0);
            let name = format!(
                "pgstac_rs_pool_test_{}_{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            );
            let template = std::env::var("PGSTAC_RS_TEST_TEMPLATE")
                .unwrap_or_else(|_| "pgstac_rs_test_template".to_string());
            let (client, connection) =
                tokio_postgres::connect(&format!("{LOCAL_BASE}/postgres"), NoTls)
                    .await
                    .unwrap();
            let handle = tokio::spawn(connection);
            let _ = client
                .execute(&format!("CREATE DATABASE {name} TEMPLATE {template}"), &[])
                .await
                .unwrap();
            handle.abort();
            CloneDb { name }
        }

        fn dsn(&self) -> String {
            format!("{LOCAL_BASE}/{}", self.name)
        }
    }

    impl Drop for CloneDb {
        fn drop(&mut self) {
            let name = self.name.clone();
            std::thread::scope(|scope| {
                let _ = scope.spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    runtime.block_on(async move {
                        let (client, connection) =
                            tokio_postgres::connect(&format!("{LOCAL_BASE}/postgres"), NoTls)
                                .await
                                .unwrap();
                        let handle = tokio::spawn(connection);
                        let _ = client
                            .execute(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                                 WHERE datname = $1",
                                &[&name],
                            )
                            .await;
                        let _ = client
                            .execute(&format!("DROP DATABASE IF EXISTS {name}"), &[])
                            .await;
                        handle.abort();
                    });
                });
            });
        }
    }

    async fn clone_pool(pooler_mode: PoolerMode) -> (CloneDb, PgstacPool) {
        let db = CloneDb::create().await;
        let config = ConnectConfig {
            dsn: Some(db.dsn()),
            ..Default::default()
        };
        let pool = PgstacPool::connect_with(
            config,
            PoolOptions {
                pooler_mode,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        (db, pool)
    }

    #[tokio::test]
    async fn search_page_through_pool() {
        // Exercises the pool -> search_page wiring end to end. The clone is an empty clean install,
        // so the page has no features and no continuation token; search-result fidelity on real data
        // is covered by tests/search_page.rs.
        let (_db, pool) = clone_pool(PoolerMode::Session).await;
        let page = pool
            .search_page(&json!({"limit": 5}), None, 5)
            .await
            .unwrap();
        assert_eq!(page.number_returned, 0);
        assert!(page.features.is_empty());
        assert!(page.next_token.is_none());
        pool.close();
    }

    #[test]
    fn default_pooler_mode_is_session() {
        assert_eq!(PoolOptions::default().pooler_mode, PoolerMode::Session);
    }

    #[tokio::test]
    async fn pool_reports_version_through_startup_search_path() {
        let config = ConnectConfig {
            dsn: Some(test_dsn()),
            ..Default::default()
        };
        let pool = PgstacPool::connect(config).await.unwrap();
        // `get_version()` lives in the pgstac schema, so a non-empty result also proves the startup
        // `search_path` reached the server.
        let version = pool.version().await.unwrap();
        assert!(!version.is_empty(), "expected a pgstac version, got empty");
    }

    #[tokio::test]
    async fn pool_connects_with_sslmode_disable() {
        // sslmode=disable skips the TLS handshake entirely; the connector is still built but unused.
        let config = ConnectConfig {
            dsn: Some(test_dsn()),
            sslmode: Some("disable".to_string()),
            ..Default::default()
        };
        let pool = PgstacPool::connect(config).await.unwrap();
        assert!(!pool.version().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pool_respects_max_size() {
        let config = ConnectConfig {
            dsn: Some(test_dsn()),
            ..Default::default()
        };
        let pool = PgstacPool::connect_with(
            config,
            PoolOptions {
                max_size: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(pool.pool.status().max_size, 2);
    }
}
