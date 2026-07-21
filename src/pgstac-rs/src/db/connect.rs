//! Connection configuration for pgstac.
//!
//! A connection string (DSN) is the base configuration; the standard libpq environment variables
//! (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGOPTIONS`, `PGAPPNAME`,
//! `PGCONNECT_TIMEOUT`, `PGSSLMODE`) fill only the parameters the DSN leaves unset. Because the DSN is
//! the base, an explicit DSN always wins over ambient `PG*` values.
//!
//! Critically, it guarantees the pgstac `search_path` is applied at connection **startup** (via the
//! libpq `options` startup parameter) rather than a runtime `SET`. A runtime `SET search_path` does
//! not survive a transaction-pooling proxy such as PgBouncer, so setting it at startup is required for
//! correctness behind a pooler.

use crate::Result;
use std::time::Duration;
use tokio_postgres::Config;
use tokio_postgres::config::SslMode;

/// The default pgstac search path, applied at connection startup.
pub const DEFAULT_SEARCH_PATH: &str = "pgstac,public";

/// The default application name reported to Postgres.
pub const DEFAULT_APPLICATION_NAME: &str = "pgstac";

/// Resolved connection configuration for a pgstac database.
///
/// The [`dsn`](Self::dsn) (or an empty [`Config`] when there is none) is the base; the libpq `PG*`
/// environment fills only the parameters the DSN leaves unset, so a DSN always wins. Build one with
/// [`ConnectConfig::from_env`] (reads `PGSTAC_DSN` + the TLS cert paths) or [`ConnectConfig::default`],
/// set/override any fields, then turn it into a [`tokio_postgres::Config`] with
/// [`ConnectConfig::to_pg_config`].
///
/// The `tokio_postgres::Config`-native connection parameters (host, port, database, user, password,
/// options, application name, connect timeout, ssl mode) are not mirrored here; set them through the
/// DSN or the `PG*` environment. Only what `Config` cannot carry stays: the startup search path and the
/// TLS certificate paths.
///
/// # Examples
///
/// ```
/// use pgstac::ConnectConfig;
///
/// let config = ConnectConfig {
///     dsn: Some("postgresql://username:password@localhost:5432/postgis".to_string()),
///     ..Default::default()
/// };
/// let pg_config = config.to_pg_config().unwrap();
/// assert_eq!(pg_config.get_dbname(), Some("postgis"));
/// ```
#[derive(Debug, Clone)]
pub struct ConnectConfig {
    /// A full connection string (libpq keyword/value or `postgresql://` URL). It is the base
    /// configuration; the `PG*` environment fills only the parameters it leaves unset.
    pub dsn: Option<String>,
    /// Path to a trusted CA certificate (`PGSSLROOTCERT`); consumed by the TLS connector.
    pub sslrootcert: Option<String>,
    /// Path to a client certificate (`PGSSLCERT`); consumed by the TLS connector.
    pub sslcert: Option<String>,
    /// Path to a client private key (`PGSSLKEY`); consumed by the TLS connector.
    pub sslkey: Option<String>,
    /// The search path applied at connection startup. Defaults to [`DEFAULT_SEARCH_PATH`].
    pub search_path: String,
    /// When true, sets `pgstac.use_queue = on` at connection startup so pgstac defers index maintenance to
    /// the query queue (drained by `PgstacPool::run_queued`). Default false.
    pub use_queue: bool,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        ConnectConfig {
            dsn: None,
            sslrootcert: None,
            sslcert: None,
            sslkey: None,
            search_path: DEFAULT_SEARCH_PATH.to_string(),
            use_queue: false,
        }
    }
}

impl ConnectConfig {
    /// Resolves a configuration from the process environment: `PGSTAC_DSN` (or `DATABASE_URL`) for the
    /// DSN, and `PGSSLROOTCERT`/`PGSSLCERT`/`PGSSLKEY` for the TLS certificate paths. The remaining libpq
    /// `PG*` connection parameters are resolved at [`to_pg_config`](Self::to_pg_config) time, filling only
    /// what the DSN leaves unset.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgstac::ConnectConfig;
    ///
    /// let config = ConnectConfig::from_env();
    /// ```
    pub fn from_env() -> Self {
        Self::from_getter(|key| std::env::var(key).ok())
    }

    /// Resolves a configuration using a caller-supplied environment getter.
    ///
    /// This is the testable core of [`ConnectConfig::from_env`]; tests pass a closure over a fixed map
    /// instead of mutating the process environment.
    pub fn from_getter<F>(get: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        ConnectConfig {
            dsn: get("PGSTAC_DSN").or_else(|| get("DATABASE_URL")),
            sslrootcert: get("PGSSLROOTCERT"),
            sslcert: get("PGSSLCERT"),
            sslkey: get("PGSSLKEY"),
            search_path: DEFAULT_SEARCH_PATH.to_string(),
            use_queue: false,
        }
    }

    /// Builds a [`tokio_postgres::Config`], guaranteeing `search_path` is set at startup via `options`.
    ///
    /// The [`dsn`](Self::dsn) (or an empty [`Config`] when there is none) is the base; each libpq `PG*`
    /// connection variable fills its parameter only when the DSN left it unset, so the DSN wins. The
    /// search path is merged into the `options` startup string unless one is already present there.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgstac::ConnectConfig;
    ///
    /// let pg_config = ConnectConfig::default().to_pg_config().unwrap();
    /// // The pgstac search_path is applied at startup via the options parameter, so it survives
    /// // transaction-mode poolers like PgBouncer.
    /// assert!(pg_config.get_options().unwrap().contains("search_path=pgstac,public"));
    /// ```
    pub fn to_pg_config(&self) -> Result<Config> {
        self.to_pg_config_from(|key| std::env::var(key).ok())
    }

    /// The testable core of [`to_pg_config`](Self::to_pg_config): the same resolution against a
    /// caller-supplied environment getter instead of the process environment.
    fn to_pg_config_from<F>(&self, get: F) -> Result<Config>
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut config = match &self.dsn {
            Some(dsn) => dsn.parse::<Config>()?,
            None => Config::new(),
        };

        // Fill each connection parameter from the environment only when the DSN left it unset.
        if config.get_hosts().is_empty()
            && let Some(host) = get("PGHOST")
        {
            let _ = config.host(&host);
        }
        if config.get_ports().is_empty()
            && let Some(port) = get("PGPORT").and_then(|value| value.parse().ok())
        {
            let _ = config.port(port);
        }
        if config.get_dbname().is_none()
            && let Some(dbname) = get("PGDATABASE")
        {
            let _ = config.dbname(&dbname);
        }
        if config.get_user().is_none()
            && let Some(user) = get("PGUSER")
        {
            let _ = config.user(&user);
        }
        if config.get_password().is_none()
            && let Some(password) = get("PGPASSWORD")
        {
            let _ = config.password(&password);
        }
        if config.get_connect_timeout().is_none()
            && let Some(timeout) = get("PGCONNECT_TIMEOUT").and_then(|value| value.parse().ok())
        {
            let _ = config.connect_timeout(Duration::from_secs(timeout));
        }
        // Application name: the DSN value wins, then PGAPPNAME, then the pgstac default.
        if config.get_application_name().is_none() {
            let app = get("PGAPPNAME");
            let _ = config.application_name(app.as_deref().unwrap_or(DEFAULT_APPLICATION_NAME));
        }
        // SSL mode: PGSSLMODE applies only when the DSN did not set one (a parsed DSN defaults to
        // Prefer, so a Prefer here means "unset").
        if matches!(config.get_ssl_mode(), SslMode::Prefer)
            && let Some(sslmode) = get("PGSSLMODE")
        {
            let _ = config.ssl_mode(parse_ssl_mode(&sslmode));
        }

        // Merge the search path into the startup options (pgbouncer-safe), without duplicating one that
        // is already present (from the DSN or PGOPTIONS). DSN options win over PGOPTIONS.
        let base_options = config
            .get_options()
            .map(str::to_string)
            .or_else(|| get("PGOPTIONS"))
            .unwrap_or_default();
        let mut merged = merge_search_path(&base_options, &self.search_path);
        if self.use_queue && !merged.contains("use_queue") {
            merged.push_str(" -c pgstac.use_queue=on");
        }
        let _ = config.options(&merged);

        Ok(config)
    }
}

/// Maps a libpq `sslmode` string to a [`SslMode`].
///
/// `tokio_postgres` only distinguishes disable / prefer / require; the `verify-ca` and `verify-full`
/// modes map to `require` and rely on the TLS connector for certificate verification. Unknown values
/// fall back to `prefer` (the libpq default).
fn parse_ssl_mode(value: &str) -> SslMode {
    match value.to_ascii_lowercase().as_str() {
        "disable" => SslMode::Disable,
        "require" | "verify-ca" | "verify-full" => SslMode::Require,
        _ => SslMode::Prefer,
    }
}

/// Appends `-c search_path=<search_path>` to a libpq `options` string unless a `search_path` is
/// already set there.
fn merge_search_path(base_options: &str, search_path: &str) -> String {
    if base_options.contains("search_path") {
        return base_options.to_string();
    }
    let setting = format!("-c search_path={search_path}");
    if base_options.is_empty() {
        setting
    } else {
        format!("{base_options} {setting}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Builds a `get` closure over a fixed environment map.
    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn default_injects_search_path_at_startup() {
        let config = ConnectConfig::default()
            .to_pg_config_from(|_| None)
            .unwrap();
        let options = config.get_options().unwrap_or_default();
        assert!(
            options.contains("search_path=pgstac,public"),
            "options should carry the startup search_path, got {options:?}"
        );
        assert_eq!(config.get_application_name(), Some("pgstac"));
    }

    #[test]
    fn use_queue_sets_the_startup_guc() {
        let off = ConnectConfig::default()
            .to_pg_config_from(|_| None)
            .unwrap();
        assert!(!off.get_options().unwrap_or_default().contains("use_queue"));

        let on = ConnectConfig {
            use_queue: true,
            ..Default::default()
        }
        .to_pg_config_from(|_| None)
        .unwrap();
        let options = on.get_options().unwrap_or_default();
        assert!(
            options.contains("pgstac.use_queue=on"),
            "use_queue should set the startup GUC, got {options:?}"
        );
        // search_path is still applied alongside it.
        assert!(options.contains("search_path=pgstac,public"));
    }

    #[test]
    fn env_fills_unset_connection_params() {
        let map = env(&[
            ("PGHOST", "db.example.com"),
            ("PGPORT", "6432"),
            ("PGDATABASE", "stac"),
            ("PGUSER", "reader"),
            ("PGPASSWORD", "secret"),
            ("PGAPPNAME", "myapp"),
            ("PGCONNECT_TIMEOUT", "7"),
        ]);
        let pg = ConnectConfig::default()
            .to_pg_config_from(|key| map.get(key).cloned())
            .unwrap();
        assert_eq!(pg.get_ports(), &[6432]);
        assert_eq!(pg.get_dbname(), Some("stac"));
        assert_eq!(pg.get_user(), Some("reader"));
        assert_eq!(pg.get_password(), Some(&b"secret"[..]));
        assert_eq!(pg.get_application_name(), Some("myapp"));
        assert_eq!(pg.get_connect_timeout(), Some(&Duration::from_secs(7)));
        assert!(
            pg.get_options().unwrap_or_default().contains("search_path"),
            "search_path must be present even with env-var config"
        );
    }

    #[test]
    fn dsn_is_base_and_gets_search_path() {
        let cfg = ConnectConfig {
            dsn: Some("postgresql://u:p@host:5432/db".to_string()),
            ..Default::default()
        };
        let pg = cfg.to_pg_config_from(|_| None).unwrap();
        assert_eq!(pg.get_dbname(), Some("db"));
        assert_eq!(pg.get_user(), Some("u"));
        assert_eq!(pg.get_ports(), &[5432]);
        assert!(pg.get_options().unwrap_or_default().contains("search_path"));
    }

    #[test]
    fn dsn_wins_over_ambient_env() {
        // An explicit DSN must be authoritative: ambient `PGDATABASE`/`PGHOST`/`PGPORT` must not
        // override the target the DSN names. Regression for the CI failure where a `--dsn <clone>`
        // load connected to `PGDATABASE=postgis` (no pgstac) instead of the clone.
        let map = env(&[
            ("PGDATABASE", "postgis"),
            ("PGHOST", "envhost"),
            ("PGPORT", "1111"),
        ]);
        let cfg = ConnectConfig {
            dsn: Some("postgresql://u:p@dsnhost:5432/target".to_string()),
            ..Default::default()
        };
        let pg = cfg.to_pg_config_from(|key| map.get(key).cloned()).unwrap();
        assert_eq!(pg.get_dbname(), Some("target"));
        assert_eq!(pg.get_ports(), &[5432]);
    }

    #[test]
    fn pgoptions_preserved_and_search_path_appended() {
        let map = env(&[("PGOPTIONS", "-c statement_timeout=5000")]);
        let pg = ConnectConfig::default()
            .to_pg_config_from(|key| map.get(key).cloned())
            .unwrap();
        let options = pg.get_options().unwrap_or_default();
        assert!(
            options.contains("statement_timeout=5000"),
            "got {options:?}"
        );
        assert!(
            options.contains("search_path=pgstac,public"),
            "got {options:?}"
        );
    }

    #[test]
    fn existing_search_path_not_duplicated() {
        let merged = merge_search_path("-c search_path=custom", "pgstac,public");
        assert_eq!(merged, "-c search_path=custom");
        assert_eq!(merged.matches("search_path").count(), 1);
    }

    #[test]
    fn default_ssl_mode_is_prefer() {
        let pg = ConnectConfig::default()
            .to_pg_config_from(|_| None)
            .unwrap();
        assert!(matches!(pg.get_ssl_mode(), SslMode::Prefer));
    }

    #[test]
    fn ssl_mode_resolves_from_pgsslmode() {
        let disable = env(&[("PGSSLMODE", "disable")]);
        assert!(matches!(
            ConnectConfig::default()
                .to_pg_config_from(|key| disable.get(key).cloned())
                .unwrap()
                .get_ssl_mode(),
            SslMode::Disable
        ));

        let verify = env(&[("PGSSLMODE", "verify-full")]);
        assert!(matches!(
            ConnectConfig::default()
                .to_pg_config_from(|key| verify.get(key).cloned())
                .unwrap()
                .get_ssl_mode(),
            SslMode::Require
        ));
    }

    #[test]
    fn from_getter_reads_ssl_cert_paths() {
        let map = env(&[
            ("PGSSLROOTCERT", "/etc/ssl/root.crt"),
            ("PGSSLCERT", "/etc/ssl/client.crt"),
            ("PGSSLKEY", "/etc/ssl/client.key"),
        ]);
        let cfg = ConnectConfig::from_getter(|key| map.get(key).cloned());
        assert_eq!(cfg.sslrootcert.as_deref(), Some("/etc/ssl/root.crt"));
        assert_eq!(cfg.sslcert.as_deref(), Some("/etc/ssl/client.crt"));
        assert_eq!(cfg.sslkey.as_deref(), Some("/etc/ssl/client.key"));
    }
}
