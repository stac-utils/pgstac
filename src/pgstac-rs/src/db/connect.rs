//! Connection configuration for pgstac.
//!
//! Resolves connection parameters from (in precedence order) explicitly-set fields, a connection
//! string (DSN), and the standard libpq environment variables (`PGHOST`, `PGPORT`, `PGDATABASE`,
//! `PGUSER`, `PGPASSWORD`, `PGOPTIONS`, `PGAPPNAME`, `PGCONNECT_TIMEOUT`).
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
/// Build one with [`ConnectConfig::from_env`] (reads libpq + `PGSTAC_DSN` env vars) or
/// [`ConnectConfig::default`] (empty, local defaults), set/override any fields, then turn it into a
/// [`tokio_postgres::Config`] with [`ConnectConfig::to_pg_config`].
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
    /// A full connection string (libpq keyword/value or `postgresql://` URL). When set it is the base
    /// configuration; the individual fields below still override or augment it.
    pub dsn: Option<String>,
    /// Database host (`PGHOST`).
    pub host: Option<String>,
    /// Database port (`PGPORT`).
    pub port: Option<u16>,
    /// Database name (`PGDATABASE`).
    pub dbname: Option<String>,
    /// User (`PGUSER`).
    pub user: Option<String>,
    /// Password (`PGPASSWORD`).
    pub password: Option<String>,
    /// Extra libpq `options` startup string (`PGOPTIONS`); the search path is merged into it.
    pub options: Option<String>,
    /// Application name (`PGAPPNAME`); defaults to [`DEFAULT_APPLICATION_NAME`].
    pub application_name: Option<String>,
    /// Connection timeout in seconds (`PGCONNECT_TIMEOUT`).
    pub connect_timeout: Option<u64>,
    /// SSL mode (`PGSSLMODE`): `disable`, `allow`, `prefer` (the default), `require`, `verify-ca`, or
    /// `verify-full`.
    pub sslmode: Option<String>,
    /// Path to a trusted CA certificate (`PGSSLROOTCERT`); consumed by the TLS connector.
    pub sslrootcert: Option<String>,
    /// Path to a client certificate (`PGSSLCERT`); consumed by the TLS connector.
    pub sslcert: Option<String>,
    /// Path to a client private key (`PGSSLKEY`); consumed by the TLS connector.
    pub sslkey: Option<String>,
    /// The search path applied at connection startup. Defaults to [`DEFAULT_SEARCH_PATH`].
    pub search_path: String,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        ConnectConfig {
            dsn: None,
            host: None,
            port: None,
            dbname: None,
            user: None,
            password: None,
            options: None,
            application_name: None,
            connect_timeout: None,
            sslmode: None,
            sslrootcert: None,
            sslcert: None,
            sslkey: None,
            search_path: DEFAULT_SEARCH_PATH.to_string(),
        }
    }
}

impl ConnectConfig {
    /// Resolves a configuration from the process environment: `PGSTAC_DSN` (or `DATABASE_URL`) for the
    /// DSN, and the standard libpq `PG*` variables for the individual fields.
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
            host: get("PGHOST"),
            port: get("PGPORT").and_then(|value| value.parse().ok()),
            dbname: get("PGDATABASE"),
            user: get("PGUSER"),
            password: get("PGPASSWORD"),
            options: get("PGOPTIONS"),
            application_name: get("PGAPPNAME"),
            connect_timeout: get("PGCONNECT_TIMEOUT").and_then(|value| value.parse().ok()),
            sslmode: get("PGSSLMODE"),
            sslrootcert: get("PGSSLROOTCERT"),
            sslcert: get("PGSSLCERT"),
            sslkey: get("PGSSLKEY"),
            search_path: DEFAULT_SEARCH_PATH.to_string(),
        }
    }

    /// Builds a [`tokio_postgres::Config`], guaranteeing `search_path` is set at startup via `options`.
    ///
    /// If [`dsn`](Self::dsn) is set it is parsed as the base config; any explicitly-set field then
    /// overrides the corresponding DSN value. The search path is merged into the `options` startup
    /// string unless one is already present there.
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
        let mut config = match &self.dsn {
            Some(dsn) => dsn.parse::<Config>()?,
            None => Config::new(),
        };

        if let Some(host) = &self.host {
            let _ = config.host(host);
        }
        if let Some(port) = self.port {
            let _ = config.port(port);
        }
        if let Some(dbname) = &self.dbname {
            let _ = config.dbname(dbname);
        }
        if let Some(user) = &self.user {
            let _ = config.user(user);
        }
        if let Some(password) = &self.password {
            let _ = config.password(password);
        }
        if let Some(timeout) = self.connect_timeout {
            let _ = config.connect_timeout(Duration::from_secs(timeout));
        }
        let _ = config.application_name(
            self.application_name
                .as_deref()
                .unwrap_or(DEFAULT_APPLICATION_NAME),
        );
        if let Some(sslmode) = &self.sslmode {
            let _ = config.ssl_mode(parse_ssl_mode(sslmode));
        }

        // Merge the search path into the startup options (pgbouncer-safe), without duplicating one
        // that is already present (from the DSN or PGOPTIONS).
        let base_options = self
            .options
            .clone()
            .or_else(|| config.get_options().map(str::to_string))
            .unwrap_or_default();
        let merged = merge_search_path(&base_options, &self.search_path);
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

    #[test]
    fn default_injects_search_path_at_startup() {
        let config = ConnectConfig::default().to_pg_config().unwrap();
        let options = config.get_options().unwrap_or_default();
        assert!(
            options.contains("search_path=pgstac,public"),
            "options should carry the startup search_path, got {options:?}"
        );
        assert_eq!(config.get_application_name(), Some("pgstac"));
    }

    #[test]
    fn from_getter_reads_libpq_vars() {
        let map: HashMap<&str, &str> = HashMap::from([
            ("PGHOST", "db.example.com"),
            ("PGPORT", "6432"),
            ("PGDATABASE", "stac"),
            ("PGUSER", "reader"),
            ("PGPASSWORD", "secret"),
            ("PGAPPNAME", "myapp"),
            ("PGCONNECT_TIMEOUT", "7"),
        ]);
        let cfg = ConnectConfig::from_getter(|key| map.get(key).map(|value| value.to_string()));
        assert_eq!(cfg.host.as_deref(), Some("db.example.com"));
        assert_eq!(cfg.port, Some(6432));
        assert_eq!(cfg.dbname.as_deref(), Some("stac"));
        assert_eq!(cfg.user.as_deref(), Some("reader"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
        assert_eq!(cfg.application_name.as_deref(), Some("myapp"));
        assert_eq!(cfg.connect_timeout, Some(7));

        let pg = cfg.to_pg_config().unwrap();
        assert_eq!(pg.get_ports(), &[6432]);
        assert_eq!(pg.get_dbname(), Some("stac"));
        assert_eq!(pg.get_user(), Some("reader"));
        assert_eq!(pg.get_application_name(), Some("myapp"));
        assert!(
            pg.get_options().unwrap_or_default().contains("search_path"),
            "search_path must be present even with libpq-var config"
        );
    }

    #[test]
    fn dsn_is_base_and_gets_search_path() {
        let cfg = ConnectConfig {
            dsn: Some("postgresql://u:p@host:5432/db".to_string()),
            ..Default::default()
        };
        let pg = cfg.to_pg_config().unwrap();
        assert_eq!(pg.get_dbname(), Some("db"));
        assert_eq!(pg.get_user(), Some("u"));
        assert_eq!(pg.get_ports(), &[5432]);
        assert!(pg.get_options().unwrap_or_default().contains("search_path"));
    }

    #[test]
    fn explicit_field_overrides_dsn() {
        let cfg = ConnectConfig {
            dsn: Some("postgresql://u:p@host:5432/db".to_string()),
            dbname: Some("override".to_string()),
            ..Default::default()
        };
        let pg = cfg.to_pg_config().unwrap();
        assert_eq!(pg.get_dbname(), Some("override"));
    }

    #[test]
    fn pgoptions_preserved_and_search_path_appended() {
        let cfg = ConnectConfig {
            options: Some("-c statement_timeout=5000".to_string()),
            ..Default::default()
        };
        let pg = cfg.to_pg_config().unwrap();
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
        let pg = ConnectConfig::default().to_pg_config().unwrap();
        assert!(matches!(pg.get_ssl_mode(), SslMode::Prefer));
    }

    #[test]
    fn ssl_mode_resolves_from_pgsslmode() {
        let disable = ConnectConfig {
            sslmode: Some("disable".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            disable.to_pg_config().unwrap().get_ssl_mode(),
            SslMode::Disable
        ));

        let verify = ConnectConfig {
            sslmode: Some("verify-full".to_string()),
            ..Default::default()
        };
        assert!(matches!(
            verify.to_pg_config().unwrap().get_ssl_mode(),
            SslMode::Require
        ));
    }

    #[test]
    fn from_getter_reads_ssl_vars() {
        let map: HashMap<&str, &str> = HashMap::from([
            ("PGSSLMODE", "require"),
            ("PGSSLROOTCERT", "/etc/ssl/root.crt"),
            ("PGSSLCERT", "/etc/ssl/client.crt"),
            ("PGSSLKEY", "/etc/ssl/client.key"),
        ]);
        let cfg = ConnectConfig::from_getter(|key| map.get(key).map(|value| value.to_string()));
        assert_eq!(cfg.sslmode.as_deref(), Some("require"));
        assert_eq!(cfg.sslrootcert.as_deref(), Some("/etc/ssl/root.crt"));
        assert_eq!(cfg.sslcert.as_deref(), Some("/etc/ssl/client.crt"));
        assert_eq!(cfg.sslkey.as_deref(), Some("/etc/ssl/client.key"));
    }
}
