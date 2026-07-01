//! rustls TLS connector construction for the connection [pool](crate::PgstacPool).
//!
//! A single [`MakeRustlsConnect`] is always used; whether TLS is actually negotiated is governed by
//! the connection's `sslmode` (see [`ConnectConfig`](crate::ConnectConfig)). The trust store is the
//! bundled Mozilla root set plus any `PGSSLROOTCERT`, and client certificates are loaded from
//! `PGSSLCERT`/`PGSSLKEY` when both are present.

use crate::{ConnectConfig, Error, Result};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;
use tokio_postgres_rustls::MakeRustlsConnect;

/// Builds the rustls TLS connector for the given configuration.
pub(crate) fn make_tls_connect(config: &ConnectConfig) -> Result<MakeRustlsConnect> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = &config.sslrootcert {
        for cert in load_certs(path)? {
            roots.add(cert)?;
        }
    }

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots);

    let client_config = match (&config.sslcert, &config.sslkey) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certs(cert_path)?;
            let key = load_key(key_path)?;
            builder.with_client_auth_cert(certs, key)?
        }
        _ => builder.with_no_client_auth(),
    };

    Ok(MakeRustlsConnect::new(client_config))
}

/// Loads all PEM certificates from a file.
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let data = std::fs::read(path)?;
    rustls_pemfile::certs(&mut data.as_slice())
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(Error::from)
}

/// Loads the first PEM private key from a file.
fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let data = std::fs::read(path)?;
    rustls_pemfile::private_key(&mut data.as_slice())?
        .ok_or_else(|| Error::Tls(format!("no private key found in {path}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_default_connector() {
        // The default config (Mozilla roots, no client auth) must yield a valid rustls connector.
        let _connector = make_tls_connect(&ConnectConfig::default()).unwrap();
    }

    #[test]
    fn missing_root_cert_is_an_error() {
        let config = ConnectConfig {
            sslrootcert: Some("/nonexistent/path/to/root.crt".to_string()),
            ..Default::default()
        };
        assert!(make_tls_connect(&config).is_err());
    }
}
