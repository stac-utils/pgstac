//! Connection configuration, TLS, the pooled client, and the connection pool.

pub(crate) mod client;
pub(crate) mod connect;
#[cfg(feature = "pool")]
pub(crate) mod pool;
#[cfg(feature = "pool")]
pub(crate) mod tls;
