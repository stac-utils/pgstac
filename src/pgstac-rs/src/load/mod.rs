//! The ingest/write path: dehydration, fragment splitting, the field registry, and the Rust loader.

pub mod dehydrate;
#[cfg(feature = "pool")]
pub(crate) mod extensions;
pub(crate) mod field_registry;
pub mod fragment;
pub mod ingest;
#[cfg(feature = "export")]
pub mod parquet_decode;
#[cfg(feature = "pool")]
pub(crate) mod pool_ingest;
#[cfg(feature = "pool")]
pub(crate) mod queryables;
