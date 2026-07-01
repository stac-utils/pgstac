//! The ingest/write path: dehydration, fragment splitting, the field registry, and the Rust loader.

pub mod dehydrate;
pub(crate) mod field_registry;
pub mod fragment;
pub mod ingest;
#[cfg(feature = "export")]
pub mod parquet_decode;
#[cfg(feature = "pool")]
pub(crate) mod pool_ingest;
