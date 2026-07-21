//! The client-side read path: keyset search, flat-memory streaming, hydration, projection, and pages.

pub mod collections;
pub mod feature;
pub mod fields;
pub mod hydrate;
pub mod keyset;
pub(crate) mod page;
pub mod search;
pub mod source;
#[cfg(feature = "pool")]
pub(crate) mod stream;
