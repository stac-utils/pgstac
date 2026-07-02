//! Low-level helpers for invoking pgstac SQL functions over any connection.

use crate::{Error, Result};
use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, Row};

/// Calls `pgstac.<function>(<params>)` and returns the single result row.
async fn row(
    client: &impl GenericClient,
    function: &str,
    params: &[&(dyn ToSql + Sync)],
) -> std::result::Result<Row, tokio_postgres::Error> {
    let param_string = (0..params.len())
        .map(|i| format!("${}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT * from pgstac.{function}({param_string})");
    client.query_one(&query, params).await
}

/// Calls a pgstac function returning a single `text` column named for the function.
pub(crate) async fn string(
    client: &impl GenericClient,
    function: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<String> {
    row(client, function, params)
        .await?
        .try_get(function)
        .map_err(Error::from)
}

/// Calls a pgstac function returning a single `boolean` column named for the function.
pub(crate) async fn boolean(
    client: &impl GenericClient,
    function: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<bool> {
    row(client, function, params)
        .await?
        .try_get(function)
        .map_err(Error::from)
}

/// Calls a pgstac function purely for its side effects, discarding the result.
pub(crate) async fn void(
    client: &impl GenericClient,
    function: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<()> {
    let _ = row(client, function, params).await?;
    Ok(())
}
