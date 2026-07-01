# pgstac

[![docs.rs](https://img.shields.io/docsrs/pgstac?style=for-the-badge)](https://docs.rs/pgstac/latest/pgstac/)
[![Crates.io](https://img.shields.io/crates/v/pgstac?style=for-the-badge)](https://crates.io/crates/pgstac)

Rust interface for [pgstac](https://github.com/stac-utils/pgstac).

## Usage

In your `Cargo.toml`:

```toml
[dependencies]
pgstac = "*"
```

See the [documentation](https://docs.rs/pgstac) for more.

### Cargo features

| Feature   | What it adds |
| --------- | ------------ |
| *(default)* | The `Pgstac` trait over any `tokio_postgres::GenericClient`. |
| `pool`    | [`PgstacPool`] — a `deadpool` connection pool (rustls TLS, PgBouncer-safe) with the read API: `search_page`, the flat-memory streaming iterator `search_items` / `stream_ndjson`, `collection_search`, `get_item`/`get_collection`/`get_queryables`, and `search_matched`. |
| `export`  | The export/dump library: scan partitions and write fully-hydrated stac-geoparquet + a sha256'd manifest. |
| `cli`     | The `pgstac` binary (implies `export`): `pgstac dump` and `pgstac search`. |
| `python`  | A pyo3 extension module (`pypgstac_rs`) built with [maturin]. |

The read API drives `search_plan` / `collection_search_plan` and does the band-stepping,
hydration (EWKB→GeoJSON, fragment merge), and keyset token minting **in Rust** — page-equivalent
to SQL `search()` but with flat memory under streaming and the work moved off the database.

### Pooled read API

```rust,no_run
use pgstac::{ConnectConfig, PgstacPool};
use futures::StreamExt;
use serde_json::json;

# tokio_test::block_on(async {
let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();

// A bounded page (keyset tokens included).
let page = pool.search_page(&json!({"collections": ["landsat-c2-l2"]}), None, 10).await.unwrap();

// Or stream every match with flat memory (one row + the fragment cache at a time).
let mut items = Box::pin(pool.search_items(json!({"collections": ["landsat-c2-l2"]}), None, None));
while let Some(item) = items.next().await {
    let _item = item.unwrap();
}
# })
```

### CLI (`cli` feature)

```sh
# Dump an instance to fully-hydrated stac-geoparquet + manifest.
pgstac dump --dsn "$PGSTAC_DSN" --out ./dump            # or s3://…, foo.tar.zst, -

# Stream a search as NDJSON / a FeatureCollection page / geoparquet.
pgstac search --dsn "$PGSTAC_DSN" -c landsat-c2-l2 --datetime 2024-01-01/.. --format ndjson
```

### Python wheel (`python` feature)

```sh
maturin build -m src/pgstac-rs/Cargo.toml   # builds the pypgstac_rs wheel
```

```python
import asyncio, orjson, pypgstac_rs

async def main():
    pool = await pypgstac_rs.Pgstac.connect()  # libpq env, or pass a dsn
    fc = orjson.loads(await pool.search(orjson.dumps({"collections": ["landsat-c2-l2"], "limit": 10}).decode()))
    print(fc["numberReturned"])

asyncio.run(main())
```

## Testing

**pgstac** needs a blank **pgstac** database for testing, so is not part of the default workspace build.
To test, from the root of the **pgstac** repository:

```sh
scripts/server
```

Then, in another terminal:

```sh
cargo test --manifest-path src/pgstac-rs/Cargo.toml
```

Each test is run in its own transaction, which is rolled back after the test.

### Customizing the test database connection

By default, the tests will connect to the database at `postgresql://username:password@localhost:5439/postgis`.
If you need to customize the connection information for whatever reason, set your `PGSTAC_RS_TEST_DB` environment variable:

```shell
PGSTAC_RS_TEST_DB=postgresql://otherusername:otherpassword@otherhost:7822/otherdbname cargo test --manifest-path src/pgstac-rs/Cargo.toml
```

## Other info

This crate used to be part of the [rustac](https://github.com/stac-utils/rustac) monorepo, but was moved here in May 2026.

[maturin]: https://github.com/PyO3/maturin
