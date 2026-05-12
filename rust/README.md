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

## Testing

**pgstac** needs a blank **pgstac** database for testing, so is not part of the default workspace build.
To test, from the root of the **pgstac** repository:

```sh
scripts/server
```

Then, in another terminal:

```sh
cargo test --manifest-path rust/Cargo.toml
```

Each test is run in its own transaction, which is rolled back after the test.

### Customizing the test database connection

By default, the tests will connect to the database at `postgresql://username:password@localhost:5439/postgis`.
If you need to customize the connection information for whatever reason, set your `PGSTAC_RS_TEST_DB` environment variable:

```shell
PGSTAC_RS_TEST_DB=postgresql://otherusername:otherpassword@otherhost:7822/otherdbname cargo test --manifest-path rust/Cargo.toml
```

## Other info

This crate used to be part of the [rustac](https://github.com/stac-utils/rustac) monorepo, but was moved here in May 2026.
