//! Parity gate: Rust `canonical::jsonb_canonical_hash` must equal SQL `pgstac.jsonb_canonical_hash`
//! byte-for-byte, so an item dehydrated in Rust gets the same `item_hash` as one ingested through the SQL
//! path.
//!
//! Stands up its own throwaway database from `src/pgstac/pgstac.sql` (dropped on `Drop`), so
//! `jsonb_canonical_hash` is guaranteed present regardless of any ambient test database. Override the
//! maintenance connection base (no database) via `PGSTAC_RS_TEST_BASE`.

use pgstac::canonical;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio_postgres::{Client, NoTls};

/// The maintenance connection base (no database), used to create + drop the throwaway database.
fn base() -> String {
    std::env::var("PGSTAC_RS_TEST_BASE")
        .unwrap_or_else(|_| "postgresql://username:password@localhost:5439".to_string())
}

/// The assembled pgstac schema, loaded into the fresh database.
const PGSTAC_SQL: &str = include_str!("../../pgstac/pgstac.sql");

/// A throwaway database built from `pgstac.sql`, dropped on `Drop`.
struct FreshDb {
    name: String,
}

impl FreshDb {
    async fn create() -> FreshDb {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let name = format!(
            "pgstac_rs_canonical_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let (client, connection) = tokio_postgres::connect(&format!("{}/postgres", base()), NoTls)
            .await
            .unwrap();
        let handle = tokio::spawn(connection);
        // CREATE DATABASE cannot run inside a transaction, so issue each statement on its own.
        let _ = client
            .execute(&format!("CREATE DATABASE {name}"), &[])
            .await
            .unwrap();
        let _ = client
            .execute(
                &format!("ALTER DATABASE {name} SET search_path TO pgstac, public"),
                &[],
            )
            .await
            .unwrap();
        handle.abort();
        FreshDb { name }
    }

    fn dsn(&self) -> String {
        format!("{}/{}", base(), self.name)
    }
}

impl Drop for FreshDb {
    fn drop(&mut self) {
        let name = self.name.clone();
        std::thread::scope(|scope| {
            let _ = scope.spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let (client, connection) =
                        tokio_postgres::connect(&format!("{}/postgres", base()), NoTls)
                            .await
                            .unwrap();
                    let handle = tokio::spawn(connection);
                    let _ = client
                        .execute(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                            &[&name],
                        )
                        .await;
                    let _ = client
                        .execute(&format!("DROP DATABASE IF EXISTS {name}"), &[])
                        .await;
                    handle.abort();
                });
            });
        });
    }
}

/// Connects to the fresh database and loads the assembled `pgstac.sql` into it.
async fn connect_and_install(db: &FreshDb) -> Client {
    let (client, connection) = tokio_postgres::connect(&db.dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
    client.batch_execute(PGSTAC_SQL).await.unwrap();
    client
}

/// A battery of values exercising key ordering, nesting, unicode, escaping, and the number formatter
/// (fixed point, the scientific boundaries, integers, negative zero, large/small magnitudes).
fn cases() -> Vec<Value> {
    vec![
        json!({"b": 1, "a": 2, "Z": 3, "aa": 4, "A": 5}),
        json!([3, 1, 2, [4, 5], {"k": 6}]),
        json!("hello \"world\"\n\t/path\\x"),
        json!({"café": "naïve", "emoji": "🚀", "Δ": "δ"}),
        // Datetime normalization (UTC, fixed 6-digit microseconds) — same instant, many spellings.
        json!({"datetime": "2023-01-07T00:00:00Z"}),
        json!({"datetime": "2023-01-07T00:00:00.000000Z"}),
        json!({"datetime": "2023-01-07T12:34:56.789Z"}),
        json!({"datetime": "2023-01-07T12:34:56.789000Z"}),
        json!({"datetime": "2023-01-07T05:00:00+05:00"}),
        json!({"datetime": "2023-01-07T00:00:00-00:00"}),
        // Forgiving forms pgstac's to_tstz accepts (date-only, space, offset-less->UTC, lowercase, ±HHMM).
        json!({"datetime": "2023-01-07"}),
        json!({"datetime": "2023-01-07 00:00:00"}),
        json!({"datetime": "2023-01-07T00:00:00"}),
        json!({"datetime": "2023-01-07t00:00:00z"}),
        json!({"datetime": "2023-01-07T05:00:00+0500"}),
        json!({"start_datetime": "2019-12-31T19:00:00-05:00", "end_datetime": "2023-06-15T12:34:56.789Z"}),
        // Datetime-shaped-but-not-a-datetime strings that must stay raw on BOTH sides (equal either way).
        json!({"suffix": "2023-01-07T00:00:00Z-suffix", "bare": "2020", "compact": "20200101", "kw": "now"}),
        json!({"nested": {"y": [1, {"x": true, "w": null}], "datetime": "2023-01-07T00:00:00Z"}}),
        json!(42),
        json!(42.0),
        json!(-1),
        json!(0),
        json!(-0.0),
        json!(0.1),
        json!(0.5),
        json!(100.0),
        json!(1000000.0),
        json!(123.456),
        json!(-105.1019),
        json!(40.1672),
        json!(0.0001),
        json!(0.00001),
        json!(0.000001),
        json!(1e15),
        json!(1e16),
        json!(1e20),
        json!(1e21),
        json!(1.5e-10),
        json!(6.022e23),
        json!(123456789012345_i64),
        json!(9999999999999999_i64),
        json!(2.5),
        json!(98765.4321),
        json!(0.30000000000000004),
        json!({
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "item-1",
            "geometry": {"type": "Point", "coordinates": [-105.1019, 40.1672]},
            "bbox": [-105.1019, 40.1672, -105.1019, 40.1672],
            "properties": {"eo:cloud_cover": 12.5, "gsd": 30.0, "datetime": "2023-01-07T00:00:00Z"}
        }),
    ]
}

#[tokio::test]
async fn canonical_and_hash_match_sql() {
    let db = FreshDb::create().await;
    let client = connect_and_install(&db).await;
    for v in cases() {
        let sql_hash: Vec<u8> = client
            .query_one("SELECT pgstac.jsonb_canonical_hash($1::jsonb)", &[&v])
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            canonical::jsonb_canonical_hash(&v).unwrap().to_vec(),
            sql_hash,
            "jsonb_canonical_hash mismatch for {v}"
        );
    }
}
