//! Parity gate: Rust `canonical::jsonb_canonical_hash` must equal SQL `pgstac.jsonb_canonical_hash`
//! byte-for-byte, so an item dehydrated in Rust gets the same `item_hash` as one ingested through the SQL
//! path.
//!
//! Connects read-only to a pgstac database (`PGSTAC_RS_TEST_DB`, default the local dev `postgis` db).

use pgstac::canonical;
use serde_json::{Value, json};
use tokio_postgres::NoTls;

fn dsn() -> String {
    std::env::var("PGSTAC_RS_TEST_DB")
        .unwrap_or_else(|_| "postgresql://username:password@localhost:5439/postgis".to_string())
}

async fn connect() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&dsn(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET search_path TO pgstac, public;")
        .await
        .unwrap();
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
    let client = connect().await;
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
