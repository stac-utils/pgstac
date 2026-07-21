//! Round-trip gate for the binary-COPY loader: dehydrate an item, binary-COPY it into a `LIKE items`
//! staging table, and assert the stored row equals SQL `content_dehydrate` of the same item (every
//! column except `pgstac_updated_at`, which is a load-time `now()`). Validates the per-column binary
//! encoding — geometry EWKB, promoted typed columns, jsonb, `text[]` — against the database.
//!
//! Connects to a pgstac database (`PGSTAC_RS_TEST_DB`, default the dev `postgis` db). Uses a manual
//! `CREATE TEMP TABLE … (LIKE items)` rather than the SECURITY DEFINER `make_binary_staging`, so it runs on
//! any pgstac install (the SECURITY DEFINER flush + the revoked direct writes are covered by the SQL gate).

use pgstac::dehydrate::{DehydrateSchema, dehydrate};
use pgstac::ingest::copy_rows;
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

fn dsn() -> String {
    std::env::var("PGSTAC_RS_TEST_DB")
        .unwrap_or_else(|_| "postgresql://username:password@localhost:5439/postgis".to_string())
}

async fn connect() -> Client {
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

fn item() -> Value {
    json!({
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
        "id": "copy-item",
        "collection": "c1",
        "geometry": {"type": "Point", "coordinates": [-105.1019, 40.1672]},
        "bbox": [-105.1019, 40.1672, -105.1019, 40.1672],
        "properties": {
            "datetime": "2023-01-07T00:00:00Z",
            "platform": "landsat-8",
            "instruments": ["oli", "tirs"],
            "eo:cloud_cover": 12.5,
            "gsd": 30.0,
            "sat:relative_orbit": 42,
            "created": "2023-01-08T01:02:03Z",
            "proj:bbox": [1.0, 2.0, 3.0, 4.0],
            "custom:keep": "stays"
        },
        "assets": {"thumbnail": {"href": "https://x/t.png", "type": "image/png", "roles": ["thumbnail"]}},
        "links": [{"rel": "self", "href": "https://x/copy-item"}],
        "extra_top": {"a": 1}
    })
}

#[tokio::test]
async fn copy_round_trips_against_content_dehydrate() {
    let mut client = connect().await;
    let schema = DehydrateSchema::load(&client).await.unwrap();

    // copy_rows shares the transaction that owns the (ON COMMIT DROP) staging table.
    let tx = client.transaction().await.unwrap();
    tx.batch_execute("CREATE TEMP TABLE _ingest_copy_staging (LIKE pgstac.items) ON COMMIT DROP")
        .await
        .unwrap();

    let it = item();
    let row = dehydrate(it.clone(), &schema).unwrap();
    let n = copy_rows(&tx, "_ingest_copy_staging", vec![row], &schema)
        .await
        .unwrap();
    assert_eq!(n, 1, "one row copied");

    // The staged row must equal content_dehydrate of the same item, ignoring the load-time timestamp.
    let matches: bool = tx
        .query_one(
            "SELECT (to_jsonb(s) - 'pgstac_updated_at') = (to_jsonb(d) - 'pgstac_updated_at') \
             FROM _ingest_copy_staging s, content_dehydrate($1::jsonb) d",
            &[&it],
        )
        .await
        .unwrap()
        .get(0);
    assert!(matches, "staged row must equal content_dehydrate output");
    tx.rollback().await.unwrap();
}
