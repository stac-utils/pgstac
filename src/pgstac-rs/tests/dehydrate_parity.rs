//! Parity gate: the Rust `dehydrate` must produce the same `items` row as SQL `content_dehydrate`, so an
//! item ingested through Rust is byte-identical to one ingested through the SQL path.
//!
//! Connects read-only to a pgstac database (`PGSTAC_RS_TEST_DB`, default the dev `postgis` db).

use chrono::{DateTime, Utc};
use pgstac::dehydrate::{DehydrateSchema, PromotedColumn, PromotedKind, PromotedValue, dehydrate};
use pgstac::geom::RawGeometry;
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls, Row};

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

/// Representative items: singular datetime, datetime range, promoted props (text/float/int/array/jsonb/
/// timestamptz), empty/absent assets+links, extra top-level keys.
fn items() -> Vec<Value> {
    vec![
        json!({
            "type": "Feature",
            "stac_version": "1.0.0",
            "stac_extensions": ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
            "id": "item-singular",
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
                "custom:keep": "stays-in-properties"
            },
            "assets": {"thumbnail": {"href": "https://x/t.png", "type": "image/png"}},
            "links": [{"rel": "self", "href": "https://x/item-1"}, {"rel": "root", "href": "https://x/"}],
            "extra_top": {"a": 1}
        }),
        json!({
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "item-range",
            "collection": "c1",
            "geometry": {"type": "Polygon", "coordinates": [[[-85.3, 30.9], [-85.3, 31.0], [-85.2, 31.0], [-85.2, 30.9], [-85.3, 30.9]]]},
            "properties": {
                "start_datetime": "2020-01-01T00:00:00Z",
                "end_datetime": "2020-06-01T00:00:00Z",
                "datetime": null,
                "eo:cloud_cover": 0.0
            },
            "assets": {},
            "links": []
        }),
        json!({
            "type": "Feature",
            "id": "item-minimal",
            "collection": "c2",
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "properties": {"datetime": "2024-06-26T12:00:00.500Z"}
        }),
    ]
}

fn read_sql_promoted(row: &Row, col: &PromotedColumn) -> PromotedValue {
    let c = col.column.as_str();
    match col.kind {
        PromotedKind::Text => PromotedValue::Text(row.get(c)),
        PromotedKind::Float => PromotedValue::Float(row.get(c)),
        PromotedKind::Int => PromotedValue::Int(row.get(c)),
        PromotedKind::BigInt => PromotedValue::BigInt(row.get(c)),
        PromotedKind::TextArray => PromotedValue::TextArray(row.get(c)),
        PromotedKind::Jsonb => PromotedValue::Jsonb(row.get(c)),
        PromotedKind::Timestamptz => PromotedValue::Timestamptz(row.get(c)),
    }
}

#[tokio::test]
async fn dehydrate_matches_content_dehydrate() {
    let client = connect().await;
    let schema = DehydrateSchema::load(&client).await.unwrap();

    for item in items() {
        let row = client
            .query_one("SELECT * FROM content_dehydrate($1::jsonb)", &[&item])
            .await
            .unwrap();
        let id_for_msg = item["id"].as_str().unwrap_or("?").to_string();
        let rust = dehydrate(item, &schema).unwrap();

        assert_eq!(rust.id, row.get::<_, String>("id"), "id ({id_for_msg})");
        assert_eq!(
            rust.collection,
            row.get::<_, String>("collection"),
            "collection ({id_for_msg})"
        );
        assert_eq!(
            rust.datetime,
            row.get::<_, DateTime<Utc>>("datetime"),
            "datetime ({id_for_msg})"
        );
        assert_eq!(
            rust.end_datetime,
            row.get::<_, DateTime<Utc>>("end_datetime"),
            "end_datetime ({id_for_msg})"
        );
        assert_eq!(
            rust.datetime_is_range,
            row.get::<_, bool>("datetime_is_range"),
            "datetime_is_range ({id_for_msg})"
        );
        assert_eq!(
            rust.stac_version,
            row.get::<_, Option<String>>("stac_version"),
            "stac_version ({id_for_msg})"
        );
        assert_eq!(
            rust.stac_extensions,
            row.get::<_, Value>("stac_extensions"),
            "stac_extensions ({id_for_msg})"
        );
        assert_eq!(
            rust.item_hash.to_vec(),
            row.get::<_, Vec<u8>>("item_hash"),
            "item_hash ({id_for_msg})"
        );
        assert_eq!(
            rust.bbox,
            row.get::<_, Option<Value>>("bbox"),
            "bbox ({id_for_msg})"
        );
        assert_eq!(
            rust.links,
            row.get::<_, Option<Value>>("links"),
            "links ({id_for_msg})"
        );
        assert_eq!(
            rust.assets,
            row.get::<_, Option<Value>>("assets"),
            "assets ({id_for_msg})"
        );
        assert_eq!(
            rust.properties,
            row.get::<_, Value>("properties"),
            "properties ({id_for_msg})"
        );
        assert_eq!(
            rust.extra,
            row.get::<_, Value>("extra"),
            "extra ({id_for_msg})"
        );
        assert_eq!(
            rust.link_hrefs,
            row.get::<_, Option<Vec<Option<String>>>>("link_hrefs"),
            "link_hrefs ({id_for_msg})"
        );
        assert_eq!(
            rust.geometry,
            row.get::<_, RawGeometry>("geometry").0,
            "geometry ({id_for_msg})"
        );

        for (i, col) in schema.columns().iter().enumerate() {
            assert_eq!(
                rust.promoted[i],
                read_sql_promoted(&row, col),
                "promoted {} ({id_for_msg})",
                col.column
            );
        }
    }
}
