//! Search-dimension parity: the Rust engine must return the same items as SQL `search()` across every
//! search dimension (ids, bbox, datetime, CQL2 filter, non-datetime sort, and `fields` projection),
//! mirroring the functionalities pypgstac / the upstream client test. Targets the rich fragment
//! instance; skips if unreachable.

#![allow(unused_crate_dependencies)]

use pgstac::search::search_page;
use serde_json::{Value, json};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Option<Client> {
    let dsn = std::env::var("PGSTAC_RS_TEST_RICH_DB").unwrap_or_else(|_| {
        "postgresql://username:password@localhost:5439/pgstac_rs_test_rich".into()
    });
    match tokio_postgres::connect(&dsn, NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(connection);
            client
                .batch_execute("SET search_path = pgstac, public;")
                .await
                .ok()?;
            Some(client)
        }
        Err(e) => {
            eprintln!("[dims] skipping: {e}");
            None
        }
    }
}

async fn sql_features(client: &Client, body: &Value) -> Vec<Value> {
    let fc: Value = client
        .query_one("SELECT search($1::jsonb)::jsonb AS fc", &[body])
        .await
        .expect("sql search")
        .get("fc");
    fc["features"].as_array().cloned().unwrap_or_default()
}

async fn rust_features(client: &Client, body: &Value) -> Vec<Value> {
    let limit = body["limit"].as_i64().unwrap_or(10);
    search_page(client, body, None, limit)
        .await
        .expect("rust search_page")
        .features
}

#[tokio::test]
async fn dimensions_match_sql() {
    let Some(client) = connect().await else {
        return;
    };
    let coll = "landsat-c2-l2";
    // A real item's id + spatial envelope, so the geometry cases are *selective* (a small real footprint),
    // not the trivial world polygon — this exercises the actual spatial WHERE clause + partition pruning.
    let sample = client
        .query_one(
            "SELECT id, ST_AsGeoJSON(ST_Envelope(geometry)) AS env, ST_XMin(geometry) AS xmin, \
             ST_YMin(geometry) AS ymin, ST_XMax(geometry) AS xmax, ST_YMax(geometry) AS ymax \
             FROM items WHERE collection = 'landsat-c2-l2' ORDER BY datetime LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    let sample_id: String = sample.get("id");
    let env: Value =
        serde_json::from_str(&sample.get::<_, String>("env")).expect("envelope geojson");
    let (xmin, ymin, xmax, ymax): (f64, f64, f64, f64) = (
        sample.get("xmin"),
        sample.get("ymin"),
        sample.get("xmax"),
        sample.get("ymax"),
    );

    let bodies = vec![
        (
            "ids",
            json!({"collections": [coll], "ids": [sample_id], "limit": 5}),
        ),
        (
            "bbox",
            json!({"collections": [coll], "bbox": [-180, -90, 180, 90], "limit": 5}),
        ),
        (
            "selective_bbox",
            json!({"collections": [coll], "bbox": [xmin, ymin, xmax, ymax], "limit": 5}),
        ),
        (
            "datetime",
            json!({"collections": [coll], "datetime": "2024-01-01T00:00:00Z/..", "limit": 5}),
        ),
        (
            "cql2_filter",
            json!({"collections": [coll], "filter": {"op": "<", "args": [{"property": "eo:cloud_cover"}, 25]}, "limit": 5}),
        ),
        (
            "query_ext",
            json!({"collections": [coll], "query": {"eo:cloud_cover": {"lt": 25}}, "limit": 5}),
        ),
        (
            "intersects",
            json!({"collections": [coll], "intersects": {"type": "Polygon", "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]]}, "limit": 5}),
        ),
        (
            "selective_intersects",
            json!({"collections": [coll], "intersects": env.clone(), "limit": 5}),
        ),
        // Adversarial: a selective footprint + a wide datetime window + a non-datetime sort all at once — the
        // spatial filter prunes partitions, the sort drives a non-keyset order; both engines must still agree.
        (
            "intersects_datetime_sort",
            json!({"collections": [coll], "intersects": env, "datetime": "2013-01-01T00:00:00Z/..", "sortby": [{"field": "eo:cloud_cover", "direction": "asc"}], "limit": 5}),
        ),
        // A footprint over open ocean: both engines must return zero rows (the empty-result path).
        (
            "intersects_no_match",
            json!({"collections": [coll], "intersects": {"type": "Polygon", "coordinates": [[[0.0, 0.0], [0.01, 0.0], [0.01, 0.01], [0.0, 0.01], [0.0, 0.0]]]}, "limit": 5}),
        ),
        (
            "nondatetime_sort",
            json!({"collections": [coll], "sortby": [{"field": "eo:cloud_cover", "direction": "asc"}], "limit": 5}),
        ),
        (
            "nondatetime_sort_desc",
            json!({"collections": [coll], "sortby": [{"field": "eo:cloud_cover", "direction": "desc"}], "limit": 5}),
        ),
    ];
    for (name, body) in bodies {
        let rust = rust_features(&client, &body).await;
        let sql = sql_features(&client, &body).await;
        assert_eq!(rust, sql, "{name} mismatch (body={body})");
        eprintln!("[dims] {name}: {} items match SQL", rust.len());
    }
}

#[tokio::test]
async fn fields_projection_matches_sql() {
    let Some(client) = connect().await else {
        return;
    };
    let coll = "landsat-c2-l2";
    for fields in [
        json!({"exclude": ["assets", "properties.eo:cloud_cover"]}),
        json!({"include": ["id", "properties.datetime"]}),
        // include id+geometry only: search_plan nulls fragment_id in the projection (no shared
        // fragment needed), so the engine skips the per-row fragment fetch -- must still match SQL.
        json!({"include": ["id", "geometry"]}),
    ] {
        let body = json!({"collections": [coll], "fields": fields, "limit": 3});
        let rust = rust_features(&client, &body).await;
        let sql = sql_features(&client, &body).await;
        assert_eq!(rust, sql, "fields mismatch (fields={fields})");
        eprintln!("[dims] fields {fields}: match SQL");
    }
}
