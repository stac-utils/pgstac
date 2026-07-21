//! Parity gate: the Rust `extract_fragment` / `strip_fragment_col` must match the SQL functions of the
//! same name (003a_items.sql), so the Rust loader splits fragment-config collections exactly like
//! `items_staging_dehydrate`. Connects read-only to a pgstac database (`PGSTAC_RS_TEST_DB`).

use pgstac::fragment::{
    FragmentConfig, build_fragment_payload, extract_fragment, strip_fragment_col, strip_link_hrefs,
};
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

/// A representative item with nested asset metadata + properties to fragment.
fn content() -> Value {
    json!({
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "frag-item",
        "collection": "c1",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {
            "datetime": "2023-01-07T00:00:00Z",
            "platform": "landsat-8",
            "constellation": "landsat"
        },
        "assets": {
            "B1": {
                "href": "https://x/B1.tif",
                "type": "image/tiff; application=geotiff",
                "title": "Coastal/Aerosol",
                "roles": ["data"],
                "eo:bands": [{"name": "B1", "common_name": "coastal"}]
            },
            "thumbnail": {
                "href": "https://x/t.png",
                "type": "image/png",
                "roles": ["thumbnail"]
            }
        }
    })
}

/// Fragment configs covering depth-2, depth-3, a whole-column path, and a top-level scalar.
fn configs() -> Vec<Vec<String>> {
    vec![
        vec![
            r#"["assets","B1","type"]"#.into(),
            r#"["assets","B1","roles"]"#.into(),
            r#"["assets","B1","eo:bands"]"#.into(),
            r#"["assets","thumbnail","type"]"#.into(),
        ],
        vec![
            r#"["properties","platform"]"#.into(),
            r#"["properties","constellation"]"#.into(),
        ],
        vec![r#"["assets","thumbnail"]"#.into()],
        vec![
            r#"["assets","B1","title"]"#.into(),
            r#"["properties","missing-key"]"#.into(),
        ],
    ]
}

#[tokio::test]
async fn extract_and_strip_match_sql() {
    let client = connect().await;
    let item = content();

    for config in configs() {
        let rust_config = FragmentConfig::parse(&config).unwrap();

        // extract_fragment(content, config)
        let sql_extract: Option<Value> = client
            .query_one(
                "SELECT extract_fragment($1::jsonb, $2::text[])",
                &[&item, &config],
            )
            .await
            .unwrap()
            .get(0);
        let rust_extract = extract_fragment(&item, &rust_config);
        assert_eq!(
            rust_extract, sql_extract,
            "extract_fragment mismatch for {config:?}"
        );

        // strip_fragment_col(col, col_name, config) for assets + properties
        for col_name in ["assets", "properties"] {
            let col = item[col_name].clone();
            let sql_strip: Value = client
                .query_one(
                    "SELECT strip_fragment_col($1::jsonb, $2::text, $3::text[])",
                    &[&col, &col_name, &config],
                )
                .await
                .unwrap()
                .get(0);
            let rust_strip = strip_fragment_col(col.clone(), col_name, &rust_config);
            assert_eq!(
                rust_strip, sql_strip,
                "strip_fragment_col({col_name}) mismatch for {config:?}"
            );
        }
    }
}

#[tokio::test]
async fn strip_link_hrefs_matches_sql() {
    let client = connect().await;
    let cases = vec![
        json!([{"rel": "self", "href": "https://x/1"}, {"rel": "root", "href": "https://x/"}]),
        json!([]),
        json!([{"rel": "license", "title": "L"}]),
        json!(null),
        json!([{"rel": "self", "href": "https://x/1"}, "not-an-object"]),
    ];
    for links in cases {
        let sql: Option<Value> = client
            .query_one("SELECT stac_links_strip_hrefs($1::jsonb)", &[&links])
            .await
            .unwrap()
            .get(0);
        let rust = strip_link_hrefs(&links);
        assert_eq!(rust, sql, "strip_link_hrefs mismatch for {links:?}");
    }
}

#[tokio::test]
async fn build_fragment_payload_matches_sql() {
    let client = connect().await;
    // content() has assets+properties (no links); add a links-bearing item to exercise links_template.
    let mut with_links = content();
    with_links["links"] = json!([
        {"rel": "self", "href": "https://x/frag-item"},
        {"rel": "license", "title": "L"}
    ]);
    let items = vec![content(), with_links];

    for item in &items {
        for config in configs() {
            let rust = build_fragment_payload(item, &FragmentConfig::parse(&config).unwrap())
                // SQL jsonb_strip_nulls yields {} (not NULL) when there is no fragment; map None -> {}.
                .unwrap_or_else(|| json!({}));
            let sql: Value = client
                .query_one(
                    "SELECT jsonb_strip_nulls(jsonb_build_object(\
                       'content', NULLIF(extract_fragment($1::jsonb, $2::text[]), '{}'::jsonb), \
                       'links_template', stac_links_strip_hrefs($1::jsonb -> 'links')))",
                    &[item, &config],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                rust, sql,
                "payload mismatch for id={} config={config:?}",
                item["id"]
            );
        }
    }
}
