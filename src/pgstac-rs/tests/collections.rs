//! Fidelity gate for the Rust collection search + getters against their SQL counterparts.
//!
//! Targets a clean fragment instance (default `pgstac_rs_test_rich`); skips if unreachable.

#![allow(unused_crate_dependencies)]

use pgstac::collections::{collection_search, get_collection, get_queryables};
use pgstac::search::get_item;
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
            eprintln!("[collections] skipping: cannot connect ({dsn}): {e}");
            None
        }
    }
}

async fn sql_collection_search(client: &Client, body: &Value) -> Value {
    client
        .query_one("SELECT collection_search($1::jsonb) AS cs", &[body])
        .await
        .expect("sql collection_search")
        .get("cs")
}

fn link_token(doc: &Value, rel: &str) -> Option<String> {
    doc["links"]
        .as_array()?
        .iter()
        .find(|l| l["rel"] == rel)
        .and_then(|l| l["href"].as_str())
        .and_then(|href| href.split("token=").nth(1))
        .map(str::to_string)
}

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap_or_default().to_string()
}

#[tokio::test]
async fn collection_search_paginates_and_matches_sql() {
    let Some(client) = connect().await else {
        return;
    };
    // One collection per page; walk all of them via next tokens and compare to SQL collection_search.
    let body = json!({"limit": 1, "sortby": [{"field": "id", "direction": "asc"}]});
    let mut token: Option<String> = None;
    let mut ids = Vec::new();
    let mut guard = 0;
    loop {
        guard += 1;
        assert!(guard < 100, "collection pagination did not terminate");

        let page = collection_search(&client, &body, token.as_deref())
            .await
            .unwrap();

        let mut sql_body = body.clone();
        if let Some(t) = &token {
            let _ = sql_body
                .as_object_mut()
                .unwrap()
                .insert("token".into(), Value::String(t.clone()));
        }
        let sql = sql_collection_search(&client, &sql_body).await;
        let sql_collections = sql["collections"].as_array().cloned().unwrap_or_default();

        assert_eq!(
            page.features, sql_collections,
            "collections mismatch at token {token:?}"
        );
        assert_eq!(
            page.next_token,
            link_token(&sql, "next"),
            "next token mismatch"
        );
        assert_eq!(
            page.prev_token,
            link_token(&sql, "prev"),
            "prev token mismatch"
        );

        ids.extend(page.features.iter().map(id_of));
        match page.next_token {
            Some(next) if !page.features.is_empty() => token = Some(next),
            _ => break,
        }
    }
    assert_eq!(
        ids.len(),
        3,
        "expected to walk all 3 collections, got {ids:?}"
    );
    eprintln!(
        "[collections] paginated {} collections, matches collection_search()",
        ids.len()
    );
}

#[tokio::test]
async fn getters_return_the_right_objects() {
    let Some(client) = connect().await else {
        return;
    };

    // get_collection
    let coll = get_collection(&client, "landsat-c2-l2").await.unwrap();
    assert_eq!(coll.as_ref().map(id_of), Some("landsat-c2-l2".to_string()));
    assert!(
        get_collection(&client, "does-not-exist")
            .await
            .unwrap()
            .is_none()
    );

    // get_item: pick a real (collection, id) from the instance.
    let row = client
        .query_one(
            "SELECT collection, id FROM items WHERE collection = 'landsat-c2-l2' LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    let (collection, id): (String, String) = (row.get("collection"), row.get("id"));
    let item = get_item(&client, &collection, &id)
        .await
        .unwrap()
        .expect("item");
    assert_eq!(item["id"], id);
    // Fully hydrated: a landsat item carries its assets.
    assert!(item["assets"].is_object() && !item["assets"].as_object().unwrap().is_empty());
    assert!(
        get_item(&client, &collection, "no-such-item")
            .await
            .unwrap()
            .is_none()
    );

    // get_queryables, catalog-wide and collection-scoped, are JSON objects.
    let q = get_queryables(&client, None).await.unwrap();
    assert!(q.is_object() && q.get("properties").is_some());
    let qc = get_queryables(&client, Some("landsat-c2-l2"))
        .await
        .unwrap();
    assert!(qc.is_object() && qc.get("properties").is_some());
    eprintln!("[collections] getters + queryables ok");
}
