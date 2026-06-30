//! End-to-end dump: DumpPlanner -> DirSink, full + partial,
//! on a 0.9.11 (NULL-trunc, base_item) and a 0.10 (month, fragment) instance.
//!
//! Validates the on-disk layout + manifest (MANIFEST.md), per-file SHA-256, item
//! counts vs SQL, and that every partition geoparquet is readable and hydrated.
//! Skips if the DB is unreachable.

#![allow(unused_crate_dependencies)]

use pgstac::export::manifest::Manifest;
use pgstac::export::plan::Prefilter;
use pgstac::export::sink::DirSink;
use pgstac::export::{DumpConfig, DumpPlanner};
use std::collections::HashMap;
use std::path::Path;
use tokio_postgres::{Client, NoTls};

async fn connect(env: &str, default: &str) -> Option<Client> {
    let conn = std::env::var(env).unwrap_or_else(|_| default.to_string());
    match tokio_postgres::connect(&conn, NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            if client
                .batch_execute("SET search_path = pgstac, public;")
                .await
                .is_err()
            {
                return None;
            }
            Some(client)
        }
        Err(e) => {
            eprintln!("[dump] skipping: cannot connect via {env} ({default}): {e}");
            None
        }
    }
}

/// SQL item counts per collection.
async fn sql_counts(client: &Client) -> HashMap<String, i64> {
    let rows = client
        .query(
            "SELECT collection, count(*) AS c FROM items GROUP BY collection",
            &[],
        )
        .await
        .unwrap();
    rows.iter()
        .map(|r| (r.get::<_, String>("collection"), r.get::<_, i64>("c")))
        .collect()
}

/// Reads + sha256-verifies the manifest against the on-disk files, and confirms
/// every partition geoparquet is readable with the recorded item count.
fn verify_dump(root: &Path, sql_counts: &HashMap<String, i64>, expect_full: bool) {
    let manifest_path = root.join("manifest.json");
    assert!(
        manifest_path.exists(),
        "manifest.json must exist (complete)"
    );
    // Manifest presence is the completion marker; the checkpoint is removed.
    assert!(
        !root.join("_checkpoint.json").exists(),
        "_checkpoint.json should be removed on success"
    );

    let manifest_bytes = std::fs::read(&manifest_path).unwrap();
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.manifest_version, "1");
    assert!(manifest.options.hydrated);
    assert_eq!(manifest.options.ordering, vec!["datetime", "id"]);
    if expect_full {
        assert_eq!(manifest.dump_type, "full");
        assert!(manifest.filter.is_none());
    }

    // Root metadata files exist + sha256 match.
    for fe in manifest.metadata_files.values() {
        let p = root.join(&fe.path);
        let bytes = std::fs::read(&p).unwrap_or_else(|_| panic!("missing {}", fe.path));
        assert_eq!(
            pgstac::export::manifest::sha256_hex(&bytes),
            fe.sha256,
            "sha256 mismatch for {}",
            fe.path
        );
        assert_eq!(bytes.len() as u64, fe.bytes);
    }

    for coll in &manifest.collections {
        // collection.json
        let cp = root.join(&coll.collection_file.path);
        let cbytes = std::fs::read(&cp).unwrap();
        assert_eq!(
            pgstac::export::manifest::sha256_hex(&cbytes),
            coll.collection_file.sha256
        );

        // Sum of partition item counts == collection count == SQL count.
        let mut sum = 0u64;
        for part in &coll.partitions {
            let pp = root.join(&part.file);
            let pbytes = std::fs::read(&pp).unwrap_or_else(|_| panic!("missing {}", part.file));
            assert_eq!(
                pgstac::export::manifest::sha256_hex(&pbytes),
                part.sha256,
                "parquet sha256 mismatch for {}",
                part.file
            );
            assert_eq!(pbytes.len() as u64, part.bytes);

            // The geoparquet must be readable with the recorded count.
            let read = read_geoparquet_count(&pbytes);
            assert_eq!(
                read as u64, part.item_count,
                "geoparquet row count != manifest for {}",
                part.file
            );
            sum += part.item_count;
        }
        assert_eq!(sum, coll.item_count, "partition sum != collection count");
        if let Some(expected) = sql_counts.get(&coll.id) {
            assert_eq!(
                coll.item_count as i64, *expected,
                "dumped count != SQL count for {}",
                coll.id
            );
        }
    }
    eprintln!(
        "[dump] verified {} collections, manifest + sha256 + geoparquet readable",
        manifest.collections.len()
    );
}

fn read_geoparquet_count(bytes: &[u8]) -> usize {
    use std::io::{Seek, SeekFrom, Write};
    let mut tf = tempfile::tempfile().unwrap();
    tf.write_all(bytes).unwrap();
    let _ = tf.seek(SeekFrom::Start(0)).unwrap();
    let ic = stac::geoparquet::from_reader(tf).unwrap();
    // Sanity: first item is hydrated (has type Feature + properties).
    if let Some(first) = ic.items.first() {
        let v = serde_json::to_value(first).unwrap();
        assert_eq!(v["type"], "Feature");
        assert!(v.get("properties").is_some());
    }
    ic.items.len()
}

#[tokio::test]
async fn dump_full_0911_nulltrunc() {
    let Some(client) = connect(
        "PGSTAC_PARITY_DB_0911",
        "postgresql://username:password@localhost:5439/a_parity0911",
    )
    .await
    else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let sink = DirSink::new(dir.path()).unwrap();
    let planner = DumpPlanner::new(DumpConfig::default());
    let report = planner.run(&client, &sink).await.unwrap();
    eprintln!(
        "[dump] 0911 full: {} collections, {} partitions, {} items",
        report.collections, report.partitions, report.items
    );
    assert!(report.items > 0);
    let counts = sql_counts(&client).await;
    verify_dump(dir.path(), &counts, true);
}

#[tokio::test]
async fn dump_full_010_month() {
    let Some(client) = connect(
        "PGSTAC_PARITY_DB_010",
        "postgresql://username:password@localhost:5439/a_parity010",
    )
    .await
    else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let sink = DirSink::new(dir.path()).unwrap();
    let planner = DumpPlanner::new(DumpConfig::default());
    let report = planner.run(&client, &sink).await.unwrap();
    eprintln!(
        "[dump] 010 full: {} collections, {} partitions, {} items",
        report.collections, report.partitions, report.items
    );
    assert!(report.items > 0);
    assert!(report.partitions >= 2, "month-partitioned -> many files");
    let counts = sql_counts(&client).await;
    verify_dump(dir.path(), &counts, true);
}

#[tokio::test]
async fn dump_partial_010_one_collection() {
    let Some(client) = connect(
        "PGSTAC_PARITY_DB_010",
        "postgresql://username:password@localhost:5439/a_parity010",
    )
    .await
    else {
        return;
    };
    // Partial: a single collection.
    let dir = tempfile::tempdir().unwrap();
    let sink = DirSink::new(dir.path()).unwrap();
    let config = DumpConfig {
        collection_ids: Some(vec!["bench_uniform".to_string()]),
        prefilter: Prefilter::default(),
        ..Default::default()
    };
    let planner = DumpPlanner::new(config);
    let report = planner.run(&client, &sink).await.unwrap();
    assert_eq!(report.collections, 1);
    assert!(report.items > 0);

    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(dir.path().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.dump_type, "partial");
    let filter = manifest.filter.expect("partial has a filter");
    assert_eq!(filter.collection_ids, vec!["bench_uniform"]);
    assert_eq!(manifest.collections.len(), 1);
    eprintln!("[dump] 010 partial bench_uniform: {} items", report.items);
}
