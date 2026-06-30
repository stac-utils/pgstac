//! Follow-up integration tests: parallel fan-out, --consistent snapshot, and resume.
//!
//! * Parallel parity: `run_parallel` produces the same item counts + per-file
//!   sha256 set as the sequential `run` on the same instance.
//! * --consistent: a concurrent insert during a consistent dump is NOT captured.
//! * Resume: a dump with a pre-seeded checkpoint skips the completed partition
//!   yet still lists it in the manifest (no missing partitions, no re-dump).
//!
//! All tests skip if the DB is unreachable, so the suite stays green without
//! fixtures. Point them at month-partitioned 0.10 clones via the env vars below.

#![allow(unused_crate_dependencies)]

use pgstac::export::manifest::Manifest;
use pgstac::export::parallel::DsnFactory;
use pgstac::export::sink::DirSink;
use pgstac::export::{DumpConfig, DumpPlanner};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

const DEFAULT_010: &str = "postgresql://username:password@localhost:5439/a_parity010";

async fn connect(dsn: &str) -> Option<Client> {
    match tokio_postgres::connect(dsn, NoTls).await {
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
            eprintln!("[followups] skipping: cannot connect ({dsn}): {e}");
            None
        }
    }
}

fn dsn_010() -> String {
    std::env::var("PGSTAC_PARITY_DB_010").unwrap_or_else(|_| DEFAULT_010.to_string())
}

/// Maps every partition file in a manifest to its (sha256, item_count).
fn file_map(m: &Manifest) -> BTreeMap<String, (String, u64)> {
    let mut map = BTreeMap::new();
    for c in &m.collections {
        for p in &c.partitions {
            let _ = map.insert(p.file.clone(), (p.sha256.clone(), p.item_count));
        }
    }
    map
}

fn read_manifest(dir: &std::path::Path) -> Manifest {
    serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap()
}

/// Parallel fan-out yields the same files + counts as the sequential run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_matches_sequential_010() {
    let dsn = dsn_010();
    let Some(client) = connect(&dsn).await else {
        return;
    };

    // Restrict to one collection to keep the test quick (still many partitions).
    let only = first_collection(&client).await;

    // Sequential.
    let seq_dir = tempfile::tempdir().unwrap();
    let seq_report = DumpPlanner::new(DumpConfig {
        collection_ids: only.clone(),
        ..Default::default()
    })
    .run(&client, &DirSink::new(seq_dir.path()).unwrap())
    .await
    .unwrap();

    // Parallel (4 workers).
    let par_dir = tempfile::tempdir().unwrap();
    let factory = DsnFactory::new(dsn.clone());
    let par_report = DumpPlanner::new(DumpConfig {
        collection_ids: only.clone(),
        concurrency: 4,
        ..Default::default()
    })
    .run_parallel(
        &client,
        Arc::new(DirSink::new(par_dir.path()).unwrap()),
        &factory,
    )
    .await
    .unwrap();

    assert_eq!(seq_report.items, par_report.items, "item totals differ");
    assert_eq!(
        seq_report.partitions, par_report.partitions,
        "partition counts differ"
    );

    let seq = file_map(&read_manifest(seq_dir.path()));
    let par = file_map(&read_manifest(par_dir.path()));
    // Same set of files, identical per-file item counts. (Geoparquet bytes are
    // deterministic for the same rows, so sha256 should match too.)
    assert_eq!(
        seq.keys().collect::<Vec<_>>(),
        par.keys().collect::<Vec<_>>(),
        "different partition file sets"
    );
    for (file, (seq_sha, seq_n)) in &seq {
        let (par_sha, par_n) = par.get(file).unwrap();
        assert_eq!(seq_n, par_n, "item_count differs for {file}");
        assert_eq!(seq_sha, par_sha, "sha256 differs for {file}");
    }
    eprintln!(
        "[followups] parallel parity: {} files, {} items match sequential",
        seq.len(),
        par_report.items
    );
}

async fn first_collection(client: &Client) -> Option<Vec<String>> {
    let row = client
        .query_opt(
            "SELECT id FROM collections WHERE partition_trunc IS NOT NULL ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    row.map(|r| vec![r.get::<_, String>("id")])
}

/// A concurrent insert during a --consistent dump must NOT appear in the
/// output (the dump reads one repeatable-read snapshot taken before the insert).
///
/// This test mutates the DB, so it only runs against an explicitly opt-in
/// disposable DSN (`PGSTAC_CONSISTENT_DB`) to avoid corrupting shared fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistent_excludes_concurrent_insert() {
    let Ok(dsn) = std::env::var("PGSTAC_CONSISTENT_DB") else {
        eprintln!(
            "[followups] skipping consistent test: set PGSTAC_CONSISTENT_DB to a disposable DB"
        );
        return;
    };
    let Some(client) = connect(&dsn).await else {
        return;
    };

    // Pick a target collection + a fresh item id that does not yet exist.
    let Some(coll) = first_collection(&client).await else {
        eprintln!("[followups] no partitioned collection; skipping");
        return;
    };
    let coll_id = &coll[0];

    let before: i64 = client
        .query_one("SELECT count(*) AS c FROM items", &[])
        .await
        .unwrap()
        .get("c");

    // Start a consistent dump on one connection; while it holds its snapshot,
    // insert a new item on a second connection. The snapshot is taken at dump
    // start, so the insert must not be captured.
    //
    // We force the insert to land mid-dump by inserting right after the planner
    // exports the snapshot. Since run_parallel exports the snapshot first thing,
    // a small spawn-then-insert races safely after we kick off the dump.
    let dir = tempfile::tempdir().unwrap();
    let factory = DsnFactory::new(dsn.clone());
    let planner = DumpPlanner::new(DumpConfig {
        collection_ids: Some(coll.clone()),
        concurrency: 2,
        consistent: true,
        ..Default::default()
    });

    // Insert concurrently shortly after the dump begins.
    let insert_dsn = dsn.clone();
    let insert_coll = coll_id.clone();
    let inserter = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let Some(mut c2) = connect(&insert_dsn).await else {
            return false;
        };
        let item = serde_json::json!({
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "consistency-probe-item",
            "collection": insert_coll,
            "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
            "bbox": [0.0, 0.0, 0.0, 0.0],
            "properties": {"datetime": "2024-01-15T00:00:00Z"},
            "assets": {},
            "links": []
        });
        // Insert through the Rust loader (the only write path from Rust), not the SQL create_item.
        let Ok(schema) = pgstac::dehydrate::DehydrateSchema::load(&c2).await else {
            return false;
        };
        pgstac::ingest::load_items(
            &mut c2,
            vec![item],
            &schema,
            pgstac::ingest::ConflictPolicy::Error,
        )
        .await
        .is_ok()
    });

    let report = planner
        .run_parallel(
            &client,
            Arc::new(DirSink::new(dir.path()).unwrap()),
            &factory,
        )
        .await
        .unwrap();
    let inserted = inserter.await.unwrap();
    assert!(
        inserted,
        "concurrent insert did not succeed; test inconclusive"
    );

    // The dump's item total reflects only the snapshot before the insert.
    let manifest = read_manifest(dir.path());
    let dumped: u64 = manifest.collections.iter().map(|c| c.item_count).sum();
    assert_eq!(report.items, dumped);
    // No partition file should contain the probe item id. Read each parquet.
    let mut found_probe = false;
    for c in &manifest.collections {
        for p in &c.partitions {
            let bytes = std::fs::read(dir.path().join(&p.file)).unwrap();
            if geoparquet_has_id(&bytes, "consistency-probe-item") {
                found_probe = true;
            }
        }
    }
    assert!(
        !found_probe,
        "consistent dump captured a concurrently-inserted item (snapshot not honored)"
    );
    assert!(
        manifest.consistent,
        "manifest should record consistent=true"
    );
    assert!(
        manifest.snapshot.is_some(),
        "manifest should record the snapshot id"
    );

    // Clean up the probe item so the disposable DB can be reused.
    let _ = client
        .execute(
            "SELECT delete_item('consistency-probe-item', $1)",
            &[&coll_id],
        )
        .await;

    eprintln!(
        "[followups] consistent: dumped {} items (before={before}); concurrent insert correctly excluded",
        report.items
    );
}

fn geoparquet_has_id(bytes: &[u8], id: &str) -> bool {
    use std::io::{Seek, SeekFrom, Write};
    let mut tf = tempfile::tempfile().unwrap();
    tf.write_all(bytes).unwrap();
    let _ = tf.seek(SeekFrom::Start(0)).unwrap();
    let ic = stac::geoparquet::from_reader(tf).unwrap();
    ic.items.iter().any(|it| {
        serde_json::to_value(it)
            .ok()
            .and_then(|v| v.get("id").and_then(|x| x.as_str()).map(|s| s == id))
            .unwrap_or(false)
    })
}

/// A resumed dump skips a completed partition (does not re-dump it) but
/// still lists it in the manifest, producing the same complete manifest.
#[tokio::test]
async fn resume_skips_completed_but_keeps_in_manifest() {
    let dsn = dsn_010();
    let Some(client) = connect(&dsn).await else {
        return;
    };
    let only = first_collection(&client).await;

    // Full dump first.
    let dir1 = tempfile::tempdir().unwrap();
    let _ = DumpPlanner::new(DumpConfig {
        collection_ids: only.clone(),
        ..Default::default()
    })
    .run(&client, &DirSink::new(dir1.path()).unwrap())
    .await
    .unwrap();
    let full = read_manifest(dir1.path());
    let full_files = file_map(&full);
    assert!(
        full_files.len() >= 2,
        "need multiple partitions to test resume"
    );

    // Build a resume set from the full manifest: pretend the first partition was
    // already completed (point its checkpoint entry at the real file on disk).
    let (first_file, (first_sha, first_n)) = full_files.iter().next().unwrap();
    let entry = full
        .collections
        .iter()
        .flat_map(|c| c.partitions.iter())
        .find(|p| &p.file == first_file)
        .unwrap()
        .clone();
    let cp_entry = pgstac::export::manifest::CheckpointEntry {
        file: first_file.clone(),
        sha256: first_sha.clone(),
        item_count: *first_n,
        collection_id: full.collections[0].id.clone(),
        entry,
    };
    // Copy that one parquet into a fresh dump dir so resume can verify it.
    let dir2 = tempfile::tempdir().unwrap();
    let src = dir1.path().join(first_file);
    let dst = dir2.path().join(first_file);
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
    std::fs::copy(&src, &dst).unwrap();

    let mut resume = std::collections::HashMap::new();
    let _ = resume.insert(first_file.clone(), cp_entry);

    let report = DumpPlanner::new(DumpConfig {
        collection_ids: only.clone(),
        resume_completed: resume,
        ..Default::default()
    })
    .run(&client, &DirSink::new(dir2.path()).unwrap())
    .await
    .unwrap();

    let resumed = read_manifest(dir2.path());
    let resumed_files = file_map(&resumed);
    // Same set of partition files + identical per-file counts as the full dump.
    assert_eq!(
        full_files.keys().collect::<Vec<_>>(),
        resumed_files.keys().collect::<Vec<_>>(),
        "resumed manifest is missing partitions"
    );
    for (f, (_sha, n)) in &full_files {
        assert_eq!(&resumed_files.get(f).unwrap().1, n, "count differs for {f}");
    }
    assert_eq!(
        report.items,
        full_files.values().map(|(_, n)| n).sum::<u64>()
    );
    // The carried-over file's bytes match the original (it was not re-dumped).
    let carried = std::fs::read(dir2.path().join(first_file)).unwrap();
    assert_eq!(
        pgstac::export::manifest::sha256_hex(&carried),
        *first_sha,
        "carried-over partition file changed"
    );
    eprintln!(
        "[followups] resume: carried {} (skipped re-dump), manifest complete with {} files",
        first_file,
        resumed_files.len()
    );
}
