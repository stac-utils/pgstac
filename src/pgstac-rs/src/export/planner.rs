//! `DumpPlanner` — the dump orchestrator (backup).
//!
//! Enumerates collections + partitions, scans each partition (cursor, ordered),
//! hydrates + encodes geoparquet to a temp file, streams it to the [`Sink`],
//! records per-file SHA-256 + counts + temporal/spatial footprint, checkpoints
//! after each completed partition, writes the root metadata files, and writes
//! `manifest.json` **last** (its presence = a complete dump).
//!
//! Failure policy (A5): default fail-fast; [`DumpConfig::skip_errors`] records a
//! per-item skip and continues. Resume: a partition whose checkpointed
//! file verifies by SHA-256 is skipped; a missing/partial one is redone whole.

use crate::export::budget::MemoryBudget;
use crate::export::format::{Format, GeoparquetMode, GeoparquetStreamWriter, ParquetCompression};
use crate::export::manifest::{
    Checkpoint, CheckpointEntry, CollectionEntry, DatetimeRange, FileEntry, Filter, Manifest,
    Options, PartitionEntry, Source, Tool, sha256_hex,
};
use crate::export::metadata::{collection_json, queryables_json, settings_json};
use crate::export::plan::{Prefilter, plan_collections};
use crate::export::sink::Sink;
use crate::hydrate::HydrationModel;
use crate::source::{ScanFilter, detect_hydration_model, load_collection_context, scan_partition};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use tokio_postgres::GenericClient;

/// Configuration for a dump run.
#[derive(Debug, Clone)]
pub struct DumpConfig {
    /// Restrict to these collection ids (partial dump). `None` = all (full).
    pub collection_ids: Option<Vec<String>>,
    /// Datetime/bbox prefilter (partial dump).
    pub prefilter: Prefilter,
    /// Parquet compression.
    pub compression: ParquetCompression,
    /// Continue past per-item errors, recording them (A5). Default false.
    pub skip_errors: bool,
    /// Resume from a prior run: partition files already completed (and
    /// verified by SHA-256), keyed by relative path. The caller loads + verifies
    /// a prior `_checkpoint.json` (see the CLI's `load_resume_set`). A keyed
    /// partition is *not* re-dumped; its prior entry is carried straight into the
    /// new manifest, so a resumed dump produces the same complete manifest as an
    /// uninterrupted run.
    pub resume_completed: std::collections::HashMap<String, CheckpointEntry>,
    /// Memory budget for the buffered geoparquet path; `None` = default ~25% RAM.
    pub memory_budget: Option<u64>,
    /// Exporter binary version string (for the manifest `tool.version`).
    pub tool_version: String,
    /// Optional informational server description (no credentials).
    pub server: Option<String>,
    /// Max partitions processed concurrently in [`DumpPlanner::run_parallel`]
    ///. `1` = effectively sequential. The sequential [`DumpPlanner::run`]
    /// ignores it. Concurrency is additionally bounded by the global memory
    /// budget on the buffered (0.9.11) path.
    pub concurrency: usize,
    /// Run the whole dump under one repeatable-read snapshot. Default off.
    /// Honored by [`DumpPlanner::run_parallel`]: a coordinator transaction
    /// exports its snapshot and every worker `SET TRANSACTION SNAPSHOT`s onto it,
    /// so all partitions see one point in time. The plain [`DumpPlanner::run`]
    /// ignores it (single connection sees its own view already).
    pub consistent: bool,
}

impl Default for DumpConfig {
    fn default() -> Self {
        DumpConfig {
            collection_ids: None,
            prefilter: Prefilter::default(),
            compression: ParquetCompression::default(),
            skip_errors: false,
            resume_completed: std::collections::HashMap::new(),
            memory_budget: None,
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            server: None,
            concurrency: 1,
            consistent: false,
        }
    }
}

/// Summary returned by a completed dump.
#[derive(Debug, Clone)]
pub struct DumpReport {
    /// Number of collections dumped.
    pub collections: usize,
    /// Number of partition files written.
    pub partitions: usize,
    /// Total items written.
    pub items: u64,
    /// Items skipped (only non-zero with `skip_errors`).
    pub skipped: u64,
}

/// The dump orchestrator.
#[derive(Debug)]
pub struct DumpPlanner {
    config: DumpConfig,
    budget: MemoryBudget,
}

impl DumpPlanner {
    /// Creates a planner with the given config.
    pub fn new(config: DumpConfig) -> Self {
        let budget = match config.memory_budget {
            Some(bytes) => MemoryBudget::with_bytes(bytes),
            None => MemoryBudget::default_budget(),
        };
        DumpPlanner { config, budget }
    }

    /// Runs the dump against `client`, writing to `sink`.
    ///
    /// `client` must be on `search_path = pgstac, public`. Works on 0.9.11
    /// (base_item) and 0.10 (fragment).
    pub async fn run<C: GenericClient, S: Sink>(&self, client: &C, sink: &S) -> Result<DumpReport> {
        let model = detect_hydration_model(client).await?;
        let pgstac_version: String = client
            .query_one("SELECT get_version() AS v", &[])
            .await?
            .get("v");

        let plans = plan_collections(
            client,
            self.config.collection_ids.as_deref(),
            &self.config.prefilter,
        )
        .await?;

        // Checkpoint: records each completed partition file as it lands;
        // written after every partition and removed implicitly when the manifest
        // (the completion marker) is written last. Resume consults
        // `config.resume_completed` (file paths already verified by the caller).
        let mut checkpoint = Checkpoint {
            manifest_version: crate::export::manifest::MANIFEST_VERSION.to_string(),
            started_at: now_rfc3339(),
            completed: Vec::new(),
        };
        let already_done = &self.config.resume_completed;

        let geoparquet_mode = match model {
            HydrationModel::BaseItem => GeoparquetMode::Buffered,
            HydrationModel::Fragment => GeoparquetMode::Stream,
        };

        let mut collection_entries: Vec<CollectionEntry> = Vec::new();
        let mut total_items: u64 = 0;
        let mut total_skipped: u64 = 0;
        let mut total_partition_files: usize = 0;

        for plan in &plans {
            let ctx = load_collection_context(client, model, &plan.id).await?;
            let coll_dir = format!("collections/{}", encode_collection_dir(&plan.id));

            // collection.json
            let coll_json = collection_json(plan);
            let coll_bytes = serde_json::to_vec_pretty(&coll_json)?;
            let coll_file = sink.put(&format!("{coll_dir}/collection.json"), &coll_bytes)?;

            let mut partition_entries: Vec<PartitionEntry> = Vec::new();
            let mut coll_item_count: u64 = 0;

            for partition in &plan.partitions {
                let rel_file = format!("{coll_dir}/{}", partition.file_name);

                // Resume: a caller-verified completed file is NOT
                // re-dumped; its prior entry is carried into the new manifest so
                // the resumed dump's manifest is identical to an uninterrupted
                // run (no missing partitions).
                if let Some(prior) = already_done.get(&rel_file) {
                    let entry = prior.entry.clone();
                    coll_item_count += entry.item_count;
                    total_items += entry.item_count;
                    total_partition_files += 1;
                    checkpoint.completed.push(prior.clone());
                    let cp_bytes = serde_json::to_vec_pretty(&checkpoint)?;
                    let _ = sink.put("_checkpoint.json", &cp_bytes)?;
                    partition_entries.push(entry);
                    continue;
                }

                let scan_filter = scan_filter_for(&self.config.prefilter);
                let outcome = dump_one_partition(
                    client,
                    sink,
                    model,
                    geoparquet_mode,
                    &partition.name,
                    &plan.id,
                    &rel_file,
                    &ctx,
                    &scan_filter,
                    self.config.compression,
                    self.config.skip_errors,
                    &self.budget,
                )
                .await?;

                total_skipped += outcome.skipped;

                if let Some(entry) = outcome.entry {
                    coll_item_count += entry.item_count;
                    total_items += entry.item_count;
                    total_partition_files += 1;
                    checkpoint.completed.push(CheckpointEntry {
                        file: entry.file.clone(),
                        sha256: entry.sha256.clone(),
                        item_count: entry.item_count,
                        collection_id: plan.id.clone(),
                        entry: entry.clone(),
                    });
                    // Persist checkpoint after each completed partition.
                    let cp_bytes = serde_json::to_vec_pretty(&checkpoint)?;
                    let _ = sink.put("_checkpoint.json", &cp_bytes)?;
                    partition_entries.push(entry);
                }
            }

            collection_entries.push(CollectionEntry {
                id: plan.id.clone(),
                collection_file: FileEntry {
                    path: format!("{coll_dir}/collection.json"),
                    sha256: coll_file.sha256,
                    bytes: coll_file.bytes,
                    count: None,
                },
                partition_trunc: plan.partition_trunc.as_manifest_str().map(str::to_string),
                item_count: coll_item_count,
                partitions: partition_entries,
            });
        }

        self.write_metadata_and_manifest(client, sink, collection_entries, pgstac_version, None)
            .await?;

        Ok(DumpReport {
            collections: plans.len(),
            partitions: total_partition_files,
            items: total_items,
            skipped: total_skipped,
        })
    }

    /// Parallel partition fan-out with optional single snapshot.
    ///
    /// `primary` is one connection used for the cheap sequential work (model
    /// detection, planning, collection.json / queryables / settings / manifest).
    /// Worker connections for the concurrent partition scans come from `factory`;
    /// up to [`DumpConfig::concurrency`] run at once, each owning its own
    /// connection. `sink` is shared (`Arc`) across workers — fs/object_store
    /// sinks write concurrently; the TAR sink serializes internally.
    ///
    /// With [`DumpConfig::consistent`] a coordinator transaction exports one
    /// repeatable-read snapshot and every worker binds to it, so all partitions
    /// see one point in time.
    ///
    /// Resume: a partition file already in
    /// [`DumpConfig::resume_completed`] is skipped, exactly as in [`Self::run`].
    pub async fn run_parallel<C, S, F>(
        &self,
        primary: &C,
        sink: std::sync::Arc<S>,
        factory: &F,
    ) -> Result<DumpReport>
    where
        C: GenericClient,
        S: Sink + 'static,
        F: crate::export::parallel::ClientFactory,
    {
        use crate::export::parallel::{Snapshot, bind_worker_to_snapshot};

        let model = detect_hydration_model(primary).await?;
        let pgstac_version: String = primary
            .query_one("SELECT get_version() AS v", &[])
            .await?
            .get("v");
        let plans = plan_collections(
            primary,
            self.config.collection_ids.as_deref(),
            &self.config.prefilter,
        )
        .await?;

        let geoparquet_mode = match model {
            HydrationModel::BaseItem => GeoparquetMode::Buffered,
            HydrationModel::Fragment => GeoparquetMode::Stream,
        };
        let scan_filter = scan_filter_for(&self.config.prefilter);

        // Optional shared snapshot.
        let snapshot = if self.config.consistent {
            Some(Snapshot::export(factory).await?)
        } else {
            None
        };
        let snapshot_id = snapshot.as_ref().map(|s| s.id().to_string());

        // Pre-seed totals + checkpoint with resumed (already-complete) partitions
        // so the new manifest is complete and resumed files are not re-dumped.
        let mut total_items: u64 = 0;
        let mut total_partition_files: usize = 0;
        let mut checkpoint = Checkpoint {
            manifest_version: crate::export::manifest::MANIFEST_VERSION.to_string(),
            started_at: now_rfc3339(),
            completed: Vec::new(),
        };

        // Write each collection.json up front (sequential, cheap) and build the
        // flat job list. Per-collection contexts are cloned into the jobs.
        let mut collection_meta: Vec<CollectionMeta> = Vec::with_capacity(plans.len());
        let mut jobs: Vec<PartitionJob> = Vec::new();
        for (ci, plan) in plans.iter().enumerate() {
            let ctx = load_collection_context(primary, model, &plan.id).await?;
            let coll_dir = format!("collections/{}", encode_collection_dir(&plan.id));
            let coll_json = collection_json(plan);
            let coll_bytes = serde_json::to_vec_pretty(&coll_json)?;
            let coll_file = sink.put(&format!("{coll_dir}/collection.json"), &coll_bytes)?;
            collection_meta.push(CollectionMeta {
                id: plan.id.clone(),
                coll_dir: coll_dir.clone(),
                collection_file: FileEntry {
                    path: format!("{coll_dir}/collection.json"),
                    sha256: coll_file.sha256,
                    bytes: coll_file.bytes,
                    count: None,
                },
                partition_trunc: plan.partition_trunc.as_manifest_str().map(str::to_string),
                partitions: Vec::new(),
                item_count: 0,
            });
            let ctx = std::sync::Arc::new(ctx);
            for partition in &plan.partitions {
                let rel_file = format!("{coll_dir}/{}", partition.file_name);
                // Resume: carry a verified prior entry straight into the
                // manifest instead of re-dumping it.
                if let Some(prior) = self.config.resume_completed.get(&rel_file) {
                    let entry = prior.entry.clone();
                    total_items += entry.item_count;
                    total_partition_files += 1;
                    let meta = &mut collection_meta[ci];
                    meta.item_count += entry.item_count;
                    meta.partitions.push(entry);
                    checkpoint.completed.push(prior.clone());
                    continue;
                }
                jobs.push(PartitionJob {
                    collection_index: ci,
                    collection_id: plan.id.clone(),
                    partition_name: partition.name.clone(),
                    rel_file,
                    ctx: ctx.clone(),
                });
            }
        }

        // Shared job queue + worker pool. Workers own one connection each and
        // pull jobs until the queue drains.
        let concurrency = self.config.concurrency.max(1).min(jobs.len().max(1));
        let queue = std::sync::Arc::new(tokio::sync::Mutex::new(
            jobs.into_iter().collect::<std::collections::VecDeque<_>>(),
        ));
        let compression = self.config.compression;
        let skip_errors = self.config.skip_errors;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Result<JobResult>>();

        let budget = self.budget.clone();
        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let queue = queue.clone();
            let sink = sink.clone();
            let budget = budget.clone();
            let tx = tx.clone();
            let snap_id = snapshot_id.clone();
            let scan_filter = scan_filter.clone();
            let factory_fut = factory.connect();
            // Connect each worker up front so a connection failure is reported
            // before the worker loop starts.
            let client = factory_fut.await?;
            if let Some(id) = &snap_id {
                bind_worker_to_snapshot(&client, id).await?;
            }
            workers.push(tokio::spawn(async move {
                loop {
                    let job = {
                        let mut q = queue.lock().await;
                        q.pop_front()
                    };
                    let Some(job) = job else { break };
                    let outcome = dump_one_partition(
                        &client,
                        sink.as_ref(),
                        model,
                        geoparquet_mode,
                        &job.partition_name,
                        &job.collection_id,
                        &job.rel_file,
                        job.ctx.as_ref(),
                        &scan_filter,
                        compression,
                        skip_errors,
                        &budget,
                    )
                    .await;
                    let msg = outcome.map(|o| JobResult {
                        collection_index: job.collection_index,
                        outcome: o,
                    });
                    let failed = msg.is_err();
                    // If the receiver is gone the dump is aborting; stop.
                    if tx.send(msg).is_err() || failed {
                        break;
                    }
                }
            }));
        }
        drop(tx); // workers hold the remaining senders; rx closes when all finish

        // Collect results as they arrive. Checkpoint (pre-seeded with resumed
        // files above) is updated incrementally; its order is non-deterministic
        // under fan-out but only used for resume (set membership), so order does
        // not matter.
        let mut total_skipped: u64 = 0;
        let mut first_error: Option<Error> = None;

        while let Some(msg) = rx.recv().await {
            match msg {
                Ok(JobResult {
                    collection_index,
                    outcome,
                }) => {
                    total_skipped += outcome.skipped;
                    if let Some(entry) = outcome.entry {
                        total_items += entry.item_count;
                        total_partition_files += 1;
                        let meta = &mut collection_meta[collection_index];
                        checkpoint.completed.push(CheckpointEntry {
                            file: entry.file.clone(),
                            sha256: entry.sha256.clone(),
                            item_count: entry.item_count,
                            collection_id: meta.id.clone(),
                            entry: entry.clone(),
                        });
                        let cp_bytes = serde_json::to_vec_pretty(&checkpoint)?;
                        let _ = sink.put("_checkpoint.json", &cp_bytes)?;
                        meta.item_count += entry.item_count;
                        meta.partitions.push(entry);
                    }
                }
                Err(e) => {
                    // Fail-fast: keep the first error, stop accepting more.
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    break;
                }
            }
        }

        // Drain/await workers (they stop when the queue empties or rx drops).
        for w in workers {
            let _ = w.await;
        }

        if let Some(e) = first_error {
            // Leave _checkpoint.json in place so a resume can pick up.
            if let Some(s) = snapshot {
                let _ = s.finish().await;
            }
            return Err(e);
        }

        // Per-partition order within a collection is non-deterministic under
        // fan-out; sort by file name so the manifest is deterministic.
        let mut collection_entries: Vec<CollectionEntry> =
            Vec::with_capacity(collection_meta.len());
        for meta in collection_meta {
            let mut partitions = meta.partitions;
            partitions.sort_by(|a, b| a.file.cmp(&b.file));
            collection_entries.push(CollectionEntry {
                id: meta.id,
                collection_file: meta.collection_file,
                partition_trunc: meta.partition_trunc,
                item_count: meta.item_count,
                partitions,
            });
        }

        self.write_metadata_and_manifest(
            primary,
            sink.as_ref(),
            collection_entries,
            pgstac_version,
            snapshot_id,
        )
        .await?;

        if let Some(s) = snapshot {
            s.finish().await?;
        }

        Ok(DumpReport {
            collections: plans.len(),
            partitions: total_partition_files,
            items: total_items,
            skipped: total_skipped,
        })
    }

    /// Writes the root metadata files (queryables/settings), the manifest LAST,
    /// its sibling sha256, and drops the checkpoint. `snapshot_id` is recorded
    /// (informational) when the dump ran under `--consistent`.
    async fn write_metadata_and_manifest<C: GenericClient, S: Sink>(
        &self,
        client: &C,
        sink: &S,
        collection_entries: Vec<CollectionEntry>,
        pgstac_version: String,
        snapshot_id: Option<String>,
    ) -> Result<()> {
        // Root metadata files.
        let (q_json, q_count) = queryables_json(client).await?;
        let q_bytes = serde_json::to_vec_pretty(&q_json)?;
        let q_file = sink.put("queryables.json", &q_bytes)?;

        let (s_json, s_count) = settings_json(client).await?;
        let s_bytes = serde_json::to_vec_pretty(&s_json)?;
        let s_file = sink.put("settings.json", &s_bytes)?;

        let mut metadata_files = BTreeMap::new();
        let _ = metadata_files.insert(
            "queryables".to_string(),
            FileEntry {
                path: "queryables.json".to_string(),
                sha256: q_file.sha256,
                bytes: q_file.bytes,
                count: Some(q_count),
            },
        );
        let _ = metadata_files.insert(
            "settings".to_string(),
            FileEntry {
                path: "settings.json".to_string(),
                sha256: s_file.sha256,
                bytes: s_file.bytes,
                count: Some(s_count),
            },
        );

        // Manifest (written LAST).
        let dump_type = if self.is_partial() { "partial" } else { "full" };
        let snapshot = snapshot_id
            .as_ref()
            .map(|id| serde_json::json!({ "exported_snapshot": id }));
        let manifest = Manifest {
            manifest_version: crate::export::manifest::MANIFEST_VERSION.to_string(),
            created_at: now_rfc3339(),
            tool: Tool {
                name: "pgstac".to_string(),
                command: "dump".to_string(),
                version: self.config.tool_version.clone(),
            },
            source: Source {
                pgstac_version,
                postgres_version: None,
                server: self.config.server.clone(),
            },
            dump_type: dump_type.to_string(),
            filter: self.filter_for_manifest(),
            consistent: snapshot_id.is_some(),
            snapshot,
            options: Options {
                hydrated: true,
                compression: self.config.compression.as_str().to_string(),
                ordering: vec!["datetime".to_string(), "id".to_string()],
            },
            metadata_files,
            collections: collection_entries,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let _ = sink.put("manifest.json", &manifest_bytes)?;
        // Sibling integrity for the manifest itself.
        let _ = sink.put(
            "manifest.json.sha256",
            format!("{}\n", sha256_hex(&manifest_bytes)).as_bytes(),
        )?;

        // Manifest is the completion marker; drop the in-progress checkpoint.
        sink.remove("_checkpoint.json")?;

        sink.finalize()?;
        Ok(())
    }

    fn is_partial(&self) -> bool {
        self.config.collection_ids.is_some()
            || self.config.prefilter.datetime.is_some()
            || self.config.prefilter.bbox.is_some()
    }

    fn filter_for_manifest(&self) -> Option<Filter> {
        if !self.is_partial() {
            return None;
        }
        Some(Filter {
            collection_ids: self.config.collection_ids.clone().unwrap_or_default(),
            datetime: self
                .config
                .prefilter
                .datetime
                .as_ref()
                .map(|(s, e)| DatetimeRange {
                    start: s.clone(),
                    end: e.clone(),
                }),
            bbox: self.config.prefilter.bbox.map(|b| b.to_vec()),
        })
    }
}

/// Budget-gated in-memory item buffer for the 0.9.11 buffered-widening path,
/// with spill-to-disk for an oversized single partition.
///
/// Items accumulate in RAM only while a [`BudgetGuard`] reservation holds. When
/// the running estimate would exceed the global budget the buffer **spills**:
/// it serializes its accumulated items as NDJSON to a temp file and releases the
/// reservation, so later batches no longer count against RAM. At encode time a
/// spilled buffer replays its NDJSON back into the encoder. Either way the whole
/// partition is encoded once, schema complete by construction.
///
/// [`BudgetGuard`]: crate::export::budget::BudgetGuard
struct BufferedItems<'b> {
    budget: &'b MemoryBudget,
    /// Items held in memory (not yet spilled).
    items: Vec<Value>,
    /// Bytes currently reserved against the budget for `items`.
    reserved: u64,
    /// The live reservation, dropped (released) on spill or at end.
    guard: Option<crate::export::budget::BudgetGuard>,
    /// Spill file (NDJSON of already-spilled items), created on first spill.
    spill: Option<std::io::BufWriter<std::fs::File>>,
    spill_handle: Option<tempfile::NamedTempFile>,
    /// Whether anything (memory or spill) has been written.
    nonempty: bool,
}

impl<'b> BufferedItems<'b> {
    fn new(budget: &'b MemoryBudget) -> Self {
        BufferedItems {
            budget,
            items: Vec::new(),
            reserved: 0,
            guard: None,
            spill: None,
            spill_handle: None,
            nonempty: false,
        }
    }

    /// Rough per-item heap estimate used for budget accounting.
    fn estimate(item: &Value) -> u64 {
        // A serialized-length proxy; cheap and good enough to bound RAM. The
        // in-memory `Value` is larger than its text, so scale up a little.
        (serde_json::to_string(item).map(|s| s.len()).unwrap_or(256) as u64) * 2
    }

    /// Adds a batch, reserving budget; spills to disk if the reservation fails.
    fn extend(&mut self, batch: Vec<Value>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.nonempty = true;
        let need: u64 = batch.iter().map(Self::estimate).sum();

        // Try to grow the reservation to cover the new batch.
        if self.try_reserve_more(need) {
            self.items.extend(batch);
            return Ok(());
        }

        // Could not reserve: spill what we have, then keep the new batch in
        // memory under a fresh (smaller) reservation if possible, else spill it
        // too. This bounds peak RAM to roughly one batch over the budget.
        self.spill_current()?;
        if self.try_reserve_more(need) {
            self.items.extend(batch);
        } else {
            // Even one batch exceeds the budget: write it straight to spill.
            self.spill_batch(&batch)?;
        }
        Ok(())
    }

    /// Attempts to extend the reservation by `extra` bytes.
    fn try_reserve_more(&mut self, extra: u64) -> bool {
        match self.budget.try_reserve(self.reserved + extra) {
            Some(g) => {
                // Replace the old guard (releases it) with the larger one.
                self.guard = Some(g);
                self.reserved += extra;
                true
            }
            None => false,
        }
    }

    /// Spills the in-memory items to the NDJSON spill file and releases the
    /// reservation.
    fn spill_current(&mut self) -> Result<()> {
        if !self.items.is_empty() {
            let batch = std::mem::take(&mut self.items);
            self.spill_batch(&batch)?;
        }
        self.reserved = 0;
        self.guard = None; // release the reservation
        Ok(())
    }

    /// Appends a batch of items as NDJSON to the spill file (creating it lazily).
    fn spill_batch(&mut self, batch: &[Value]) -> Result<()> {
        use std::io::Write;
        if self.spill.is_none() {
            let handle = crate::export::budget::spill_file()?;
            let file = handle.reopen()?;
            self.spill_handle = Some(handle);
            self.spill = Some(std::io::BufWriter::new(file));
        }
        let w = self.spill.as_mut().expect("spill writer present");
        for item in batch {
            serde_json::to_writer(&mut *w, item)?;
            w.write_all(b"\n")?;
        }
        Ok(())
    }

    /// Encodes all buffered items (spilled + in-memory) to a geoparquet file at
    /// `out`. Returns `false` if there were no items (no file should be written).
    fn encode_to(mut self, out: &std::path::Path, compression: ParquetCompression) -> Result<bool> {
        if !self.nonempty {
            return Ok(false);
        }
        // Gather: replay any spilled NDJSON, then append in-memory items.
        let mut all: Vec<Value> = Vec::new();
        if let Some(w) = self.spill.take() {
            // Flush + reopen the spill file for reading.
            let _ = w.into_inner().map_err(std::io::Error::other)?;
            if let Some(handle) = &self.spill_handle {
                use std::io::{BufRead, BufReader};
                let f = handle.reopen()?;
                for line in BufReader::new(f).lines() {
                    let line = line?;
                    if line.is_empty() {
                        continue;
                    }
                    all.push(serde_json::from_str(&line)?);
                }
            }
        }
        all.append(&mut self.items);
        if all.is_empty() {
            return Ok(false);
        }
        let bytes = crate::export::format::encode_all(
            Format::Geoparquet {
                compression,
                mode: GeoparquetMode::Buffered,
                max_row_group_row_count: None,
            },
            all,
        )?;
        std::fs::write(out, &bytes)?;
        Ok(true)
    }
}

/// Dumps one partition to a temp geoparquet file, streams it to the sink, and
/// returns the manifest entry (None if the partition has no items).
///
/// Free function (not a method) so the parallel fan-out can run it in
/// concurrent worker tasks each owning their own connection. `budget` gates the
/// buffered (0.9.11) path: items accumulate in memory only while a reservation
/// holds; an oversized partition spills its buffered items to a temp file before
/// the final encode so peak memory stays under the global budget.
#[allow(clippy::too_many_arguments)]
async fn dump_one_partition<C: GenericClient, S: Sink>(
    client: &C,
    sink: &S,
    model: HydrationModel,
    mode: GeoparquetMode,
    partition_name: &str,
    collection: &str,
    rel_file: &str,
    ctx: &crate::hydrate::CollectionContext,
    scan_filter: &ScanFilter,
    compression: ParquetCompression,
    skip_errors: bool,
    budget: &MemoryBudget,
) -> Result<PartitionOutcome> {
    // Footprint accumulators.
    let mut footprint = Footprint::default();
    let mut item_count: u64 = 0;
    let mut skipped: u64 = 0;

    // Temp file for the encoded geoparquet -> streamed to sink (A6). The
    // NamedTempFile owns the path + cleanup; we read/write via fresh handles
    // on its path so no borrow outlives the encode (the stream writer needs
    // exclusive ownership of its File for the whole scan).
    let spill = crate::export::budget::spill_file()?;
    let spill_path = spill.path().to_path_buf();

    {
        // Buffered (0.9.11) path: accumulate items, spilling to a temp file when
        // the in-memory working set would exceed the budget. Stream (0.10) path
        // never buffers, so the budget does not apply.
        let mut buffer = BufferedItems::new(budget);
        let mut stream_writer: Option<GeoparquetStreamWriter<std::fs::File>> = match mode {
            GeoparquetMode::Stream => {
                let file = std::fs::File::create(&spill_path)?;
                Some(GeoparquetStreamWriter::new(file, compression))
            }
            GeoparquetMode::Buffered => None,
        };

        // Scan + per-batch handling. The scan callback is sync; we collect
        // footprint and feed the encoder.
        let scan_result = scan_partition(
            client,
            model,
            partition_name,
            collection,
            ctx,
            scan_filter,
            |batch| {
                let mut accepted: Vec<Value> = Vec::with_capacity(batch.len());
                for item in batch {
                    match validate_item(&item) {
                        Ok(()) => {
                            footprint.observe(&item);
                            item_count += 1;
                            accepted.push(item);
                        }
                        Err(e) => {
                            if skip_errors {
                                skipped += 1;
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                match mode {
                    GeoparquetMode::Buffered => buffer.extend(accepted)?,
                    GeoparquetMode::Stream => {
                        stream_writer
                            .as_mut()
                            .expect("stream writer present")
                            .write_batch(accepted)?;
                    }
                }
                Ok(())
            },
        )
        .await;

        let _scanned = scan_result?;

        // Finalize the encoder.
        let produced = match mode {
            GeoparquetMode::Buffered => buffer.encode_to(&spill_path, compression)?,
            GeoparquetMode::Stream => stream_writer
                .take()
                .expect("stream writer present")
                .finish()?,
        };

        if !produced {
            // Empty partition -> no file.
            return Ok(PartitionOutcome {
                entry: None,
                skipped,
            });
        }
    }

    // Stream the finished temp file to the sink (also yields sha256). `spill`
    // still owns the path until it drops at end of scope.
    let written = sink.put_file(rel_file, &spill_path)?;
    drop(spill);

    Ok(PartitionOutcome {
        entry: Some(PartitionEntry {
            name: partition_name.to_string(),
            file: rel_file.to_string(),
            sha256: written.sha256,
            bytes: written.bytes,
            item_count,
            datetime_range: footprint.datetime_range(),
            end_datetime_range: footprint.end_datetime_range(),
            bbox: footprint.bbox(),
        }),
        skipped,
    })
}

struct PartitionOutcome {
    entry: Option<PartitionEntry>,
    skipped: u64,
}

/// One partition's worth of work for the parallel pool.
struct PartitionJob {
    /// Index into `collection_meta` (which collection this partition belongs to).
    collection_index: usize,
    /// Collection id (for pre-loading the collection's fragments before the scan).
    collection_id: String,
    /// Source partition relation name.
    partition_name: String,
    /// Output path relative to the dump root.
    rel_file: String,
    /// Per-collection hydration context (base_item for 0.9.11), shared (`Arc`).
    ctx: std::sync::Arc<crate::hydrate::CollectionContext>,
}

/// A worker's result for one partition.
struct JobResult {
    collection_index: usize,
    outcome: PartitionOutcome,
}

/// Mutable per-collection accumulator for the parallel path (partition entries
/// arrive out of order, so we collect them here and assemble at the end).
struct CollectionMeta {
    id: String,
    #[allow(dead_code)]
    coll_dir: String,
    collection_file: FileEntry,
    partition_trunc: Option<String>,
    partitions: Vec<PartitionEntry>,
    item_count: u64,
}

/// Validates an item enough to encode it (must be a Feature object with an id).
fn validate_item(item: &Value) -> Result<()> {
    if !item.is_object() {
        return Err(Error::Export("item is not a JSON object".into()));
    }
    if item.get("id").and_then(Value::as_str).is_none() {
        return Err(Error::Export("item missing string id".into()));
    }
    Ok(())
}

/// Accumulates per-partition temporal + spatial footprint from streamed items.
#[derive(Debug, Default)]
struct Footprint {
    dt_min: Option<String>,
    dt_max: Option<String>,
    edt_min: Option<String>,
    edt_max: Option<String>,
    bbox: Option<[f64; 4]>,
}

impl Footprint {
    fn observe(&mut self, item: &Value) {
        let props = &item["properties"];
        let dt = props
            .get("datetime")
            .and_then(Value::as_str)
            .or_else(|| props.get("start_datetime").and_then(Value::as_str));
        if let Some(dt) = dt {
            observe_min(&mut self.dt_min, dt);
            observe_max(&mut self.dt_max, dt);
        }
        let edt = props
            .get("end_datetime")
            .and_then(Value::as_str)
            .or_else(|| props.get("datetime").and_then(Value::as_str));
        if let Some(edt) = edt {
            observe_min(&mut self.edt_min, edt);
            observe_max(&mut self.edt_max, edt);
        }
        if let Some(bbox) = item.get("bbox").and_then(Value::as_array)
            && bbox.len() >= 4
        {
            let vals: Vec<f64> = bbox.iter().filter_map(Value::as_f64).collect();
            if vals.len() >= 4 {
                // bbox may be 4 or 6 (3D): west/south are [0],[1]; east/north sit at len/2 and
                // len/2+1 (indices 2,3 for a 4-element bbox; 3,4 for a 6-element 3D bbox).
                let mid = vals.len() / 2;
                let (w, s) = (vals[0], vals[1]);
                let (e, n) = (vals[mid], vals[mid + 1]);
                self.merge_bbox(w, s, e, n);
            }
        }
    }

    fn merge_bbox(&mut self, w: f64, s: f64, e: f64, n: f64) {
        match &mut self.bbox {
            Some(b) => {
                b[0] = b[0].min(w);
                b[1] = b[1].min(s);
                b[2] = b[2].max(e);
                b[3] = b[3].max(n);
            }
            None => self.bbox = Some([w, s, e, n]),
        }
    }

    fn datetime_range(&self) -> Option<DatetimeRange> {
        match (&self.dt_min, &self.dt_max) {
            (Some(lo), Some(hi)) => Some(DatetimeRange {
                start: lo.clone(),
                end: hi.clone(),
            }),
            _ => None,
        }
    }

    fn end_datetime_range(&self) -> Option<DatetimeRange> {
        match (&self.edt_min, &self.edt_max) {
            (Some(lo), Some(hi)) => Some(DatetimeRange {
                start: lo.clone(),
                end: hi.clone(),
            }),
            _ => None,
        }
    }

    fn bbox(&self) -> Option<Vec<f64>> {
        self.bbox.map(|b| b.to_vec())
    }
}

fn observe_min(slot: &mut Option<String>, v: &str) {
    match slot {
        Some(cur) if cur.as_str() <= v => {}
        _ => *slot = Some(v.to_string()),
    }
}

fn observe_max(slot: &mut Option<String>, v: &str) {
    match slot {
        Some(cur) if cur.as_str() >= v => {}
        _ => *slot = Some(v.to_string()),
    }
}

/// Translates a plan-level [`Prefilter`] into a per-item [`ScanFilter`].
fn scan_filter_for(prefilter: &Prefilter) -> ScanFilter {
    ScanFilter {
        datetime: prefilter.datetime.clone(),
        bbox: prefilter.bbox,
    }
}

/// Percent-encodes path-unsafe characters in a collection id for use as a
/// directory name (MANIFEST.md §1). The authoritative id is always the JSON
/// `id`, never parsed from the dir name.
fn encode_collection_dir(id: &str) -> String {
    let mut out = String::with_capacity(id.len());
    for b in id.bytes() {
        let safe = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.');
        if safe {
            out.push(b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// RFC3339 (UTC, second precision) timestamp via Postgres-independent clock.
fn now_rfc3339() -> String {
    // Avoid a chrono dep: format from SystemTime.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Days/time arithmetic for a UTC RFC3339 timestamp.
    let days = now / 86_400;
    let secs_of_day = now % 86_400;
    let (h, m, s) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Days-since-epoch -> (year, month, day), Howard Hinnant's civil algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn feat(id: &str) -> Value {
        json!({
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": id,
            "collection": "c",
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
            "bbox": [1.0, 2.0, 1.0, 2.0],
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "assets": {},
            "links": []
        })
    }

    fn read_geoparquet_ids(path: &std::path::Path) -> Vec<String> {
        let f = std::fs::File::open(path).unwrap();
        let ic = stac::geoparquet::from_reader(f).unwrap();
        ic.items
            .iter()
            .map(|it| {
                serde_json::to_value(it).unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    /// Spill: a budget far smaller than the data forces the buffered
    /// path to spill to disk, yet every item is still encoded (no drops). The
    /// encode replays the spill + in-memory tail, so all ids round-trip.
    #[test]
    fn buffered_items_spills_under_tiny_budget_and_loses_nothing() {
        // A 1-byte budget: every batch fails to reserve -> everything spills.
        let budget = MemoryBudget::with_bytes(1);
        let mut buf = BufferedItems::new(&budget);
        let ids: Vec<String> = (0..50).map(|i| format!("item-{i:03}")).collect();
        // Feed in several batches.
        for chunk in ids.chunks(7) {
            let batch: Vec<Value> = chunk.iter().map(|id| feat(id)).collect();
            buf.extend(batch).unwrap();
        }
        // Budget fully released after spilling (no reservation held).
        assert_eq!(budget.used(), 0, "spill must release the reservation");
        let out = crate::export::budget::spill_file().unwrap();
        let produced = buf.encode_to(out.path(), ParquetCompression::Zstd).unwrap();
        assert!(produced, "non-empty buffer must produce a file");
        let mut got = read_geoparquet_ids(out.path());
        got.sort();
        let mut want = ids.clone();
        want.sort();
        assert_eq!(got, want, "spill+encode must preserve every item");
    }

    /// With a generous budget nothing spills; items stay in memory and encode.
    #[test]
    fn buffered_items_in_memory_when_budget_fits() {
        let budget = MemoryBudget::with_bytes(64 * 1024 * 1024);
        let mut buf = BufferedItems::new(&budget);
        buf.extend(vec![feat("a"), feat("b")]).unwrap();
        assert!(budget.used() > 0, "in-memory buffer holds a reservation");
        let out = crate::export::budget::spill_file().unwrap();
        assert!(buf.encode_to(out.path(), ParquetCompression::Zstd).unwrap());
        let mut got = read_geoparquet_ids(out.path());
        got.sort();
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn buffered_items_empty_produces_no_file() {
        let budget = MemoryBudget::with_bytes(1024);
        let buf = BufferedItems::new(&budget);
        let out = crate::export::budget::spill_file().unwrap();
        assert!(!buf.encode_to(out.path(), ParquetCompression::Zstd).unwrap());
    }

    #[test]
    fn dir_encoding() {
        assert_eq!(encode_collection_dir("landsat-c2-l2"), "landsat-c2-l2");
        assert_eq!(encode_collection_dir("a b/c"), "a%20b%2Fc");
        assert_eq!(encode_collection_dir("ok.id_1"), "ok.id_1");
    }

    #[test]
    fn footprint_min_max() {
        let mut fp = Footprint::default();
        fp.observe(&serde_json::json!({
            "properties": {"datetime": "2024-01-10T00:00:00Z"},
            "bbox": [-10.0, -5.0, 10.0, 5.0]
        }));
        fp.observe(&serde_json::json!({
            "properties": {"datetime": "2024-01-05T00:00:00Z"},
            "bbox": [-20.0, -1.0, 2.0, 8.0]
        }));
        let dr = fp.datetime_range().unwrap();
        assert_eq!(dr.start, "2024-01-05T00:00:00Z");
        assert_eq!(dr.end, "2024-01-10T00:00:00Z");
        assert_eq!(fp.bbox().unwrap(), vec![-20.0, -5.0, 10.0, 8.0]);
    }

    #[test]
    fn civil_epoch() {
        // 1970-01-01
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2000-01-01 = 10957 days after epoch
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
    }
}
