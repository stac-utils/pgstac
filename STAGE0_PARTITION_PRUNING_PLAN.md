# PgSTAC Stage-0 Partition Pruning Plan

## Problem Statement

PgSTAC needs a Stage-0 partition pruning architecture that can intercept CQL2 search filters, evaluate them against precomputed partition statistics, and build a partition-targeted `UNION ALL` query plan fully inside PostgreSQL (PL/pgSQL).

### Hard Constraint

The pruning system must **never underfetch**. If statistics are missing, stale, incomplete, or an operator is unsupported, the partition must be included (fail-open behavior).

---

## Current Approach Being Pursued

### Approach A: In-Database Zone-Map Pruning Engine (Primary)

Implement a PostgreSQL-native pruning layer that:

1. Uses `queryables` as the configuration source for which properties are zone-map tracked.
2. Stores per-partition summary stats (time, geometry, and selected property ranges/sets).
3. Traverses CQL2 filter structures and evaluates them against partition stats.
4. Produces a candidate partition list.
5. Generates dynamic SQL (`UNION ALL`) only over candidate partitions.
6. Falls back to including partitions whenever pruning confidence is not guaranteed.

Why this approach is attractive:
- Keeps all logic close to data and planner context.
- Avoids app-layer duplication of filter semantics.
- Works for any client that calls PgSTAC SQL APIs.

---

## Alternative Options and Trade-Offs

## Option B: PostgreSQL Native Planner + Constraint Exclusion Only

Rely primarily on native partition constraints and planner pruning without custom zone-map evaluation.

**Why it may be better**
- Lower maintenance burden.
- Uses built-in PostgreSQL optimizations.
- Less custom SQL/PLpgSQL complexity.

**Why it may be worse**
- Limited pruning for non-partition-key properties.
- Less control over fallback behavior tied to CQL2 semantics.
- Harder to optimize for domain-specific STAC query patterns.

## Option C: App-Layer Pre-Pruning Before SQL Execution

Implement pruning logic in API/backend code and pass selected partitions into SQL.

**Why it may be better**
- Easier to iterate with general-purpose languages and tooling.
- Potentially simpler debugging and testing workflows.

**Why it may be worse**
- Splits source of truth across app + database.
- Can drift from PgSTAC SQL semantics.
- Adds coupling to specific API implementations.

## Option D: Hybrid Pruning (Planner + Minimal SQL Hints)

Use planner constraints as baseline and add only lightweight SQL-side candidate narrowing.

**Why it may be better**
- Lower risk than a full custom pruning evaluator.
- Incremental adoption path.

**Why it may be worse**
- May deliver only partial performance gains.
- Can become an awkward middle-ground with unclear ownership boundaries.

---

## Recommended Direction

Proceed with **Approach A** (in-database zone-map pruning), but with strict fail-open semantics and staged rollout gates so correctness is protected at every phase.

---

## Fully Fleshed Execution Plan

## Phase 0: Guardrails and Baseline

### Goals
- Define correctness invariants and measurement baselines.

### Deliverables
- Explicit invariant: no underfetch across all supported filter patterns.
- Baseline benchmark set (query latency, planning time, partitions scanned).
- Feature flag(s) to disable pruning globally/per-query.

### Exit Criteria
- Baseline captured and repeatable.
- Rollback path available.

## Phase 1: Configuration and Metadata Foundation

### Goals
- Define which queryables participate in zone-map pruning.

### Deliverables
- Queryable-level tracking flag and retrieval API.
- Default tracked fields (starting with `datetime` and other low-risk primitives).
- Tests for config scoping and idempotent behavior.

### Exit Criteria
- Tracked queryables can be resolved per collection reliably.

## Phase 2: Partition Zone-Map Storage Model

### Goals
- Materialize partition-level stats for tracked properties.

### Deliverables
- Schema for per-partition min/max/set summaries as appropriate by type.
- Refresh/update hooks integrated with partition maintenance flow.
- Freshness metadata and null/unknown handling rules.

### Exit Criteria
- Stats exist and stay consistent after load/repartition operations.

## Phase 3: CQL2-to-Pruning Evaluator

### Goals
- Evaluate filter predicates against partition stats safely.

### Deliverables
- Operator support matrix (supported/unsupported).
- Evaluator functions for boolean trees (`and`, `or`, `not`) and leaf predicates.
- Strict fail-open behavior for unsupported ops or missing stats.

### Exit Criteria
- Evaluator never excludes partitions when confidence is insufficient.
- Unit tests cover mixed supported/unsupported filter trees.

## Phase 4: Dynamic UNION ALL Query Construction

### Goals
- Use candidate partition set to reduce scanned partitions.

### Deliverables
- Query builder that emits partition-targeted `UNION ALL`.
- Stable ordering/paging compatibility with existing search behavior.
- Fallback to current query path when candidate set cannot be trusted.

### Exit Criteria
- Search correctness parity with existing implementation.
- Observable partition-scan reduction on benchmark suite.

## Phase 5: Validation, Safety, and Rollout

### Goals
- Prove correctness and production readiness.

### Deliverables
- Differential test harness: compare pruned vs non-pruned result sets.
- Shadow mode metrics (compute pruning decisions without enforcing).
- Progressive rollout plan (off → shadow → partial → default-on).

### Exit Criteria
- Zero correctness regressions in differential testing.
- Performance benefit confirmed under representative workloads.

## Phase 6: Operationalization and Documentation

### Goals
- Make feature maintainable for long-term use.

### Deliverables
- Operator support docs and known fail-open cases.
- Runbooks for stale stats, disabling feature, and troubleshooting.
- Maintenance guidance for adding new tracked queryables/operators.

### Exit Criteria
- On-call/developer documentation complete.
- Feature ownership and maintenance workflow established.

---

## Risk Register and Mitigations

1. **Incorrect pruning (underfetch)**  
   - Mitigation: fail-open defaults, differential testing, shadow mode.

2. **Stale or missing stats**  
   - Mitigation: freshness metadata, conservative include-on-uncertainty behavior.

3. **Operator coverage gaps**  
   - Mitigation: explicit support matrix; unsupported operators always include.

4. **Complexity creep in PL/pgSQL evaluator**  
   - Mitigation: staged operator rollout, strict function boundaries, focused tests.

5. **Performance regressions from planner/query generation overhead**  
   - Mitigation: benchmark gates, feature flags, fast fallback path.

---

## Success Criteria

- No underfetch regressions in automated differential tests.
- Measurable reduction in partitions scanned for target query classes.
- Reduced median and tail latency for complex CQL2 searches.
- Safe operational controls (toggle, shadow mode, diagnostics) available.

---

## Current Baseline Mapping (Implemented)

- Trigger/queue maintenance path:
  - `partition_after_triggerfunc` enqueues or executes `update_partition_stats(..., true)` through `run_or_queue`.
  - Queue execution and draining flow through `run_queued_queries_intransaction`.
  - Partition stats refresh logic lives in `update_partition_stats` / `update_partition_stats_q`.
- Datetime-sort search/chunking path:
  - Chunk windows are produced by `chunker`.
  - Partition SQL fragments are generated by `partition_queries` and `partition_query_view`.
  - Iterative chunk execution for top-N fetch lives in `search_rows`.
  - Strategy routing and overrides now live in `search_rows_strategy` with
    `datetime_limit_strategy` (`chunk` / `big_union` / `hybrid`).

## Datetime + LIMIT Benchmark Track (Implemented)

- Benchmark helper: `benchmark_datetime_limit_strategies(...)`
  - Compares `chunk`, `big_union`, and `hybrid` over the same workload and limits.
  - Supports repeated rounds and top-N buckets (`small`, `medium`, `large`).
  - Captures correctness parity (`matches_chunk`) plus latency/planning/execution metrics.
  - Reports relation-touch counts from `EXPLAIN (ANALYZE, FORMAT JSON)` output.
