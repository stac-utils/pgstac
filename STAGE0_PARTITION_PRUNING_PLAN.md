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

---

## What to Do Next in a Runnable Benchmark Environment

This section is the execution manual for finishing strategy selection in an
environment where Docker/build and benchmark execution are available.

### Non-Negotiable Invariants (Must Hold for Every Run)

1. **Never underfetch**: result set from candidate strategy must be a superset
   of chunk baseline before final `LIMIT` and exactly parity-matching after
   final ordering + limit semantics.
2. **Fail-open on uncertainty**:
   - missing/stale stats
   - unsupported predicates/operators
   - runtime strategy errors
   - excessive partition-query expansion
   must route to current chunk behavior.
3. **No silent behavior drift**: strategy choice and fallback reason must be
   observable in query diagnostics.

### Strategy Set Under Test

1. `chunk` (baseline/current behavior)
2. `big_union` (single generated union path from candidate partitions)
3. `hybrid` (bounded union with threshold-based fallback to `chunk`)

All comparisons must run with identical:
- datasets
- seeds
- query matrix
- concurrency profile
- PostgreSQL settings
- queue strategy settings.

---

## Canonical Workload Matrix (Datetime + LIMIT, CPU-Intensive Filters)

Use a matrix generator and keep a checked-in workload manifest so every run is
 reproducible.

### Dimensions

1. **Filter selectivity**
   - very selective
   - moderate
   - broad
2. **CQL complexity**
   - simple conjunctive
   - mixed nested boolean
   - OR-heavy wide trees
   - NOT-heavy expressions
3. **Predicate type mix**
   - datetime range only
   - datetime + numeric properties
   - datetime + text comparisons
   - datetime + spatial + property mix
4. **Top-N sizes**
   - small: 10, 50
   - medium: 100, 250, 500
   - large: 1000, 5000
5. **Partition topology**
   - low partition count
   - medium partition count
   - high partition count
   - hot/cold skew (few hot partitions + many cold)
6. **Stats freshness states**
   - fully fresh
   - partially stale
   - mostly stale
   - missing stats subset
7. **Read/write concurrency**
   - read-only
   - moderate ingest + reads
   - heavy ingest + reads
   - delayed queue drain / backlog pressure
8. **Queue mode**
   - `sync`
   - `async`
   - `adaptive`

### Minimum Matrix Size

- At least 10 query templates per complexity tier.
- At least 3 independent seeds per dataset shape.
- At least 10 benchmark rounds per (strategy, query, seed) tuple after warmup.

---

## Benchmark Protocol (Step-by-Step)

### Step 1: Environment Control

1. Pin CPU/memory limits and Postgres config.
2. Disable unrelated background jobs.
3. Record:
   - git SHA
   - PostgreSQL version
   - extension versions
   - settings diff from defaults.

### Step 2: Dataset Build and Validation

1. Build dataset from manifest + seed.
2. Verify partition counts and skew expectations.
3. Verify stats freshness state for the target scenario.
4. Snapshot queue state (count + oldest age).

### Step 3: Safety Precheck

For each query in matrix:
1. Run baseline chunk query and capture deterministic ordered IDs.
2. Run candidate strategy query and compare IDs.
3. If mismatch:
   - classify (`underfetch`, `overfetch-before-limit`, ordering drift, tie drift)
   - force scenario to fail gate.

### Step 4: Timed Runs

1. Warmup runs (discarded from summary).
2. Measured rounds with randomized query order to reduce temporal bias.
3. Collect per round:
   - total latency
   - planning time
   - execution time
   - rows scanned
   - partitions touched/scanned
   - fallback occurrence + reason
   - queue depth/age before and after.

### Step 5: Stress Runs

Run dedicated stress profiles:
1. delayed queue drain
2. lock contention windows
3. partial stats freshness
4. high OR-width predicates
5. high partition count with small top-N.

### Step 6: Confidence Evaluation

Use statistical checks:
1. compute p50/p95 per strategy per workload slice
2. compare against baseline with confidence intervals
3. require improvement consistency across seeds and rounds
4. reject if wins rely on outlier slices only.

---

## Decision Scorecard (Hard Gates First, Ranking Second)

### Hard Rejection Gates

Reject a strategy immediately if any are true:
1. any confirmed underfetch
2. fallback paths do not preserve baseline correctness
3. unstable behavior under stale/missing stats scenarios
4. queue/ingest interference exceeds defined operational guardrails
5. planner overhead dominates and produces p95 regressions in primary workloads.

### Ranking Metrics (Only After Passing Gates)

Primary:
1. p95 latency (datetime+limit slices)
2. partitions scanned/touched

Secondary tie-breakers:
1. p50 latency
2. planning overhead
3. operational complexity (configuration burden + failure surface)
4. rollback simplicity.

### Confidence Rule

If no strategy demonstrates stable advantage with confidence, keep `chunk`
default and retain candidate paths behind explicit opt-in toggles.

---

## Rollout Playbook

### Stage 0: `off`
- Default strategy = `chunk`.
- Collect only baseline diagnostics.

### Stage 1: `shadow`
- Compute candidate strategy decisions but execute `chunk`.
- Log strategy would-have-chosen and fallback reasons.

### Stage 2: `limited`
- Enable candidate strategy for small allowlisted query classes/tenants.
- Keep global instant rollback to `chunk`.

### Stage 3: `default`
- Promote chosen strategy to default only after sustained green scorecard.
- Keep per-query override and global kill switch permanently.

### Required Controls

1. Global strategy toggle (`datetime_limit_strategy`).
2. Per-query override (`_search.conf.datetime_limit_strategy`).
3. Hybrid threshold controls for bounded union.
4. Diagnostics fields:
   - selected strategy
   - fallback reason
   - partition candidate count
   - executed partition count
   - queue backlog/age snapshot.

---

## Observability and Diagnostics Requirements

Add or verify query-level diagnostics contain:
1. `strategy_selected`
2. `strategy_effective` (after fallback)
3. `fallback_reason` (if any)
4. `candidate_partition_count`
5. `executed_partition_count`
6. `planning_ms`
7. `execution_ms`
8. `queue_depth`
9. `queue_oldest_age_ms`
10. `top_n_bucket`.

All fields should be machine-parseable for benchmark aggregation.

---

## Regression Benchmark Suite (Persistent)

Create a persistent suite focused on datetime-sorted top-N:
1. fixed manifest + seeds
2. includes chunk vs big_union vs hybrid
3. includes stale/missing stats profiles
4. includes ingest/read concurrency profile
5. includes OR-heavy and CPU-intensive filters
6. emits trendable JSON artifacts.

CI policy:
1. run core subset on every PR (fast slices)
2. run full matrix on schedule/manual trigger
3. fail PR only on correctness regressions by default
4. flag performance drift with thresholds and maintainers review gate.

---

## Final Decision Record Template

When benchmark execution is complete, produce a decision record containing:

1. **Context**
   - problem statement
   - candidate strategies
   - constraints/invariants
2. **Method**
   - dataset manifest and seeds
   - matrix dimensions
   - run counts and confidence method
3. **Results**
   - correctness parity summary
   - p50/p95 by slice
   - planning/execution overhead
   - partitions scanned/touched deltas
   - ingest/queue impact
4. **Rejected Alternatives**
   - why each was rejected (with evidence)
5. **Chosen Strategy**
   - rationale tied to scorecard gates
6. **Rollout Decision**
   - stages, controls, rollback
7. **Follow-ups**
   - instrumentation gaps
   - threshold tuning
   - future operator coverage.

---

## Immediate Next Actions Checklist

1. Add benchmark manifest file for query matrix + seeds.
2. Add diagnostics payload fields for strategy/fallback/partition counts.
3. Add stress profiles for queue delay and lock contention.
4. Add aggregation script/report format for p50/p95 + parity summaries.
5. Execute staged benchmark rounds in a stable environment.
6. Publish decision record and keep regression suite in CI/scheduled runs.
