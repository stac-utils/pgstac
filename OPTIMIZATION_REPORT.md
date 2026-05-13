# CQL2 cached partition-stat pruning optimization report

Repository: `/home/runner/work/pgstac/pgstac`  
Branch observed: `copilot/continue-implement-zone-map-partition-pruning`  
Generated: 2026-05-13

## Corrected goal

The target is not just generic temporal/spatial zone-map pruning. The target is to quickly determine which item partitions **could be touched by an arbitrary CQL2 filter** using cached per-partition statistics, and to do it faster and with fewer locks than PostgreSQL partition pruning plus constraint exclusion.

There are two execution modes to optimize:

1. **Queries not sorted by datetime**: cached partition stats should primarily make planning faster by producing a candidate partition set without forcing PostgreSQL to reason through many child table constraints.
2. **Queries sorted by datetime with a limit**: cached partition stats should also guide datetime-ordered chunking so expensive CQL2 filters can short-circuit after enough rows are found.

Meaningful benchmark targets: candidate partition lookup should be at least 2x faster than the EXPLAIN/constraint-pruning baseline for non-datetime sorts, and datetime-sorted LIMIT queries should be able to identify the first candidate chunks in single-digit milliseconds on the benchmark fixture. An expensive CQL2 filter is any residual filter that requires item JSON/property extraction, spatial predicates, or low-selectivity scans across many partitions after candidate selection.

The current implementation still relies on `/home/runner/work/pgstac/pgstac/src/pgstac/sql/004_search.sql` `chunker()`, which runs `EXPLAIN (format json)` against `items` and extracts planned relation names. That means it still leans on partition pruning and constraint exclusion. The proposed direction is to make partition candidacy a data lookup against cached statistics, not a planning side effect.

## Current-state observations

- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/003b_partitions.sql` has `partition_stats(partition, dtrange, edtrange, spatial, last_updated, keys)`, which is already the natural home for cached partition-level statistics.
- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/004_search.sql` has `search_wheres.partitions`, but the current pruning flow does not populate/use it as the primary partition candidate cache.
- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/002b_cql.sql` converts CQL2 JSON to SQL, but there is no companion planner that converts CQL2 into partition-stat predicates.
- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/002a_queryables.sql` already understands queryables, wrappers, and indexes; this should guide which per-partition stats are feasible for CQL2 pruning.
- Constraint maintenance currently adds lock and validation overhead. The desired design should reduce reliance on per-partition CHECK constraints for search planning, while keeping correctness by treating stats as safe over-approximations.

## Scoring criteria

Each approach is scored 1-5 for:

- **Impact**: expected improvement in candidate partition discovery, planning time, or datetime-limit short-circuiting.
- **Complexity**: implementation simplicity; 5 is easiest.
- **Risk**: correctness and operational safety; 5 is safest.
- **Cost**: engineering/runtime cost; 5 is cheapest.

Overall score is qualitative, weighted toward near-term relevance to CQL2 cached-stat pruning.

## Ranked optimization options

| Rank | Approach | Impact | Complexity | Risk | Cost | Overall | Rationale |
|---:|---|---:|---:|---:|---:|---:|---|
| 1 | Build a CQL2-to-partition-stat predicate planner | 5 | 3 | 4 | 3 | 4.25 | Core missing piece: translate supported CQL2 clauses into safe over-approximate predicates on cached stats. |
| 2 | Extend `partition_stats` with per-queryable min/max ranges | 5 | 3 | 4 | 3 | 4.20 | Numeric/date CQL2 filters become metadata lookups instead of child-table planning. |
| 3 | Add categorical value summaries per partition | 5 | 3 | 4 | 3 | 4.15 | Equality/IN filters can prune partitions by cached value sets. |
| 4 | Use cached stats to return ordered candidate partitions for datetime DESC/ASC | 5 | 4 | 4 | 4 | 4.50 | Directly addresses sorted-by-datetime LIMIT short-circuiting. |
| 5 | Populate and use `search_wheres.partitions` for normalized CQL2 filters | 5 | 4 | 4 | 4 | 4.45 | Existing table can cache candidate partitions after first computation. |
| 6 | Replace EXPLAIN-based `chunker()` with stats-backed candidate lookup | 5 | 3 | 4 | 3 | 4.20 | Removes dependence on planner partition pruning and constraint exclusion. |
| 7 | Use stats-backed chunks only when sort starts with datetime | 5 | 4 | 5 | 4 | 4.60 | Keeps behavior narrow and safer while optimizing the high-value LIMIT case. |
| 8 | For non-datetime sorts, pass candidate partition list into query generation | 4 | 4 | 4 | 4 | 4.00 | Improves planning by reducing child relations considered. |
| 9 | Make partition-stat predicates conservative with explicit false-positive tolerance | 5 | 4 | 5 | 4 | 4.60 | Ensures stats pruning never drops possible matches. |
| 10 | Track stat freshness and dirty partitions | 4 | 3 | 4 | 3 | 3.75 | Required if constraints are no longer the planning source of truth. |
| 11 | Add queryable stats capability metadata | 4 | 4 | 5 | 4 | 4.25 | Records which queryables have min/max, enum, bloom, spatial, or unsupported summaries. |
| 12 | Normalize CQL2 AST before hashing candidate sets | 4 | 4 | 4 | 5 | 4.15 | Improves cache hit rates for equivalent filters. |
| 13 | Add per-partition spatial bbox/geometry extent to candidate planner | 4 | 4 | 4 | 4 | 4.00 | Spatial CQL2 filters can use cached extents before item geometry. |
| 14 | Add GiST indexes over stats ranges and spatial extents | 4 | 4 | 4 | 4 | 4.00 | Makes stats table lookups scale with many partitions. |
| 15 | Add GIN indexes over categorical stat arrays | 4 | 4 | 4 | 4 | 4.00 | Helps platform/constellation/instrument equality filters. |
| 16 | Use bloom filters for high-cardinality categorical queryables | 4 | 2 | 3 | 2 | 3.10 | Useful for IDs/tags but requires probabilistic false-positive handling. |
| 17 | Store top-N most common values per partition | 3 | 3 | 4 | 3 | 3.35 | Better than simple arrays for bounded summary storage. |
| 18 | Store value-count sketches per partition | 3 | 2 | 3 | 2 | 2.85 | Helps selectivity estimation but is more complex. |
| 19 | Store nullability/has-value flags per queryable | 3 | 4 | 5 | 5 | 4.00 | Cheap pruning for `is null`, `is not null`, and property existence clauses. |
| 20 | Represent unsupported CQL2 clauses as no-op metadata predicates | 4 | 5 | 5 | 5 | 4.65 | Preserves correctness while allowing partial pruning. |
| 21 | Split CQL2 into metadata-prunable and residual SQL filters | 5 | 3 | 5 | 3 | 4.30 | Candidate stats prune first; original CQL2 SQL still enforces correctness. |
| 22 | Return pruning diagnostics with reasons per partition | 3 | 4 | 5 | 4 | 3.85 | Critical for validating false positives/negatives during rollout. |
| 23 | Add benchmark-only truth comparison against actual item filter results | 5 | 4 | 5 | 4 | 4.60 | Verifies stats candidates are supersets of true touched partitions. |
| 24 | Add SQL helper returning candidate partition names, not query text | 5 | 4 | 4 | 4 | 4.40 | Clean API boundary for search, benchmark, and tests. |
| 25 | Wire existing `partition_queries(..., partitions text[])` parameter | 4 | 4 | 4 | 4 | 4.00 | Existing signature hints at intended candidate-list support. |
| 26 | Generate datetime chunks from stats dtrange/edtrange order | 5 | 4 | 4 | 4 | 4.45 | Replaces planner-derived `partition_steps` for sorted LIMIT searches. |
| 27 | Use cumulative estimated row counts to size chunks | 4 | 3 | 4 | 3 | 3.75 | Avoids too many tiny partitions per chunk or too few rows per chunk. |
| 28 | Adaptive chunk size based on observed residual filter selectivity | 4 | 2 | 3 | 2 | 3.05 | Improves short-circuiting after runtime feedback. |
| 29 | Cache successful chunk progress for repeated datetime searches | 3 | 3 | 3 | 3 | 3.00 | Useful for tile/page repetition but less core than candidate lookup. |
| 30 | Use collection filter as the first stats-table predicate | 4 | 5 | 5 | 5 | 4.60 | Very cheap and safe when collections are present. |
| 31 | Derive collection candidates from CQL2 collection predicates | 3 | 3 | 4 | 3 | 3.35 | Handles filters that encode collection outside STAC `collections`. |
| 32 | Keep constraints optional for ingestion validation but not search planning | 4 | 3 | 4 | 3 | 3.75 | Reduces lock pressure while retaining admin safety if desired. |
| 33 | Add setting to disable constraint rewrite in `update_partition_stats` | 4 | 4 | 4 | 4 | 4.00 | Immediate path to avoid constraint-management locks. |
| 34 | Make `istrigger=true` stats updates refresh cached stats only | 4 | 4 | 4 | 4 | 4.00 | Aligns ingest-trigger behavior with stats-first pruning. |
| 35 | Store stats in JSONB by queryable name | 4 | 3 | 3 | 4 | 3.55 | Flexible schema, but slower than typed columns for hot queryables. |
| 36 | Store stats in typed side table `(partition, queryable, stat_type, value)` | 4 | 3 | 4 | 3 | 3.75 | Extensible and indexable; more joins. |
| 37 | Promote hot queryables to typed generated columns in stats table | 4 | 3 | 4 | 3 | 3.75 | Faster lookups for common filters like cloud cover/platform. |
| 38 | Use queryable definitions to choose stat extractors automatically | 4 | 2 | 4 | 2 | 3.30 | Reduces manual configuration but must respect wrappers. |
| 39 | Maintain stats incrementally from item statement triggers | 4 | 2 | 3 | 2 | 3.05 | Avoids full rescans but adds concurrency complexity. |
| 40 | Queue asynchronous stat recomputation for dirty partitions | 4 | 3 | 3 | 3 | 3.45 | Avoids write-path locks while keeping stats eventually fresh. |
| 41 | Add stat staleness fallback to EXPLAIN/constraint path | 4 | 3 | 5 | 3 | 3.95 | Safe rollout path when cached stats are missing. |
| 42 | Persist candidate partition arrays by search hash and stats epoch | 5 | 3 | 4 | 3 | 4.10 | Prevents stale cache hits after partition stats change. |
| 43 | Add global stats epoch increment on partition stat refresh | 3 | 4 | 4 | 4 | 3.65 | Simple invalidation primitive. |
| 44 | Add per-partition stats version for partial cache invalidation | 3 | 3 | 4 | 3 | 3.30 | More precise invalidation at higher implementation cost. |
| 45 | Use `EXPLAIN` only as benchmark/control path | 3 | 5 | 5 | 5 | 4.00 | Keeps old behavior available while moving production toward stats. |
| 46 | Benchmark planning time separately from execution time | 4 | 5 | 5 | 5 | 4.60 | Separates non-datetime-sort benefit from execution benefit. |
| 47 | Benchmark datetime LIMIT short-circuit by chunks touched | 5 | 5 | 5 | 5 | 5.00 | Directly measures the stated sorted-query goal. |
| 48 | Benchmark false positives and false negatives per CQL2 operator | 5 | 4 | 5 | 4 | 4.60 | Proves safety and quality of candidate pruning. |
| 49 | Start with `and`, comparison, `between`, `=`, `in`, spatial intersects | 5 | 4 | 5 | 4 | 4.60 | Covers high-value CQL2 subset with manageable semantics. |
| 50 | Treat `or` as union of candidate sets | 4 | 3 | 5 | 3 | 3.95 | Correct and useful, but can broaden candidates. |
| 51 | Treat `not` as unprunable unless a safe complement exists | 4 | 5 | 5 | 5 | 4.55 | Avoids unsafe negative pruning. |
| 52 | Use De Morgan normalization only for safe supported cases | 3 | 2 | 4 | 2 | 2.95 | Could improve pruning but easy to get wrong. |
| 53 | Add admin function to explain candidate-pruning plan for CQL2 | 3 | 3 | 5 | 3 | 3.45 | Operational visibility for users tuning queryables. |
| 54 | Integrate candidate stats with `where_stats` count estimates | 3 | 3 | 4 | 3 | 3.30 | Makes context/count planning aware of stats candidates. |
| 55 | Use candidate partitions to restrict actual count queries | 4 | 3 | 4 | 3 | 3.75 | Improves expensive count paths after pruning is correct. |

## Top 5 approaches selected for benchmarking now

1. **Stats-backed CQL2 candidate lookup**: materialize per-partition stats for CQL2-filterable properties and measure candidate lookup latency versus `chunker()`.
2. **Residual-safe CQL2 split**: benchmark metadata-prunable clauses while preserving the full SQL filter for correctness.
3. **Datetime DESC/ASC chunk ordering from cached stats**: measure how quickly stats can produce the first candidate chunks for sorted `LIMIT` queries.
4. **Candidate-set cache keyed by normalized CQL2 + collections + datetime + geometry + order**: measure warm repeated-query planning latency.
5. **Truth/candidate quality metrics**: compare cached candidates to actual touched partitions to measure false positives and guard against false negatives.

## Benchmark scaffold updates

Files:

- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/run_benchmark.sh`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/benchmark_zone_map_pruning.py`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/benchmark_config.json`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/README.md`

The benchmark now creates a benchmark-only cached stats table:

```text
bench_partition_cql2_stats(
  partition,
  collection,
  dtrange,
  edtrange,
  spatial,
  cloud_cover_range,
  platforms,
  row_count
)
```

It loads synthetic STAC items with CQL2-filterable properties:

- `eo:cloud_cover` for numeric range pruning,
- `platform` for categorical pruning,
- `datetime` / `end_datetime` for temporal pruning and chunk ordering,
- geometry for spatial candidate pruning.

Benchmark variants:

1. `baseline_explain_constraint_pruning`: current `pgstac.chunker(where)` using planner/constraint pruning.
2. `cached_cql2_stats_candidates`: count candidate partitions from cached CQL2 stats.
3. `cached_cql2_stats_candidate_rows`: estimate candidate row volume from cached stats.
4. `cached_cql2_stats_datetime_desc_chunks`: first datetime-desc candidate chunks for sorted `LIMIT` short-circuiting.
5. `cached_cql2_stats_datetime_asc_chunks`: first datetime-asc candidate chunks.
6. `cached_partition_set_for_cql2`: warm candidate-set cache keyed by normalized CQL2 inputs.

## Ready-to-run instructions

```bash
cd /home/runner/work/pgstac/pgstac
benchmarks/zone_map_partition_pruning/run_benchmark.sh
```

With a custom config:

```bash
cd /home/runner/work/pgstac/pgstac
benchmarks/zone_map_partition_pruning/run_benchmark.sh /absolute/path/to/benchmark_config.json
```

Default output:

```text
/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/results/zone_map_partition_pruning_results.json
```

Platform note: an earlier baseline `scripts/test --formatting` attempt was blocked by Docker image build dependency resolution for `apt.postgresql.org`. The benchmark scaffold is ready for a Docker-capable platform where PgSTAC image dependencies are reachable.

## Benchmark interpretation plan

For non-datetime sorts, focus on:

- candidate lookup latency versus `baseline_explain_constraint_pruning`,
- number of candidate partitions,
- estimated candidate rows,
- cache warm/cold delta.

For datetime-sorted `LIMIT` queries, focus on:

- time to produce first N candidate chunks,
- number of chunks needed before enough rows are expected,
- candidate row volume in first chunks versus all candidates,
- whether stats ordering matches the desired datetime direction.

Correctness gates before production SQL changes:

- cached candidates must be a superset of true touched partitions,
- unsupported CQL2 clauses must become residual filters, not unsafe pruning predicates,
- stale/missing stats must fall back to broader candidates or the existing planner path,
- candidate cache keys must include stats epoch/version before production use.

## Recommended next implementation sequence

1. Run the updated benchmark scaffold on a runnable Docker platform.
2. Add truth-comparison output: actual partitions with matching rows versus cached candidate partitions.
3. Prototype a SQL helper that accepts normalized CQL2, collections, datetime range, geometry, order, and limit, and returns candidate partition names plus pruning reasons.
4. Wire that helper into `partition_queries()` using its existing `partitions` argument.
5. Add stats epoch invalidation for cached candidate arrays.
6. Add SQL tests for supported CQL2 operators, unsupported residual filters, datetime DESC/ASC chunking, stale stats fallback, and zero-match filters.
