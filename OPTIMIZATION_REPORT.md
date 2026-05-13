# Zone-map partition pruning optimization report

Repository: `/home/runner/work/pgstac/pgstac`  
Branch observed: `copilot/continue-implement-zone-map-partition-pruning`  
Generated: 2026-05-13

## Current-state summary

The current branch has no local uncommitted changes at the start of this continuation. The current pruning path is centered on:

- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/004_search.sql`: `chunker()` runs `EXPLAIN (format json) SELECT 1 FROM items WHERE ...`, extracts planned relation names, and joins them to `partition_steps`; `partition_queries()` converts those chunks into per-time-window SQL.
- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/006_tilesearch.sql`: `geometrysearch()` calls `partition_queries()` and opens one cursor per generated query.
- `/home/runner/work/pgstac/pgstac/src/pgstac/sql/003b_partitions.sql`: `partition_stats`, `partition_sys_meta`, `partitions_view`, and `partition_steps` already contain the raw ingredients for zone-map pruning: partition datetime ranges, end-datetime ranges, and estimated spatial extents.

Main opportunity: replace or augment the current planner/EXPLAIN-discovery path with direct metadata lookups against zone-map-like partition metadata, then use those lookups to reduce planning overhead, reduce false-positive partitions, and prepare better per-partition execution order.

## Scoring criteria

Each approach is scored 1-5 for:

- **Impact**: expected improvement in pruning latency, query planning overhead, or end-to-end search latency.
- **Complexity**: implementation simplicity; 5 is easiest.
- **Risk**: compatibility and correctness safety; 5 is safest.
- **Cost**: engineering and operational cost; 5 is cheapest.

Overall score is a qualitative decision score from the same criteria, with extra weight on near-term relevance to the current branch's zone-map partition-pruning problem.

## Ranked optimization options

| Rank | Approach | Impact | Complexity | Risk | Cost | Overall | Rationale |
|---:|---|---:|---:|---:|---:|---:|---|
| 1 | Direct temporal pruning from `partition_sys_meta.constraint_dtrange` | 5 | 5 | 5 | 5 | 5.00 | Avoids EXPLAIN for datetime-selective searches using metadata that already exists. |
| 2 | Direct temporal pruning from `partition_stats.dtrange` with GiST index | 5 | 4 | 4 | 4 | 4.40 | Uses persisted min/max ranges and index lookup; needs stats freshness checks. |
| 3 | Add GiST index on `partition_stats.spatial` for spatial zone maps | 5 | 4 | 4 | 4 | 4.40 | Enables partition-level bbox filtering before item-level spatial predicates. |
| 4 | Two-phase temporal-then-spatial pruning | 5 | 4 | 4 | 4 | 4.40 | Combines high-selectivity temporal pruning with spatial extent filtering. |
| 5 | Cache pruned partition sets by normalized search hash | 4 | 4 | 4 | 5 | 4.15 | Strong repeated-query win; uses existing search hash/cache concepts. |
| 6 | Parse STAC datetime into a separate `tstzrange` argument before building raw SQL | 4 | 4 | 5 | 4 | 4.15 | Avoids reverse-parsing SQL WHERE text and improves correctness. |
| 7 | Replace `partition_steps` materialized view refresh with incremental updates | 4 | 4 | 4 | 4 | 4.00 | Removes full refresh work on each stats/partition update. |
| 8 | Batch compile pruned partitions into one `UNION ALL` query | 4 | 4 | 4 | 4 | 4.00 | Reduces PL/pgSQL loop and cursor overhead in search paths. |
| 9 | Spatial bbox denormalization into minx/miny/maxx/maxy numeric columns | 4 | 4 | 4 | 4 | 4.00 | Cheaper comparisons than geometry calls; good for broad bbox filters. |
| 10 | Add generated/cached partition lower/upper timestamp columns | 4 | 4 | 4 | 4 | 4.00 | Makes btree/BRIN-friendly pruning possible without range operator overhead. |
| 11 | Store pruned partitions in `search_wheres.partitions` | 4 | 4 | 4 | 4 | 4.00 | The column exists but is not populated by current pruning flow. |
| 12 | Collection-first partition candidate lookup | 4 | 5 | 5 | 5 | 4.60 | Very safe for searches constrained by collections; can reduce metadata scan width. |
| 13 | Create composite index on `(collection, dtrange)` equivalent metadata | 4 | 4 | 5 | 4 | 4.25 | Optimizes the common collection + datetime query pattern. |
| 14 | Use `edtrange` alongside `dtrange` in pruning | 4 | 4 | 4 | 4 | 4.00 | Better overlap pruning for intervals where `datetime` and `end_datetime` differ. |
| 15 | Fast path for exact collection + month/year partition_trunc | 4 | 4 | 5 | 4 | 4.25 | Directly maps date windows to partition names for common partition schemes. |
| 16 | Normalize and hash WHERE clauses before cache lookup | 3 | 4 | 4 | 5 | 3.75 | Increases cache hits for semantically identical searches. |
| 17 | Add partition-pruning instrumentation table | 3 | 5 | 5 | 5 | 4.00 | Low-risk observability to guide future tuning and regression detection. |
| 18 | Benchmark-only planner overhead isolation for `chunker()` | 3 | 5 | 5 | 5 | 4.00 | Does not optimize directly, but is essential to measure EXPLAIN removal. |
| 19 | Adaptive temporal-first vs spatial-first strategy selection | 4 | 3 | 3 | 3 | 3.45 | Helpful across workloads but requires reliable selectivity estimates. |
| 20 | Spatial-first pruning for tile searches | 4 | 3 | 4 | 3 | 3.65 | Tile searches always have a geometry; can reduce temporal work on small tiles. |
| 21 | Cache tile envelope geometries by z/x/y | 3 | 4 | 5 | 5 | 3.95 | Reduces repeated geometry construction in `xyzsearch()`. |
| 22 | Add `&&` bbox predicate before `ST_Intersects` in generated spatial filters | 4 | 5 | 5 | 5 | 4.60 | Safe and often lets GiST indexes short-circuit before exact spatial tests. |
| 23 | Maintain partition_stats by statement-level delta tracking | 4 | 3 | 3 | 3 | 3.45 | Reduces stats refresh cost but adds trigger/state complexity. |
| 24 | Defer stats refresh and mark partitions dirty | 4 | 3 | 3 | 4 | 3.60 | Improves ingest throughput; pruning must tolerate stale metadata. |
| 25 | Background refresh of dirty partition zone maps | 3 | 3 | 3 | 3 | 3.00 | Operationally useful but needs worker/process design. |
| 26 | Use approximate spatial extent during ingest, exact extent on maintenance | 3 | 4 | 3 | 4 | 3.45 | Faster loads with acceptable pruning false positives. |
| 27 | Add false-positive feedback to zone-map stats | 3 | 3 | 4 | 3 | 3.30 | Learns partitions that pass zone-map but return no rows. |
| 28 | Per-query partition limit ordering by recency | 3 | 4 | 4 | 4 | 3.65 | Helps top-N datetime-desc searches return early. |
| 29 | Order partitions by estimated row count for high-limit searches | 3 | 4 | 4 | 4 | 3.65 | Can improve scan efficiency when result ordering is flexible. |
| 30 | Add BRIN index on partition lower/upper timestamp columns | 3 | 4 | 5 | 4 | 3.85 | Low-maintenance option for large metadata tables. |
| 31 | Range partition metadata table independent of `pg_partition_tree()` | 4 | 3 | 3 | 3 | 3.45 | Avoids catalog traversal but duplicates partition truth. |
| 32 | Materialize parsed collection and partition bounds in `partition_stats` | 4 | 4 | 4 | 4 | 4.00 | Makes metadata lookups cheaper than calling helper functions repeatedly. |
| 33 | Avoid repeated `get_tstz_constraint()` calls in views | 3 | 4 | 4 | 4 | 3.60 | Current views call constraint parsing multiple times per partition. |
| 34 | Memoize `get_tstz_constraint()` results during one query | 3 | 3 | 4 | 3 | 3.30 | Saves catalog/regex work but may be harder in SQL-only implementation. |
| 35 | Replace regex constraint parsing with structured metadata | 4 | 3 | 4 | 3 | 3.65 | Better robustness; requires migration of constraint-derived state. |
| 36 | Add SQL function returning partition names instead of SQL text | 4 | 4 | 4 | 4 | 4.00 | Separates pruning from query generation and simplifies benchmarks. |
| 37 | Return pruning diagnostics with partition names and reasons | 3 | 4 | 5 | 4 | 3.85 | Useful to verify correctness and explain false positives. |
| 38 | Use temporary table of candidate partitions per search | 3 | 4 | 4 | 4 | 3.65 | Can simplify joins and reuse candidates across count/search/tile phases. |
| 39 | Use array of partition names to constrain generated SQL | 4 | 3 | 4 | 3 | 3.65 | Makes the existing `partitions` argument useful in `partition_queries()`. |
| 40 | Rewrite `partition_queries()` to honor its `partitions` parameter | 4 | 4 | 4 | 4 | 4.00 | Existing signature has a parameter that is not used in the current flow. |
| 41 | Push LIMIT into partition candidate generation | 3 | 3 | 4 | 3 | 3.30 | Avoids generating all partitions when top-N can stop early. |
| 42 | Use cursor-free set-returning execution for small partition sets | 3 | 3 | 4 | 3 | 3.30 | Reduces PL/pgSQL cursor overhead in `geometrysearch()`. |
| 43 | Use prepared dynamic SQL templates for common orderings | 3 | 3 | 4 | 3 | 3.30 | Reduces string formatting overhead for repeated datetime asc/desc patterns. |
| 44 | Canonicalize geometry filters to bbox for pruning metadata | 3 | 4 | 4 | 4 | 3.65 | Uses cheap bbox for candidate partitions, exact geometry later. |
| 45 | Store spatial extent as `box2d` or `box3d` text/numeric cache | 3 | 4 | 4 | 4 | 3.65 | Smaller metadata and cheaper comparisons than full geometry. |
| 46 | Tile-grid partition affinity cache | 3 | 3 | 3 | 3 | 3.00 | Caches z/x/y to partitions; best for map tile workloads. |
| 47 | Hierarchical spatial grid over partition extents | 4 | 2 | 3 | 2 | 3.00 | Potentially high impact but more invasive than GiST. |
| 48 | Learned partition prediction from search history | 3 | 1 | 2 | 1 | 2.15 | Interesting but high-risk and unnecessary before deterministic zone maps. |
| 49 | Bloom filter per partition for property filters | 3 | 2 | 3 | 2 | 2.75 | Useful for categorical CQL filters but adds storage and maintenance. |
| 50 | Min/max zone maps for selected numeric queryables | 4 | 2 | 3 | 2 | 3.15 | Extends pruning beyond time/space but requires queryable-specific maintenance. |
| 51 | Per-partition tsvector statistics for full-text `q` pruning | 2 | 2 | 3 | 2 | 2.35 | Narrower workload; likely lower ROI than temporal/spatial. |
| 52 | Use Postgres extended statistics on partition metadata columns | 2 | 4 | 5 | 4 | 3.45 | Helps planner estimate metadata queries; not a full pruning replacement. |
| 53 | Partition metadata vacuum/analyze scheduling | 2 | 5 | 5 | 5 | 3.80 | Operational hygiene for stable plans. |
| 54 | Parallel partition execution after pruning | 4 | 2 | 3 | 2 | 3.15 | End-to-end win, but bigger execution architecture change. |
| 55 | FDW/foreign partition metadata cache | 2 | 1 | 2 | 1 | 1.65 | Overkill unless partition metadata is externalized. |

## Top 5 approaches selected for benchmarking

1. **Direct temporal pruning from `partition_sys_meta.constraint_dtrange`**: validates the simplest EXPLAIN-free path using current metadata.
2. **Direct temporal pruning from `partition_stats.dtrange` with GiST index**: tests persisted zone-map ranges and exposes stats freshness needs.
3. **Spatial zone-map pruning with GiST on `partition_stats.spatial`**: measures whether partition extents reduce tile/geometry fan-out.
4. **Two-phase temporal + spatial pruning**: measures the combined candidate reduction factor for realistic STAC searches.
5. **Cached pruned partition sets keyed by normalized search inputs**: measures warm-cache performance for repeated map/tile/search workloads.

## Benchmark scaffold added

Files added:

- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/run_benchmark.sh`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/benchmark_zone_map_pruning.py`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/benchmark_config.json`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/README.md`
- `/home/runner/work/pgstac/pgstac/benchmarks/zone_map_partition_pruning/results/` for generated JSON output

The benchmark script creates an isolated database, installs PgSTAC from `/opt/src/pgstac/pgstac.sql`, loads month-partitioned fixture data, refreshes partition stats, creates benchmark-only metadata indexes, and times these variants:

1. `baseline_explain_chunker`: current `pgstac.chunker(where_text)` path.
2. `direct_constraint_temporal`: direct `partition_sys_meta` range overlap.
3. `partition_stats_temporal`: direct `partitions_view` / `partition_stats` range overlap.
4. `partition_stats_temporal_spatial`: range overlap plus spatial extent overlap/intersection.
5. `cached_partition_set`: temp cache of partition arrays by normalized benchmark input.

## Ready-to-run instructions

From the repository root:

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

Important platform note: a baseline `scripts/test --formatting` attempt was made before editing, but Docker image build failed because `apt.postgresql.org` could not resolve and Debian/PostgreSQL package versions conflicted. The benchmark scaffold is therefore prepared to run immediately on a platform where the PgSTAC Docker build dependencies are reachable.

## Benchmark interpretation plan

For each query window, compare:

- median and mean milliseconds for each variant,
- returned candidate partition count,
- speedup versus `baseline_explain_chunker`,
- cold direct lookup versus warm cached lookup,
- temporal-only versus temporal+spatial candidate reduction.

Decision thresholds:

- If direct temporal metadata lookup is at least 2x faster than `chunker()` with identical or broader candidate sets, prototype replacing `chunker()` for simple datetime/collection searches.
- If spatial zone-map pruning reduces candidates by at least 30% for tile-like geometries, add a production GiST index and bbox-first predicate path.
- If cached partition sets provide at least 5x warm-query speedup, populate `search_wheres.partitions` or a related cache in production.
- If direct stats and constraint-derived ranges disagree, prioritize stats freshness and correctness before changing production pruning.

## Recommended next implementation sequence

1. Run the benchmark scaffold on a Docker-capable platform with reachable PostgreSQL package repositories.
2. Confirm correctness by comparing candidate partition arrays, not only counts, between baseline and direct approaches.
3. Implement a non-invasive SQL helper that returns candidate partition names from collection, datetime range, and optional geometry.
4. Wire that helper into `partition_queries()` via the existing `partitions` argument.
5. Add pgtap/basic SQL coverage for datetime-only, spatial-only, combined, open-ended datetime, and empty-result searches.
6. Only after correctness is proven, add production indexes or cache population logic.
