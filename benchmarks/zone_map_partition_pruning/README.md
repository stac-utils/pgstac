# CQL2 cached partition-stat pruning benchmark scaffold

This scaffold benchmarks the current EXPLAIN/constraint-pruning path against cached per-partition statistics for CQL2-like filters. It is focused on two goals:

1. quickly determining partitions that could be touched by a CQL2 filter without relying on partition constraint exclusion, and
2. ordering those candidate partitions into datetime chunks for expensive datetime-sorted searches with a small `LIMIT`.

## Run

```bash
cd /home/runner/work/pgstac/pgstac
benchmarks/zone_map_partition_pruning/run_benchmark.sh
```

Optional custom config:

```bash
benchmarks/zone_map_partition_pruning/run_benchmark.sh /absolute/path/to/config.json
```

Results are written to `benchmarks/zone_map_partition_pruning/results/zone_map_partition_pruning_results.json` by default.
