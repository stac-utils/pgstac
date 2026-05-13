# Zone-map partition pruning benchmark scaffold

This scaffold benchmarks the current EXPLAIN-driven `chunker()` path against five candidate zone-map pruning strategies without editing PgSTAC SQL source files.

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
