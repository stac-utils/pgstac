---
description: "Debug a pypgstac data loading issue"
---

Diagnose a pypgstac Loader problem:

1. Check which load mode is being used (`insert`, `ignore`, `upsert`, `delsert`)
2. Verify the loader queries `partition_sys_meta` (live VIEW), not `partitions` (stale MATERIALIZED VIEW)
3. Check that generators are materialized to `list()` before `load_partition()` — generators can't survive tenacity retries
4. Verify `item.pop("partition", None)` uses `None` default for retry safety
5. Check the retry decorator covers: `CheckViolation`, `DeadlockDetected`, `SerializationFailure`, `LockNotAvailable`, `ObjectInUse`
6. Check `before_sleep` handler sets `partition.requires_update = True` on `CheckViolation`
7. Verify `get_tstz_constraint()` regex handles fractional seconds (`.` in timestamps)

Key files:
- `src/pypgstac/src/pypgstac/load.py` — Loader class
- `src/pgstac/sql/003b_partitions.sql` — partition constraint functions
