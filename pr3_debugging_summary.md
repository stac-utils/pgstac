# PR3: PgSTAC Field Registry Optimization & Debugging Summary

This document captures the complete architectural state, debugging analysis, and remaining tasks for **PR3 (PgSTAC v0.10.0 Field Registry on Partition Stats)**. It serves as full internal memory and an actionable checklist so you can seamlessly continue development and testing in VSCode.

---

## 1. Architectural State (PR3)

The goal of PR3 is to replace the legacy relational `item_field_registry` table with a performant, asynchronous JSONB-based field registry maintained on `partition_stats` and aggregated up to `collections`.

### Key Implementations Completed:
- **`field_registry` JSONB Columns**: Added to `partition_stats` and `collections` tables.
- **Efficient Extraction (`jsonb_field_rows`)**: Extracts `{path, type}` pairs directly from dehydrated JSON (`items.content`) without invoking the expensive `content_hydrate()` function.
- **Robust Type-Widening (`jsonb_merge_registry`)**: Merges registry entries and widens conflicting types (e.g., `number` + `string` -> `string`).
- **Collection Rollup Aggregate (`jsonb_merge_registry_agg`)**: Rolls up partition-level registries into collection-level registries.
- **Asynchronous Ingestion Integration**:
  - `update_partition_stats` uses `TABLESAMPLE SYSTEM(field_registry_sample_percent)` to sample schema without slowing down high-throughput ingestion.
  - `items_touch_triggerfunc` refactored to operate `BEFORE UPDATE` only, ensuring `pgstac_updated_at` and SHA-256 `content_hash` calculation are performant.
- **Maintenance Lifecycle**: `refresh_field_registry` refactored to invoke `update_partition_stats`.

---

## 2. Debugging Analysis: Root Causes of Remaining pgTAP Failures

When running `scripts/runinpypgstac test --pgtap`, exactly 3 tests fail out of 266 in `src/pgstac/tests/pgtap/003_items.sql`.

### Failure 1 & 2: `has_column` Checks for `field_registry`
```text
not ok 83 - field_registry
# Failed test 83: "field_registry"
not ok 84 - field_registry
# Failed test 84: "field_registry"
```
- **Root Cause**: In pgTAP, the function signatures for `has_column` are:
  1. `has_column(table_name, column_name)`
  2. `has_column(table_name, column_name, description)`
  3. `has_column(schema_name, table_name, column_name, description)`
- When calling `has_column('pgstac'::name, 'partition_stats'::name, 'field_registry'::name)`, pgTAP matched signature #2 (`table, column, description`). It searched for a column named `'partition_stats'` in a table named `'pgstac'`, which correctly failed.
- **Solution**: Pass the 4th `description` argument so pgTAP correctly matches signature #3.

### Failure 3: `update_partition_stats` Populates `field_registry`
```text
not ok 94 - update_partition_stats populates field_registry on partition_stats
# Failed test 94: "update_partition_stats populates field_registry on partition_stats"
```
- **Root Cause**: `update_partition_stats` uses `TABLESAMPLE SYSTEM(sample_pct)` where `sample_pct` defaults to 5.0 (5%). `SYSTEM` sampling in PostgreSQL samples at the **block/page level**, not the row level. For a tiny test table with only 1 item (occupying exactly 1 block), a 5% block sampling rate results in `0 rows` selected 95% of the time. Consequently, `new_registry` remains empty (`{}`).
- **Solution**: Temporarily set `field_registry_sample_percent` to `100` during the test setup in `003_items.sql` so that 100% of blocks/rows are sampled during test verification.

---

## 3. Required Code Changes in `003_items.sql`

To resolve all test failures, apply the following diff to `src/pgstac/tests/pgtap/003_items.sql`:

```diff
--- a/src/pgstac/tests/pgtap/003_items.sql
+++ b/src/pgstac/tests/pgtap/003_items.sql
@@ -28,11 +28,15 @@ SELECT has_function('pgstac'::name, 'refresh_field_registry', ARRAY['text']);
 SELECT has_function('pgstac'::name, 'refresh_field_registry', ARRAY['text']);

 -- partition_stats has field_registry column
-SELECT has_column('pgstac'::name, 'partition_stats'::name, 'field_registry'::name);
+SELECT has_column('pgstac'::name, 'partition_stats'::name, 'field_registry'::name, 'partition_stats has field_registry column');

 -- collections has field_registry column
-SELECT has_column('pgstac'::name, 'collections'::name, 'field_registry'::name);
+SELECT has_column('pgstac'::name, 'collections'::name, 'field_registry'::name, 'collections has field_registry column');

+-- Ensure 100% sampling during tests so single-row test tables populate the field registry reliably
+INSERT INTO pgstac_settings (name, value) VALUES ('field_registry_sample_percent', '100')
+ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value;
+
 DELETE FROM collections WHERE id in ('pgstac-test-collection', 'pgstac-test-collection2');
 \copy collections (content) FROM 'tests/testdata/collections.ndjson';
```

---

## 4. Developer Action Plan & Checklist

Follow these steps in VSCode / terminal to complete PR3:

- `[ ]` **Apply Fixes**: Edit `/home/bitner/data/pgstac/.worktree-pr3/src/pgstac/tests/pgtap/003_items.sql` using the diff above.
- `[ ]` **Run pgTAP Test Suite**:
  ```bash
  cd /home/bitner/data/pgstac/.worktree-pr3
  scripts/runinpypgstac test --pgtap
  ```
  *(Expect clean pass of all 266 tests)*
- `[ ]` **Verify Full Test Suite**:
  ```bash
  scripts/test --nomigrations
  ```
- `[ ]` **Merge/Rebase Workflow**:
  - Wait for PR2 to be reviewed and merged into `main`.
  - Rebase PR3 branch onto `main`.
  - Run `scripts/stageversion VERSION` (if version bumps are needed).
- `[ ]` **Create PR3 on GitHub**: Draft the PR explaining the performance benefits of the non-blocking `TABLESAMPLE` registry architecture.
