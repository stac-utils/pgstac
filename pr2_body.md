## Description

This PR (PR2) focuses on optimizing the metadata update and hashing lifecycle in PgSTAC to improve ingestion performance. It introduces deterministic STAC item content hashing and reduces the reliance on row-based triggers for ingestion.

### Key Changes
- **Renamed** the conceptual `updated_at` column for the table metadata to `pgstac_updated_at` (added explicitly to the schema as `pgstac_updated_at`).
- **Added** a `content_hash` column to track a deterministic SHA-256 hash of the STAC item's content.
- **Refactored Triggers**: Removed the expensive `BEFORE INSERT` trigger from the `items` table. The `items_touch_triggerfunc` is now bound strictly to `BEFORE UPDATE` to compute hashes and `pgstac_updated_at` only on manual row mutations outside of the bulk load path.
- **Optimized Content Dehydration**: Rewrote `content_dehydrate` in `PLPGSQL` to natively calculate `pgstac_updated_at` and `content_hash` (via `encode(sha256(content::text::bytea), 'hex')`) directly during the insert stage, completely bypassing the need for an insert trigger.
- **Updated PyPgSTAC Loader**: Altered `src/pypgstac/src/pypgstac/load.py` to use `INCLUDING DEFAULTS` when constructing `items_ingest_temp`, ensuring that direct COPY statements lacking `pgstac_updated_at` correctly fall back to the default `now()` value rather than throwing a `NotNullViolation`.

### Testing
- Full `PGTap` and basic SQL tests pass.
- Incremental migrations validate properly (using `pgpkg` generated artifacts).
- PyPgSTAC loader tests pass successfully with the updated temp table logic.

### Related Tasks
This is the second phase (PR2) of the v0.10.0 architecture restructuring plan.
