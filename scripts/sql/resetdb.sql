BEGIN;
	DROP SCHEMA IF EXISTS pgstac CASCADE;
	\i pgstac.sql
\copy collections (content) FROM 'test/testdata/collections.ndjson'
\copy items (content) FROM 'test/testdata/items.ndjson'
SELECT backfill_partitions();
COMMIT;
