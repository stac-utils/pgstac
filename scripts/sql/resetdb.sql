BEGIN;
	DROP SCHEMA IF EXISTS pgstac CASCADE;
	\i pgstac.sql
	\copy collections (content) FROM 'test/testdata/collections.ndjson'
	\copy items_staging_ignore (content) FROM 'test/testdata/items.ndjson'
COMMIT;
