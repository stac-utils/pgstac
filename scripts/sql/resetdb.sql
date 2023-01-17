BEGIN;
	DROP SCHEMA IF EXISTS pgstac CASCADE;
	ALTER DATABASE postgis SET SEARCH_PATH TO pgstac, public;
	\i pgstac.sql
	\copy collections (content) FROM 'test/testdata/collections.ndjson'
	\copy items_staging_ignore (content) FROM 'test/testdata/items.ndjson'
COMMIT;
