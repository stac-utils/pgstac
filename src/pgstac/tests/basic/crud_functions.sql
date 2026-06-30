-- run tests as pgstac_ingest
SET ROLE pgstac_ingest;
SET pgstac.use_queue=FALSE;
SELECT get_setting_bool('use_queue');
-- NOTE: collection extent is no longer auto-updated on ingest. It is refreshed EXPLICITLY via
-- update_collection_extents() (exercised at the end of this test), so the inline checks below show an
-- unset extent right after ingest.
--create base data to use with tests
CREATE TEMP TABLE test_items AS
SELECT jsonb_build_object(
    'id', concat('pgstactest-crudtest-', (row_number() over ())::text),
    'collection', 'pgstactest-crudtest',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object( 'datetime', g::text)
) as content FROM generate_series('2020-01-01'::timestamptz, '2022-01-01'::timestamptz, '1 month'::interval) g;

--test collection partioned by year
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-crudtest"}', 'year');

-- Create an item
SELECT create_item((SELECT content FROM test_items LIMIT 1));
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

-- Extent is NOT auto-updated on ingest (now explicit/async) -- expect it unset here
SELECT content->'extent' FROM collections WHERE id='pgstactest-crudtest';

-- Update item with new datetime that is in a different partition
SELECT update_item((SELECT content || '{"properties":{"datetime":"2023-01-01 00:00:00Z"}}'::jsonb  FROM test_items LIMIT 1));
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

-- Extent is NOT auto-updated on ingest (now explicit/async) -- expect it unset here
SELECT content->'extent' FROM collections WHERE id='pgstactest-crudtest';

-- Update item with new datetime that is in a different partition
SELECT upsert_item((SELECT content || '{"properties":{"datetime":"2023-02-01 00:00:00Z"}}'::jsonb  FROM test_items LIMIT 1));
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

-- Delete an item
SELECT delete_item('pgstactest-crudtest-1', 'pgstactest-crudtest');
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

WITH c AS (SELECT content FROM test_items LIMIT 2),
aggregated AS (SELECT jsonb_agg(content) as items FROM c)
SELECT create_items(items) FROM aggregated;
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

-- clear the collection via delete_item (direct DELETE on items is blocked by the privilege wall)
WITH ids AS MATERIALIZED (SELECT id FROM items WHERE collection='pgstactest-crudtest')
SELECT count(*) AS cleared FROM (SELECT delete_item(id, 'pgstactest-crudtest') FROM ids) d;

-- upsert items that do not exist yet
WITH c AS (SELECT content FROM test_items LIMIT 2),
aggregated AS (SELECT jsonb_agg(content) as items FROM c)
SELECT upsert_items(items) FROM aggregated;
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

-- upsert items that already exist and are to be modified
WITH c AS (SELECT content || '{"properties":{"datetime":"2023-02-01 00:00:00Z"}}'::jsonb as content FROM test_items LIMIT 2),
aggregated AS (SELECT jsonb_agg(content) as items FROM c)
SELECT upsert_items(items) FROM aggregated;
SELECT id, geometry, collection, datetime, end_datetime, properties, extra FROM items WHERE collection='pgstactest-crudtest' ORDER BY id;

-- Collection extent is refreshed EXPLICITLY (maintenance), not automatically on ingest. Tighten the
-- partitions to exact stats, then refresh the stored extent, and verify it is now populated.
SELECT tighten_partition_stats(partition) FROM partitions_view WHERE collection='pgstactest-crudtest';
SELECT update_collection_extents();
SELECT content->'extent' FROM collections WHERE id='pgstactest-crudtest';

-- check formatting of temporal extent
SELECT collection_temporal_extent('pgstactest-crudtest');

