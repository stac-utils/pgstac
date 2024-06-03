-- run tests as pgstac_ingest
SET ROLE pgstac_ingest;
SET pgstac.use_queue=FALSE;
SELECT get_setting_bool('use_queue');
SET pgstac.update_collection_extent=TRUE;
SELECT get_setting_bool('update_collection_extent');
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
SELECT * FROM items WHERE collection='pgstactest-crudtest';

SELECT content->'extent' FROM collections WHERE id='pgstactest-crudtest';

-- Update item with new datetime that is in a different partition
SELECT update_item((SELECT content || '{"properties":{"datetime":"2023-01-01 00:00:00Z"}}'::jsonb  FROM test_items LIMIT 1));
SELECT * FROM items WHERE collection='pgstactest-crudtest';

-- Update item with new datetime that is in a different partition
SELECT upsert_item((SELECT content || '{"properties":{"datetime":"2023-02-01 00:00:00Z"}}'::jsonb  FROM test_items LIMIT 1));
SELECT * FROM items WHERE collection='pgstactest-crudtest';

-- Delete an item
SELECT delete_item('pgstactest-crudtest-1', 'pgstactest-crudtest');
SELECT * FROM items WHERE collection='pgstactest-crudtest';

WITH c AS (SELECT content FROM test_items LIMIT 2),
aggregated AS (SELECT jsonb_agg(content) as items FROM c)
SELECT create_items(items) FROM aggregated;
SELECT * FROM items WHERE collection='pgstactest-crudtest';

DELETE FROM items WHERE collection='pgstactest-crudtest';

-- upsert items that do not exist yet
WITH c AS (SELECT content FROM test_items LIMIT 2),
aggregated AS (SELECT jsonb_agg(content) as items FROM c)
SELECT upsert_items(items) FROM aggregated;
SELECT * FROM items WHERE collection='pgstactest-crudtest';

-- upsert items that already exist and are to be modified
WITH c AS (SELECT content || '{"properties":{"datetime":"2023-02-01 00:00:00Z"}}'::jsonb as content FROM test_items LIMIT 2),
aggregated AS (SELECT jsonb_agg(content) as items FROM c)
SELECT upsert_items(items) FROM aggregated;
SELECT * FROM items WHERE collection='pgstactest-crudtest';
