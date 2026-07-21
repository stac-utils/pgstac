-- run tests as pgstac_ingest
SET ROLE pgstac_ingest;
SET pgstac.use_queue=FALSE;
SELECT get_setting_bool('use_queue');
SET pgstac.update_collection_extent=TRUE;
SELECT get_setting_bool('update_collection_extent');
--create base data to use with tests
CREATE TEMP TABLE test_items AS
SELECT jsonb_build_object(
    'id', concat('pgstactest-partitioned-', (row_number() over ())::text),
    'collection', 'pgstactest-partitioned',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object( 'datetime', g::text)
) as content FROM generate_series('2020-01-01'::timestamptz, '2022-01-01'::timestamptz, '1 week'::interval) g;

--test non-partitioned collection
INSERT INTO collections (content) VALUES ('{"id":"pgstactest-partitioned"}');
INSERT INTO items_staging(content)
SELECT content FROM test_items;
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned';

--test collection partioned by year
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-partitioned-year"}', 'year');
INSERT INTO items_staging(content)
SELECT content || '{"collection":"pgstactest-partitioned-year"}'::jsonb FROM test_items;
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-year';

--test collection partioned by month
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-partitioned-month"}', 'month');
INSERT INTO items_staging(content)
SELECT content || '{"collection":"pgstactest-partitioned-month"}'::jsonb FROM test_items;
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-month';

--test repartitioning from year to non partitioned
UPDATE collections SET partition_trunc=NULL WHERE id='pgstactest-partitioned-year';
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-year';
SELECT count(*) FROM items WHERE collection='pgstactest-partitioned-year';

--test repartitioning from non-partitioned to year
UPDATE collections SET partition_trunc='year' WHERE id='pgstactest-partitioned';
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned';
SELECT count(*) FROM items WHERE collection='pgstactest-partitioned';

--check that partition stats have been updated
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned' and spatial IS NULL;

--test noop for repartitioning
UPDATE collections SET content=content || '{"foo":"bar"}'::jsonb WHERE id='pgstactest-partitioned-month';
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-month';
SELECT count(*) FROM items WHERE collection='pgstactest-partitioned-month';

--test using query queue
SET pgstac.use_queue=TRUE;
SELECT get_setting_bool('use_queue');

INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-partitioned-q"}', 'month');
INSERT INTO items_staging(content)
SELECT content || '{"collection":"pgstactest-partitioned-q"}'::jsonb FROM test_items;
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-q';

--Under the widen-now / tighten-async model, ingest widens partition stats inline with spatial left NULL
--(always a search candidate) until an async tighten, and queues NO stats work, so the query queue stays
--empty.
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-q' and spatial IS NULL;
SELECT count(*)>0 FROM query_queue;

--tighten the partitions explicitly (the async maintenance step) to compute exact stats
SELECT count(*)>0 AS tightened FROM (
    SELECT tighten_partition_stats(partition) FROM partitions_view WHERE collection='pgstactest-partitioned-q'
) t;

--after tighten, non-empty partitions have an exact spatial extent (NULL remains only for empty partitions)
SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-q' and spatial IS NULL;

--collection extents are refreshed explicitly (maintenance), not on ingest
SELECT update_collection_extents();
SELECT id, content->'extent' FROM collections WHERE id LIKE 'pgstactest-partitioned%' ORDER BY id;

--check that values for datetimes that are non 4 digit or that have very high precision are ingesting correctly and that partitioning is working for them
SET pgstac.use_queue=FALSE;
SELECT get_setting_bool('use_queue');

INSERT INTO test_items (content)
SELECT jsonb_build_object(
    'id', 'pgstactest-partitioned-whackyyear',
    'collection', 'pgstactest-partitioned',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object( 'datetime', '10000-01-01T00:00:00Z')
);
INSERT INTO test_items (content)
SELECT jsonb_build_object(
    'id', 'pgstactest-partitioned-whackyprecision',
    'collection', 'pgstactest-partitioned',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object( 'datetime', '2000-01-01T00:00:00.12389878917192387129837Z')
);
INSERT INTO test_items (content)
SELECT jsonb_build_object(
    'id', 'pgstactest-partitioned-startend',
    'collection', 'pgstactest-partitioned',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object( 'start_datetime', '2000-01-01T00:00:00.12389878917192387129837Z', 'end_datetime', '99999-01-01T00:00:00Z')
);

INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-partitioned-oddballs"}', 'month');
INSERT INTO items_staging(content)
SELECT content || '{"collection":"pgstactest-partitioned-oddballs"}'::jsonb FROM test_items;

SELECT count(*) FROM partitions_view WHERE collection='pgstactest-partitioned-oddballs';

SELECT collection, constraint_dtrange, constraint_edtrange, dtrange, edtrange
FROM partitions_view
WHERE collection='pgstactest-partitioned-oddballs'
ORDER BY constraint_dtrange;
