SET pgstac.use_queue=TRUE;
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
SELECT count(*) FROM partitions WHERE collection='pgstactest-partitioned';

--test collection partioned by year
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-partitioned-year"}', 'year');
INSERT INTO items_staging(content)
SELECT content || '{"collection":"pgstactest-partitioned-year"}'::jsonb FROM test_items;
SELECT count(*) FROM partitions WHERE collection='pgstactest-partitioned-year';

--test collection partioned by month
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-partitioned-month"}', 'month');
INSERT INTO items_staging(content)
SELECT content || '{"collection":"pgstactest-partitioned-month"}'::jsonb FROM test_items;
SELECT count(*) FROM partitions WHERE collection='pgstactest-partitioned-month';

--test repartitioning from year to non partitioned
UPDATE collections SET partition_trunc=NULL WHERE id='pgstactest-partitioned-year';
SELECT count(*) FROM partitions WHERE collection='pgstactest-partitioned-year';
SELECT count(*) FROM items WHERE collection='pgstactest-partitioned-year';

--test repartitioning from non-partitioned to year
UPDATE collections SET partition_trunc='year' WHERE id='pgstactest-partitioned';
SELECT count(*) FROM partitions WHERE collection='pgstactest-partitioned';
SELECT count(*) FROM items WHERE collection='pgstactest-partitioned';

--test noop for repartitioning
UPDATE collections SET content=content || '{"foo":"bar"}'::jsonb WHERE id='pgstactest-partitioned-month';
SELECT count(*) FROM partitions WHERE collection='pgstactest-partitioned-month';
SELECT count(*) FROM items WHERE collection='pgstactest-partitioned-month';

--check that partition stats haven't been updated
SELECT collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange, spatial FROM partitions WHERE collection='pgstactest-partitioned-year';

--check that queue has items
SELECT count(*) FROM query_queue;

--run queue items to update partition stats
SELECT run_queued_queries_intransaction();

--check that partition stats have been updated
SELECT collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange, spatial FROM partitions WHERE collection='pgstactest-partitioned-year';
