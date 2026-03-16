-- Test that partition views and tables work identically with and without pgstac in search_path
SET ROLE pgstac_ingest;
SET pgstac.use_queue=FALSE;

-- Set up test data with pgstac in search_path
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-searchpath"}', 'month');
CREATE TEMP TABLE sp_test_items AS
SELECT jsonb_build_object(
    'id', concat('pgstactest-searchpath-', (row_number() over ())::text),
    'collection', 'pgstactest-searchpath',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object('datetime', g::text)
) as content FROM generate_series('2020-01-01'::timestamptz, '2020-06-01'::timestamptz, '1 week'::interval) g;
INSERT INTO items_staging(content) SELECT content FROM sp_test_items;

-- Capture results with pgstac in search_path
CREATE TEMP TABLE sp_partition_sys_meta AS
SELECT * FROM partition_sys_meta WHERE collection='pgstactest-searchpath' ORDER BY partition;

CREATE TEMP TABLE sp_partition_stats AS
SELECT * FROM partition_stats WHERE partition IN (SELECT partition FROM sp_partition_sys_meta) ORDER BY partition;

CREATE TEMP TABLE sp_partitions AS
SELECT * FROM partitions WHERE collection='pgstactest-searchpath' ORDER BY partition;

CREATE TEMP TABLE sp_partitions_view AS
SELECT * FROM partitions_view WHERE collection='pgstactest-searchpath' ORDER BY partition;

CREATE TEMP TABLE sp_partition_steps AS
SELECT * FROM partition_steps WHERE name IN (SELECT partition FROM sp_partition_sys_meta) ORDER BY name;

-- Verify we have data
SELECT count(*) > 0 AS has_partition_sys_meta FROM sp_partition_sys_meta;
SELECT count(*) > 0 AS has_partition_stats FROM sp_partition_stats;
SELECT count(*) > 0 AS has_partitions FROM sp_partitions;
SELECT count(*) > 0 AS has_partitions_view FROM sp_partitions_view;
SELECT count(*) > 0 AS has_partition_steps FROM sp_partition_steps;

-- Now remove pgstac from search_path
SET search_path TO public;

-- partition_sys_meta: compare counts and key columns
SELECT (
    SELECT count(*) FROM pgstac.partition_sys_meta WHERE collection='pgstactest-searchpath'
) = (
    SELECT count(*) FROM sp_partition_sys_meta
) AS partition_sys_meta_count_match;

SELECT count(*) = 0 AS partition_sys_meta_data_match FROM (
    (SELECT partition, collection, constraint_dtrange, constraint_edtrange FROM pgstac.partition_sys_meta WHERE collection='pgstactest-searchpath'
     EXCEPT
     SELECT partition, collection, constraint_dtrange, constraint_edtrange FROM sp_partition_sys_meta)
    UNION ALL
    (SELECT partition, collection, constraint_dtrange, constraint_edtrange FROM sp_partition_sys_meta
     EXCEPT
     SELECT partition, collection, constraint_dtrange, constraint_edtrange FROM pgstac.partition_sys_meta WHERE collection='pgstactest-searchpath')
) diff;

-- partition_stats: compare counts and key columns
SELECT (
    SELECT count(*) FROM pgstac.partition_stats WHERE partition IN (SELECT partition FROM sp_partition_sys_meta)
) = (
    SELECT count(*) FROM sp_partition_stats
) AS partition_stats_count_match;

SELECT count(*) = 0 AS partition_stats_data_match FROM (
    (SELECT partition, dtrange, edtrange FROM pgstac.partition_stats WHERE partition IN (SELECT partition FROM sp_partition_sys_meta)
     EXCEPT
     SELECT partition, dtrange, edtrange FROM sp_partition_stats)
    UNION ALL
    (SELECT partition, dtrange, edtrange FROM sp_partition_stats
     EXCEPT
     SELECT partition, dtrange, edtrange FROM pgstac.partition_stats WHERE partition IN (SELECT partition FROM sp_partition_sys_meta))
) diff;

-- partitions (materialized view): compare counts and key columns
SELECT (
    SELECT count(*) FROM pgstac.partitions WHERE collection='pgstactest-searchpath'
) = (
    SELECT count(*) FROM sp_partitions
) AS partitions_count_match;

SELECT count(*) = 0 AS partitions_data_match FROM (
    (SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM pgstac.partitions WHERE collection='pgstactest-searchpath'
     EXCEPT
     SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM sp_partitions)
    UNION ALL
    (SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM sp_partitions
     EXCEPT
     SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM pgstac.partitions WHERE collection='pgstactest-searchpath')
) diff;

-- partitions_view: compare counts and key columns
SELECT (
    SELECT count(*) FROM pgstac.partitions_view WHERE collection='pgstactest-searchpath'
) = (
    SELECT count(*) FROM sp_partitions_view
) AS partitions_view_count_match;

SELECT count(*) = 0 AS partitions_view_data_match FROM (
    (SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM pgstac.partitions_view WHERE collection='pgstactest-searchpath'
     EXCEPT
     SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM sp_partitions_view)
    UNION ALL
    (SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM sp_partitions_view
     EXCEPT
     SELECT partition, collection, partition_dtrange, constraint_dtrange, constraint_edtrange, dtrange, edtrange FROM pgstac.partitions_view WHERE collection='pgstactest-searchpath')
) diff;

-- partition_steps: compare counts and key columns
SELECT (
    SELECT count(*) FROM pgstac.partition_steps WHERE name IN (SELECT partition FROM sp_partition_sys_meta)
) = (
    SELECT count(*) FROM sp_partition_steps
) AS partition_steps_count_match;

SELECT count(*) = 0 AS partition_steps_data_match FROM (
    (SELECT name, sdate, edate FROM pgstac.partition_steps WHERE name IN (SELECT partition FROM sp_partition_sys_meta)
     EXCEPT
     SELECT name, sdate, edate FROM sp_partition_steps)
    UNION ALL
    (SELECT name, sdate, edate FROM sp_partition_steps
     EXCEPT
     SELECT name, sdate, edate FROM pgstac.partition_steps WHERE name IN (SELECT partition FROM sp_partition_sys_meta))
) diff;

-- Restore search_path
SET search_path TO pgstac, public;
