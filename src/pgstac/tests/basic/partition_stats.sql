-- Tests for partition metadata tracked on partition_stats, which is what
-- search uses to find partitions.
SET ROLE pgstac_ingest;
SET pgstac.use_queue=FALSE;
SET pgstac.update_collection_extent=FALSE;

CREATE TEMP TABLE ps_items AS
SELECT jsonb_build_object(
    'id', concat('pgstactest-pstats-', (row_number() over ())::text),
    'collection', 'pgstactest-pstats',
    'geometry', '{"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}'::json,
    'properties', jsonb_build_object('datetime', g::text)
) as content FROM generate_series('2020-01-01'::timestamptz, '2020-04-01'::timestamptz, '1 week'::interval) g;

INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-pstats"}', 'month');
INSERT INTO items_staging (content) SELECT content FROM ps_items;

-- every leaf partition has an identity row
SELECT count(*) = 0 AS every_partition_has_identity FROM partitions_view pv
WHERE NOT EXISTS (
    SELECT 1 FROM partition_stats ps
    WHERE ps.partition = pv.partition
      AND ps.collection IS NOT NULL
      AND ps.partition_dtrange IS NOT NULL
);

-- and no rows for partitions that do not exist
SELECT count(*) = 0 AS no_orphan_rows FROM partition_stats ps
WHERE NOT EXISTS (SELECT 1 FROM partitions_view pv WHERE pv.partition = ps.partition);

-- identity columns agree with the catalog
SELECT count(*) = 0 AS identity_matches_catalog
FROM partitions_view pv JOIN partition_stats ps USING (partition)
WHERE ps.collection IS DISTINCT FROM pv.collection
   OR ps.partition_dtrange IS DISTINCT FROM pv.partition_dtrange;

-- partition_oid name handling
SELECT
    partition_oid(partition) IS NOT NULL AS bare_name_resolves,
    partition_oid('pgstac.' || partition) IS NOT NULL AS qualified_resolves,
    partition_oid('pgstac."' || partition || '"') IS NOT NULL AS quoted_resolves,
    partition_oid('public.' || partition) IS NULL AS other_schema_rejected
FROM partition_stats WHERE collection = 'pgstactest-pstats' ORDER BY partition LIMIT 1;

SELECT partition_oid('_items_no_such_partition') IS NULL AS missing_is_null;

-- partition_catalog_meta returns nothing for a partition that does not exist,
-- and nothing for an intermediate (non-leaf) partition
SELECT count(*) = 0 AS meta_missing_is_empty FROM partition_catalog_meta('_items_no_such_partition');

SELECT count(*) = 0 AS meta_skips_intermediate
FROM collections c, LATERAL partition_catalog_meta(format('_items_%s', c.key))
WHERE c.id = 'pgstactest-pstats';

-- an update for a partition that no longer exists must not create a row
SELECT update_partition_stats('_items_no_such_partition');
SELECT count(*) = 0 AS no_row_for_missing_partition
FROM partition_stats WHERE partition = '_items_no_such_partition';

-- run_or_queue / update_partition_stats_q report whether they ran
SET pgstac.use_queue=FALSE;
SELECT run_or_queue('SELECT 1') AS runs_inline_when_queue_off;
SET pgstac.use_queue=TRUE;
SELECT run_or_queue('SELECT 2') AS queues_when_queue_on;
DELETE FROM query_queue;
SET pgstac.use_queue=FALSE;

-- chunker bands cover the full data range of the collection
SELECT
    min(s) = date_trunc('month', min_dt) AS bands_start_at_first_month,
    max(e) > max_dt AS bands_cover_last_item
FROM chunker($q$ collection = 'pgstactest-pstats' $q$),
LATERAL (SELECT min(datetime) AS min_dt, max(datetime) AS max_dt FROM items WHERE collection='pgstactest-pstats') d
GROUP BY min_dt, max_dt;

-- searching the collection finds every item
SELECT jsonb_array_length(search('{"collections":["pgstactest-pstats"],"limit":100}')->'features')
    = (SELECT count(*) FROM items WHERE collection='pgstactest-pstats') AS search_returns_all;

-- sync_partition_stats removes orphans and restores missing identity rows
INSERT INTO partition_stats (partition, collection, partition_dtrange)
    VALUES ('_items_orphan_row', 'pgstactest-pstats', '[-infinity,infinity]'::tstzrange);
DELETE FROM partition_stats WHERE partition = (
    SELECT partition FROM partition_stats WHERE collection='pgstactest-pstats' ORDER BY partition LIMIT 1
);
SELECT sync_partition_stats();
SELECT count(*) = 0 AS sync_pruned_orphan FROM partition_stats WHERE partition = '_items_orphan_row';
SELECT count(*) = 0 AS sync_restored_identity FROM partitions_view pv
WHERE NOT EXISTS (
    SELECT 1 FROM partition_stats ps
    WHERE ps.partition = pv.partition
      AND ps.collection IS NOT NULL
      AND ps.partition_dtrange IS NOT NULL
);

-- the staging tables each clear themselves
INSERT INTO items_staging_upsert (content) SELECT content FROM ps_items LIMIT 1;
SELECT count(*) AS items_staging_upsert_cleared FROM items_staging_upsert;
INSERT INTO items_staging_ignore (content) SELECT content FROM ps_items LIMIT 1;
SELECT count(*) AS items_staging_ignore_cleared FROM items_staging_ignore;

-- deleting the collection removes its partition_stats rows. delete_collection
-- is SECURITY DEFINER, so pgstac_ingest can drop the partition tables it owns.
SELECT delete_collection('pgstactest-pstats');
SELECT count(*) AS stats_rows_after_collection_delete
FROM partition_stats WHERE collection = 'pgstactest-pstats';

-- queued mode: the identity row still exists before the queue is drained
SET pgstac.use_queue=TRUE;
INSERT INTO collections (content, partition_trunc) VALUES ('{"id":"pgstactest-pstats-q"}', 'month');
INSERT INTO items_staging (content)
SELECT content || '{"collection":"pgstactest-pstats-q"}'::jsonb FROM ps_items;

SELECT count(*) > 0 AS queued_partitions_visible
FROM partition_stats WHERE collection = 'pgstactest-pstats-q' AND partition_dtrange IS NOT NULL;

SELECT count(*) = 0 AS queued_observed_ranges_deferred
FROM partition_stats WHERE collection = 'pgstactest-pstats-q' AND dtrange IS NOT NULL;

SELECT jsonb_array_length(search('{"collections":["pgstactest-pstats-q"],"limit":100}')->'features')
    = (SELECT count(*) FROM items WHERE collection='pgstactest-pstats-q') AS queued_search_returns_all;

SET pgstac.use_queue=FALSE;
