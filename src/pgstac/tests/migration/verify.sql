-- Compare the migrated database against the snapshot taken on the old version.
-- Every check raises, so ON_ERROR_STOP makes the suite fail on any regression.
SET SEARCH_PATH TO pgstac, public;

DO $$
DECLARE
    s public.migration_snapshot%ROWTYPE;
    now_items bigint;
    now_collections bigint;
    now_partitions bigint;
    now_extents jsonb;
    now_items_per jsonb;
    now_search_per jsonb;
    now_temporal_per jsonb;
    missing bigint;
    orphans bigint;
    unconstrained bigint;
BEGIN
    SELECT * INTO s FROM public.migration_snapshot;

    SELECT count(*) INTO now_items FROM items;
    SELECT count(*) INTO now_collections FROM collections;
    SELECT count(*) INTO now_partitions FROM partitions_view;
    SELECT jsonb_object_agg(id, content->'extent') INTO now_extents FROM collections;

    SELECT jsonb_object_agg(c, n) INTO now_items_per FROM (
        SELECT collection AS c, count(*) AS n FROM items GROUP BY collection
    ) x;
    SELECT jsonb_object_agg(c, n) INTO now_search_per FROM (
        SELECT c, jsonb_array_length(
            search(jsonb_build_object('collections', jsonb_build_array(c), 'limit', 500))->'features'
        ) AS n
        FROM unnest(ARRAY['mig-flat','mig-month','mig-year']) c
    ) y;
    SELECT jsonb_object_agg(c, n) INTO now_temporal_per FROM (
        SELECT c, jsonb_array_length(
            search(jsonb_build_object(
                'collections', jsonb_build_array(c),
                'datetime', '2020-06-01T00:00:00Z/2020-09-01T00:00:00Z',
                'limit', 500
            ))->'features'
        ) AS n
        FROM unnest(ARRAY['mig-flat','mig-month','mig-year']) c
    ) z;

    -- Nothing may be lost.
    IF now_items <> s.items THEN
        RAISE EXCEPTION 'items changed across migration: % -> %', s.items, now_items;
    END IF;
    IF now_collections <> s.collections THEN
        RAISE EXCEPTION 'collections changed across migration: % -> %', s.collections, now_collections;
    END IF;
    IF now_partitions <> s.partitions THEN
        RAISE EXCEPTION 'partitions changed across migration: % -> %', s.partitions, now_partitions;
    END IF;
    IF now_items_per IS DISTINCT FROM s.items_per_collection THEN
        RAISE EXCEPTION 'items per collection changed: % -> %', s.items_per_collection, now_items_per;
    END IF;

    -- Search must return exactly what it did before, including the temporal
    -- overlap query, which depends on partition pruning and the CHECK
    -- constraints the migration rebuilds.
    IF now_search_per IS DISTINCT FROM s.search_per_collection THEN
        RAISE EXCEPTION 'search results changed: % -> %', s.search_per_collection, now_search_per;
    END IF;
    IF now_temporal_per IS DISTINCT FROM s.temporal_search_per_collection THEN
        RAISE EXCEPTION 'temporal search results changed: % -> %',
            s.temporal_search_per_collection, now_temporal_per;
    END IF;

    -- A valid extent must never be replaced, in particular not with JSON null.
    IF now_extents IS DISTINCT FROM s.extents THEN
        RAISE EXCEPTION 'collection extents changed: % -> %', s.extents, now_extents;
    END IF;

    -- Search finds partitions through partition_stats, so every partition needs
    -- an identity row and there must be no rows for partitions that are gone.
    SELECT count(*) INTO missing FROM partitions_view pv
    WHERE NOT EXISTS (
        SELECT 1 FROM partition_stats ps
        WHERE ps.partition = pv.partition
          AND ps.collection IS NOT NULL
          AND ps.partition_dtrange IS NOT NULL
    );
    IF missing > 0 THEN
        RAISE EXCEPTION '% partitions have no identity row in partition_stats', missing;
    END IF;

    SELECT count(*) INTO orphans FROM partition_stats ps
    WHERE NOT EXISTS (SELECT 1 FROM partitions_view pv WHERE pv.partition = ps.partition);
    IF orphans > 0 THEN
        RAISE EXCEPTION '% partition_stats rows reference partitions that do not exist', orphans;
    END IF;

    -- The migration is what repairs the CHECK constraints that 0.9.11 left off
    -- the staging path. Without a validated end_datetime constraint a partition
    -- cannot be pruned by an end_datetime predicate.
    SELECT count(*) INTO unconstrained FROM partitions_view
    WHERE constraint_edtrange = '[-infinity,infinity]'::tstzrange;
    IF unconstrained > 0 THEN
        RAISE EXCEPTION '% partitions still have no end_datetime constraint after migration', unconstrained;
    END IF;

    RAISE NOTICE 'migration verified: % items, % partitions, extents and search results unchanged',
        now_items, now_partitions;
END;
$$;

-- Ingest has to keep working afterwards, including a batch outside the
-- constraints the migration just built, which check_partition must widen.
INSERT INTO items_staging (content) VALUES (
    '{"id":"post-migrate","collection":"mig-month","type":"Feature",
      "geometry":{"type":"Point","coordinates":[5,5]},
      "properties":{"datetime":"2031-07-01T00:00:00Z"}}'
);

DO $$
DECLARE
    found int;
BEGIN
    SELECT jsonb_array_length(
        search('{"collections":["mig-month"],"ids":["post-migrate"]}')->'features'
    ) INTO found;
    IF found <> 1 THEN
        RAISE EXCEPTION 'item ingested after migration is not searchable (found %)', found;
    END IF;
    RAISE NOTICE 'post-migration ingest verified';
END;
$$;

DROP TABLE public.migration_snapshot;
