-- Build a populated database on the version under test and record what a
-- migration has to preserve. Run against the OLD version, before migrating.
SET SEARCH_PATH TO pgstac, public;

INSERT INTO collections (content) VALUES (
    '{"id":"mig-flat","type":"Collection",
      "extent":{"spatial":{"bbox":[[-180,-90,180,90]]},
                "temporal":{"interval":[["2020-01-01T00:00:00Z",null]]}}}'
);
INSERT INTO collections (content, partition_trunc) VALUES (
    '{"id":"mig-month","type":"Collection",
      "extent":{"spatial":{"bbox":[[-180,-90,180,90]]},
                "temporal":{"interval":[["2020-01-01T00:00:00Z",null]]}}}',
    'month'
);
INSERT INTO collections (content, partition_trunc) VALUES (
    '{"id":"mig-year","type":"Collection",
      "extent":{"spatial":{"bbox":[[-180,-90,180,90]]},
                "temporal":{"interval":[["2020-01-01T00:00:00Z",null]]}}}',
    'year'
);

-- Spread over two years so the partitioned collections get several partitions,
-- and give a tenth of the items a long duration so end_datetime is not simply
-- equal to datetime.
INSERT INTO items_staging (content)
SELECT jsonb_build_object(
    'id', c || '-' || g,
    'collection', c,
    'type', 'Feature',
    'geometry', jsonb_build_object(
        'type','Point',
        'coordinates', jsonb_build_array((g % 60) - 30, (g % 40) - 20)
    ),
    'properties', jsonb_build_object(
        'datetime', dt::text,
        'end_datetime', (
            dt + CASE WHEN g % 10 = 0 THEN '20 days'::interval ELSE '1 hour'::interval END
        )::text
    )
)
FROM generate_series(1, 150) g,
     unnest(ARRAY['mig-flat','mig-month','mig-year']) c,
     LATERAL (SELECT '2020-01-01'::timestamptz + ((g * 5) || ' days')::interval AS dt) d;

-- Kept outside the pgstac schema so the migration cannot touch it.
DROP TABLE IF EXISTS public.migration_snapshot;
CREATE TABLE public.migration_snapshot AS
SELECT
    (SELECT count(*) FROM items) AS items,
    (SELECT count(*) FROM collections) AS collections,
    (SELECT count(*) FROM partitions_view) AS partitions,
    (SELECT jsonb_object_agg(id, content->'extent') FROM collections) AS extents,
    (SELECT jsonb_object_agg(c, n) FROM (
        SELECT collection AS c, count(*) AS n FROM items GROUP BY collection
    ) x) AS items_per_collection,
    (SELECT jsonb_object_agg(c, n) FROM (
        SELECT c, jsonb_array_length(
            search(jsonb_build_object('collections', jsonb_build_array(c), 'limit', 500))->'features'
        ) AS n
        FROM unnest(ARRAY['mig-flat','mig-month','mig-year']) c
    ) y) AS search_per_collection,
    (SELECT jsonb_object_agg(c, n) FROM (
        SELECT c, jsonb_array_length(
            search(jsonb_build_object(
                'collections', jsonb_build_array(c),
                'datetime', '2020-06-01T00:00:00Z/2020-09-01T00:00:00Z',
                'limit', 500
            ))->'features'
        ) AS n
        FROM unnest(ARRAY['mig-flat','mig-month','mig-year']) c
    ) z) AS temporal_search_per_collection
;

SELECT * FROM public.migration_snapshot;
