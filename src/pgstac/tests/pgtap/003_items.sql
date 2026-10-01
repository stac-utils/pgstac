SELECT has_table('pgstac'::name, 'items'::name);


SELECT is_indexed('pgstac'::name, 'items'::name, 'geometry');

SELECT is_partitioned('pgstac'::name,'items'::name);


SELECT has_function('pgstac'::name, 'get_item', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'delete_item', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'create_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'create_items', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_items', ARRAY['jsonb']);


-- tools to update collection extents based on extents in items
SELECT has_function('pgstac'::name, 'collection_bbox', ARRAY['text']);
SELECT has_function('pgstac'::name, 'collection_temporal_extent', ARRAY['text']);
SELECT has_function('pgstac'::name, 'update_collection_extents', '{}'::text[]);

DELETE FROM collections WHERE id in ('pgstac-test-collection', 'pgstac-test-collection2');
\copy collections (content) FROM 'tests/testdata/collections.ndjson';

SELECT create_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 28, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');

SELECT results_eq($$
    SELECT content->'properties'->>'eo:cloud_cover' FROM items WHERE collection='pgstac-test-collection';
    $$,$$
    SELECT '28';
    $$,
    'Test create_item function'
);

SELECT update_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 29, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');

SELECT results_eq($$
    SELECT content->'properties'->>'eo:cloud_cover' FROM items WHERE collection='pgstac-test-collection';
    $$,$$
    SELECT '29';
    $$,
    'Test update_item function'
);

select delete_item('pgstac-test-item-0003');

SELECT results_eq($$
    SELECT count(*) FROM items WHERE collection='pgstac-test-collection';
    $$,$$
    SELECT 0::bigint;
    $$,
    'Test delete_item function'
);


-- Base item versioning: items keep hydrating against the base item they were dehydrated from.

SELECT create_collection('{"id": "pgstac-test-baseitems", "type": "Collection", "stac_version": "1.0.0", "description": "base item versioning", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2011-01-01T00:00:00Z", "2011-12-31T00:00:00Z"]]}}, "item_assets": {"image": {"type": "image/tiff", "title": "Image"}, "thumbnail": {"type": "image/jpeg", "title": "Thumbnail"}}}');

SELECT is_empty($$
    SELECT * FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,
    'A collection that has never been edited has no base_items rows'
);

SELECT is(
    (SELECT base_item_id FROM current_base_item('pgstac-test-baseitems')),
    NULL::int,
    'current_base_item has no id before the first edit'
);

SELECT create_item('{"id": "pgstac-test-baseitem-a", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-baseitems", "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff", "title": "Image"}, "thumbnail": {"href": "https://example.com/a.jpg", "type": "image/jpeg", "title": "Thumbnail"}}, "properties": {"datetime": "2011-06-01T00:00:00Z"}, "stac_extensions": []}');

CREATE TEMP TABLE baseitem_snapshot AS
SELECT get_item('pgstac-test-baseitem-a', 'pgstac-test-baseitems') AS snapshot;

SELECT results_eq($$
    SELECT content ? 'pgstac:base_item' FROM items WHERE id='pgstac-test-baseitem-a';
    $$,$$
    SELECT false;
    $$,
    'An item loaded before any edit is stored without a tag'
);

SELECT update_collection(jsonb_set(content, '{extent,temporal,interval,0,1}', '"2012-12-31T00:00:00Z"'))
FROM collections WHERE id='pgstac-test-baseitems';

SELECT is_empty($$
    SELECT * FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,
    'An update that does not change the base item creates no base_items rows'
);

-- One edit that changes an item_assets value, adds a key and removes an asset.
SELECT update_collection(content || '{"item_assets": {"image": {"type": "image/tiff", "title": "Image (changed)", "roles": ["data"]}}}'::jsonb)
FROM collections WHERE id='pgstac-test-baseitems';

SELECT results_eq($$
    SELECT count(*) FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,$$
    SELECT 2::bigint;
    $$,
    'The first base item edit records the old and the new base item'
);

SELECT results_eq($$
    SELECT base_item FROM base_items WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1;
    $$,$$
    SELECT base_item FROM collections WHERE id='pgstac-test-baseitems';
    $$,
    'The highest base_items id holds the collection current base item'
);

SELECT results_eq($$
    SELECT base_item_id, base_item FROM current_base_item('pgstac-test-baseitems');
    $$,$$
    SELECT max(id), (SELECT base_item FROM collections WHERE id='pgstac-test-baseitems') FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,
    'current_base_item is the newest base_items id with the collection current base item'
);

SELECT results_eq($$
    SELECT get_item('pgstac-test-baseitem-a', 'pgstac-test-baseitems');
    $$,$$
    SELECT snapshot FROM baseitem_snapshot;
    $$,
    'Editing a collection does not change how an already loaded item hydrates'
);

SELECT create_item('{"id": "pgstac-test-baseitem-b", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-baseitems", "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff", "title": "Image"}, "thumbnail": {"href": "https://example.com/a.jpg", "type": "image/jpeg", "title": "Thumbnail"}}, "properties": {"datetime": "2011-06-01T00:00:00Z"}, "stac_extensions": []}');

SELECT results_eq($$
    SELECT (content->>'pgstac:base_item')::int FROM items WHERE id='pgstac-test-baseitem-b';
    $$,$$
    SELECT id FROM base_items WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1;
    $$,
    'An item loaded after an edit is tagged with the current base item'
);

SELECT results_eq($$
    SELECT get_item('pgstac-test-baseitem-b', 'pgstac-test-baseitems') - 'id';
    $$,$$
    SELECT snapshot - 'id' FROM baseitem_snapshot;
    $$,
    'An item loaded after an edit hydrates to the same content as one loaded before'
);

SELECT results_eq($$
    SELECT count(*) FROM jsonb_array_elements(
        search('{"collections": ["pgstac-test-baseitems"]}')->'features'
    ) f WHERE f - 'id' = (SELECT snapshot - 'id' FROM baseitem_snapshot);
    $$,$$
    SELECT 2::bigint;
    $$,
    'Hydrated search returns both items correctly'
);

SELECT results_eq($$
    SELECT bool_and(
        CASE f->>'id'
            -- Loaded before any edit, so its base item is the first one, NOT the current one.
            -- An implementation that ignored the tag would hand both items the current one.
            WHEN 'pgstac-test-baseitem-a' THEN f->'pgstac:base_item' = (
                SELECT base_item FROM base_items
                WHERE collection='pgstac-test-baseitems' ORDER BY id ASC LIMIT 1
            )
            ELSE f->'pgstac:base_item' = (
                SELECT base_item FROM base_items
                WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1
            )
        END
    ) FROM jsonb_array_elements(
        search('{"collections": ["pgstac-test-baseitems"], "conf": {"nohydrate": true}}')->'features'
    ) f;
    $$,$$
    SELECT true;
    $$,
    'A nohydrate search carries each item''s own base item, the first one when untagged'
);

SELECT results_eq($$
    SELECT collection_base_item('pgstac-test-baseitems');
    $$,$$
    SELECT base_item FROM base_items WHERE collection='pgstac-test-baseitems' ORDER BY id ASC LIMIT 1;
    $$,
    'The one argument call form returns the initial base item'
);

SELECT results_eq($$
    SELECT collection_base_item(
        'pgstac-test-baseitems',
        (SELECT id FROM base_items WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1)
    );
    $$,$$
    SELECT base_item FROM collections WHERE id='pgstac-test-baseitems';
    $$,
    'The two argument call form returns the base item the tag names'
);

SELECT update_collection(jsonb_set(content, '{stac_version}', '"1.1.0"'))
FROM collections WHERE id='pgstac-test-baseitems';

SELECT results_eq($$
    SELECT count(*) FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,$$
    SELECT 3::bigint;
    $$,
    'A second base item edit records only the new base item'
);

-- The tag now names a superseded base item, so resolving it and ignoring it give different
-- answers. Every assertion before this point holds either way.
SELECT results_eq($$
    SELECT get_item('pgstac-test-baseitem-b', 'pgstac-test-baseitems') - 'id';
    $$,$$
    SELECT snapshot - 'id' FROM baseitem_snapshot;
    $$,
    'A tagged item still hydrates against its own base item after a later edit'
);

SELECT results_eq($$
    SELECT get_item('pgstac-test-baseitem-a', 'pgstac-test-baseitems');
    $$,$$
    SELECT snapshot FROM baseitem_snapshot;
    $$,
    'An untagged item still hydrates against the first base item after a later edit'
);

SELECT create_item('{"id": "pgstac-test-baseitem-d", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-baseitems", "pgstac:base_item": 42, "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff", "title": "Image"}}, "properties": {"datetime": "2011-06-01T00:00:00Z"}, "stac_extensions": []}');

SELECT results_eq($$
    SELECT (content->>'pgstac:base_item')::int FROM items WHERE id='pgstac-test-baseitem-d';
    $$,$$
    SELECT id FROM base_items WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1;
    $$,
    'A tag supplied by the caller is discarded and replaced'
);

-- Item d is tagged with the current base item, so this is what hydrating
-- against collections.base_item yields.
CREATE TEMP TABLE baseitem_d_snapshot AS
SELECT get_item('pgstac-test-baseitem-d', 'pgstac-test-baseitems') AS snapshot;

UPDATE items SET content = content || '{"pgstac:base_item": 999999}'::jsonb
WHERE id='pgstac-test-baseitem-d' AND collection='pgstac-test-baseitems';

SELECT results_eq($$
    SELECT get_item('pgstac-test-baseitem-d', 'pgstac-test-baseitems');
    $$,$$
    SELECT snapshot FROM baseitem_d_snapshot;
    $$,
    'An item tagged with a base item that does not exist hydrates against the collection current base item'
);

SELECT lives_ok(
    $$ SELECT get_item('pgstac-test-baseitem-d', 'pgstac-test-baseitems') $$,
    'A base item tag that does not exist degrades to the current base item rather than failing'
);

UPDATE items SET content = content || '{"pgstac:base_item": "notanint"}'::jsonb
WHERE id='pgstac-test-baseitem-d' AND collection='pgstac-test-baseitems';

SELECT results_eq($$
    SELECT get_item('pgstac-test-baseitem-d', 'pgstac-test-baseitems');
    $$,$$
    SELECT snapshot FROM baseitem_d_snapshot;
    $$,
    'An item whose tag is not an integer degrades to the current base item instead of aborting the page'
);

-- get_tstz_constraint pins the deparse. Unpinned, pg_get_constraintdef renders 01/02/2020 here,
-- the ISO-only pattern reads that as NULL, and the partition silently widens to infinity, losing
-- both pruning and its statistics.
SET DateStyle TO 'SQL, DMY';
SELECT lives_ok(
    $$ SELECT update_partition_stats(partition, true) FROM partition_sys_meta
       WHERE collection = 'pgstac-test-collection' LIMIT 1 $$,
    'partition statistics can be recalculated under a non ISO DateStyle'
);
SELECT isnt(
    (SELECT constraint_dtrange FROM partition_sys_meta
      WHERE collection = 'pgstac-test-collection' LIMIT 1),
    tstzrange('-infinity', 'infinity', '[]'),
    'the constraint it wrote reads back as a bounded range, not as unbounded'
);
RESET DateStyle;

-- A catalog partitioned by a session that was not in UTC has partitions on local month
-- boundaries. Nothing rewrites them, so they have to keep taking new records: a UTC-aligned
-- partition built beside one would overlap it and stop ingest entirely.
SELECT create_collection('{"id": "pgstac-test-tzpart", "type": "Collection", "stac_version": "1.0.0", "description": "legacy partition alignment", "license": "proprietary", "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}, "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}}, "links": []}');
UPDATE collections SET partition_trunc = 'month' WHERE id = 'pgstac-test-tzpart';
DO $tz$
DECLARE
    parent text;
BEGIN
    SELECT format('_items_%s', key) INTO parent FROM collections WHERE id = 'pgstac-test-tzpart';
    -- Built by hand rather than through check_partition, which buckets in UTC: these are the
    -- bounds an America/New_York session produced for January 2020.
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF items FOR VALUES IN (%L) PARTITION BY RANGE (datetime)',
        parent, 'pgstac-test-tzpart');
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        parent || '_202001', parent,
        '2020-01-01 05:00:00+00'::timestamptz, '2020-02-01 05:00:00+00'::timestamptz);
    -- check_partition would have made these, so they would be the schema owner's, and it records
    -- the partition and its real bounds in partition_stats as it goes.
    EXECUTE format('ALTER TABLE %I OWNER TO pgstac_admin', parent);
    EXECUTE format('ALTER TABLE %I OWNER TO pgstac_admin', parent || '_202001');
    INSERT INTO partition_stats (partition, collection, partition_dtrange)
    VALUES (
        parent || '_202001', 'pgstac-test-tzpart',
        tstzrange('2020-01-01 05:00:00+00'::timestamptz, '2020-02-01 05:00:00+00'::timestamptz, '[)')
    );
END
$tz$;

-- partition_bound_expr pins the deparse: unpinned, pg_get_expr renders 01/01/2020 05:00:00 UTC
-- here, constraint_tstzrange reads that as NULL and the partition silently looks unbounded.
SET DateStyle TO 'SQL, DMY';
SELECT is(
    (SELECT partition_dtrange FROM partition_sys_meta WHERE collection = 'pgstac-test-tzpart'),
    tstzrange('2020-01-01 05:00:00+00'::timestamptz, '2020-02-01 05:00:00+00'::timestamptz, '[)'),
    'a partition bound reads back as its real range under a non ISO DateStyle'
);
RESET DateStyle;

SELECT lives_ok($$
    SELECT create_item('{"id": "tzitem", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-tzpart", "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {}, "properties": {"datetime": "2020-02-01T02:00:00Z"}, "stac_extensions": []}');
$$, 'an item whose UTC month differs from a legacy partition still loads');

SELECT is(
    (SELECT tableoid::regclass::text FROM items
      WHERE id = 'tzitem' AND collection = 'pgstac-test-tzpart'),
    (SELECT '_items_' || key || '_202001' FROM collections WHERE id = 'pgstac-test-tzpart'),
    'the item lands in the legacy partition that already covers it'
);

SELECT is(
    (SELECT count(*)::int FROM partition_stats WHERE collection = 'pgstac-test-tzpart'),
    1,
    'no second, UTC-aligned partition is built beside the legacy one'
);

SELECT delete_collection('pgstac-test-tzpart');

SELECT delete_collection('pgstac-test-baseitems');

SELECT is_empty($$
    SELECT * FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,
    'Deleting a collection removes its base_items rows'
);

SELECT create_collection('{"id": "pgstac-test-baseitems", "type": "Collection", "stac_version": "1.0.0", "description": "base item versioning", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2011-01-01T00:00:00Z", "2011-12-31T00:00:00Z"]]}}, "item_assets": {"image": {"type": "image/tiff", "title": "Image"}}}');

SELECT is_empty($$
    SELECT * FROM base_items WHERE collection='pgstac-test-baseitems';
    $$,
    'Recreating a collection leaves no base_items rows behind'
);

SELECT delete_collection('pgstac-test-baseitems');
DROP TABLE baseitem_snapshot;
DROP TABLE baseitem_d_snapshot;

-- A dotted queryable has to get an index the planner can match to the cql2 filter.

SELECT create_item('{"id": "pgstac-test-item-dotted", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-collection", "bbox": [0, 0, 1, 1], "links": [], "assets": {}, "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "properties": {"datetime": "2011-06-01T00:00:00Z", "test:detail": {"value": "x"}}, "stac_extensions": []}');

SELECT lives_ok(
    $$ SELECT upsert_queryable(
           'test:detail.value',
           definition => '{"type":"string"}'::jsonb,
           property_wrapper => 'to_text',
           property_index_type => 'BTREE'
       ); $$,
    'Can register a dotted queryable that asks for an index.'
);

SELECT results_eq(
    $$ SELECT count(*)::int FROM pgstac_indexes WHERE field = 'test:detail.value'; $$,
    $$ SELECT 1; $$,
    'exactly one index is built and recognised for the dotted queryable'
);

SELECT is_empty(
    $$ SELECT field FROM queryable_indexes('items', true) WHERE field = 'test:detail.value'; $$,
    'the dotted queryable index is not reported as a pending change'
);

SELECT lives_ok(
    $$ SELECT maintain_partitions(); $$,
    'maintain_partitions runs again'
);

SELECT results_eq(
    $$ SELECT count(*)::int FROM pg_indexes
       WHERE schemaname='pgstac' AND tablename ~ '_items_' AND indexdef LIKE '%test:detail%'; $$,
    $$ SELECT 1; $$,
    'a second maintain_partitions run does not add a duplicate index'
);

CREATE FUNCTION pg_temp.dotted_index_used() RETURNS boolean AS $f$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE $q$EXPLAIN (format json) SELECT 1 FROM items WHERE to_text(content->'properties'->'test:detail'->'value') = 'x'$q$ INTO plan;
    RETURN plan @? '$.**."Index Cond" ? (@ like_regex "test:detail")';
END;
$f$ LANGUAGE PLPGSQL SET enable_seqscan TO off;

SELECT ok(
    pg_temp.dotted_index_used(),
    'the planner uses the dotted queryable index instead of scanning'
);

SELECT delete_item('pgstac-test-item-dotted', 'pgstac-test-collection');
SELECT delete_queryable('test:detail.value');

-- GIN and BRIN queryable indexes, and the plans the cql2 array operators get.

SELECT create_collection('{"id": "pgstac-test-idxtypes", "type": "Collection", "stac_version": "1.0.0", "description": "gin and brin index types", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2011-01-01T00:00:00Z", "2011-12-31T00:00:00Z"]]}}}');

SELECT create_items((
    SELECT jsonb_agg(jsonb_build_object(
        'id', 'pgstac-test-idxtypes-' || i,
        'type', 'Feature',
        'stac_version', '1.0.0',
        'collection', 'pgstac-test-idxtypes',
        'links', '[]'::jsonb,
        'assets', '{}'::jsonb,
        'bbox', '[0,0,1,1]'::jsonb,
        'geometry', '{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}'::jsonb,
        'stac_extensions', '[]'::jsonb,
        'properties', jsonb_build_object(
            'datetime', '2011-06-01T00:00:00Z',
            'test:instruments', jsonb_build_array('band' || (i % 7), 'common'),
            'test:platforms', jsonb_build_array('sat' || (i % 3), 'common'),
            'test:num', i
        )
    ))
    FROM generate_series(1, 1000) i
));

SELECT lives_ok(
    $$ SELECT upsert_queryable(
           'test:instruments',
           definition => '{"type":"array"}'::jsonb,
           property_wrapper => 'to_text_array',
           property_index_type => 'GIN',
           collection_ids => ARRAY['pgstac-test-idxtypes']
       );
       SELECT upsert_queryable(
           'test:num',
           definition => '{"type":"integer"}'::jsonb,
           property_wrapper => 'to_int',
           property_index_type => 'BRIN',
           collection_ids => ARRAY['pgstac-test-idxtypes']
       ); $$,
    'Can register queryables that ask for GIN and BRIN indexes.'
);

SELECT results_eq(
    $$ SELECT am.amname::text COLLATE "default" FROM pgstac_indexes i
        JOIN pg_class c ON (c.relname = i.indexname AND c.relnamespace = 'pgstac'::regnamespace)
        JOIN pg_am am ON (am.oid = c.relam)
        WHERE i.field = 'test:instruments'; $$,
    $$ SELECT 'gin'::text; $$,
    'the GIN queryable gets exactly one index and it is built with the gin access method'
);

SELECT results_eq(
    $$ SELECT am.amname::text COLLATE "default" FROM pgstac_indexes i
        JOIN pg_class c ON (c.relname = i.indexname AND c.relnamespace = 'pgstac'::regnamespace)
        JOIN pg_am am ON (am.oid = c.relam)
        WHERE i.field = 'test:num'; $$,
    $$ SELECT 'brin'::text; $$,
    'the BRIN queryable gets exactly one index and it is built with the brin access method'
);

SELECT is_empty(
    $$ SELECT field FROM queryable_indexes('items', true) WHERE field IN ('test:instruments', 'test:num'); $$,
    'the GIN and BRIN indexes read back as matching their queryables'
);

SELECT lives_ok(
    $$ SELECT maintain_partitions(); $$,
    'maintain_partitions runs again over the GIN and BRIN queryables'
);

SELECT results_eq(
    $$ SELECT count(*)::int FROM pgstac_indexes WHERE field IN ('test:instruments', 'test:num'); $$,
    $$ SELECT 2; $$,
    'a second maintain_partitions run does not add a duplicate GIN or BRIN index'
);

-- Asks the planner which index the cql2 filter lands on, with seqscan off so an
-- unusable index shows up as a seq scan rather than a cost preference.
CREATE FUNCTION pg_temp.plan_uses_index(_filter text, _indexname text) RETURNS boolean AS $f$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE format(
        $q$EXPLAIN (format json) SELECT 1 FROM items WHERE collection = 'pgstac-test-idxtypes' AND %s$q$,
        _filter
    ) INTO plan;
    RETURN plan @? format('$.**."Index Name" ? (@ == %s)', to_json(_indexname))::jsonpath;
END;
$f$ LANGUAGE PLPGSQL SET enable_seqscan TO off;

SELECT ok(
    pg_temp.plan_uses_index(
        cql2_query('{"op":"a_contains","args":[{"property":"test:instruments"},["band1"]]}'::jsonb),
        (SELECT indexname FROM pgstac_indexes WHERE field = 'test:instruments')
    ),
    'a_contains on a GIN queryable is planned as an index scan on that GIN index'
);

SELECT ok(
    pg_temp.plan_uses_index(
        cql2_query('{"op":"a_overlaps","args":[{"property":"test:instruments"},["band1","band2"]]}'::jsonb),
        (SELECT indexname FROM pgstac_indexes WHERE field = 'test:instruments')
    ),
    'a_overlaps on a GIN queryable is planned as an index scan on that GIN index'
);

SELECT ok(
    pg_temp.plan_uses_index(
        cql2_query('{"op":"a_contained_by","args":[{"property":"test:instruments"},["band1","common","extra"]]}'::jsonb),
        (SELECT indexname FROM pgstac_indexes WHERE field = 'test:instruments')
    ),
    'a_contained_by on a GIN queryable is planned as an index scan on that GIN index'
);

SELECT ok(
    pg_temp.plan_uses_index(
        cql2_query('{"op":"a_equals","args":[{"property":"test:instruments"},["band1","common"]]}'::jsonb),
        (SELECT indexname FROM pgstac_indexes WHERE field = 'test:instruments')
    ),
    'a_equals on a GIN queryable is planned as an index scan on that GIN index'
);

SELECT ok(
    pg_temp.plan_uses_index(
        cql2_query('{"op":"<","args":[{"property":"test:num"},100]}'::jsonb),
        (SELECT indexname FROM pgstac_indexes WHERE field = 'test:num')
    ),
    'a range filter on a BRIN queryable is planned as an index scan on that BRIN index'
);

-- An array queryable with no property_wrapper: the wrapper has to be inferred as
-- to_text_array, or the GIN index has no operator class and the filters mistype.
SELECT lives_ok(
    $$ SELECT upsert_queryable(
           'test:platforms',
           definition => '{"type":"array","items":{"type":"string"}}'::jsonb,
           property_index_type => 'GIN',
           collection_ids => ARRAY['pgstac-test-idxtypes']
       );
       SELECT maintain_partitions(); $$,
    'Can register an array queryable that asks for a GIN index without spelling out the wrapper.'
);

SELECT results_eq(
    $$ SELECT am.amname::text COLLATE "default" FROM pgstac_indexes i
        JOIN pg_class c ON (c.relname = i.indexname AND c.relnamespace = 'pgstac'::regnamespace)
        JOIN pg_am am ON (am.oid = c.relam)
        WHERE i.field = 'test:platforms'; $$,
    $$ SELECT 'gin'::text; $$,
    'the wrapperless array queryable gets exactly one index and it is built with the gin access method'
);

SELECT ok(
    pg_temp.plan_uses_index(
        cql2_query('{"op":"a_contains","args":[{"property":"test:platforms"},["sat1"]]}'::jsonb),
        (SELECT indexname FROM pgstac_indexes WHERE field = 'test:platforms')
    ),
    'a_contains on a wrapperless array queryable is planned as an index scan on that GIN index'
);

SELECT results_eq(
    $$ SELECT BTRIM(stac_search_to_where('{"filter-lang":"cql2-json","filter":{"op":"a_contains","args":[{"property":"test:platforms"},["sat1"]]}}'::jsonb), E' \n'); $$,
    $$ SELECT $e$to_text_array(content->'properties'->'test:platforms') @> '{sat1}'$e$; $$,
    'an a_contains filter on a wrapperless array queryable reads the property as a text array'
);

SELECT results_eq(
    $$ SELECT BTRIM(stac_search_to_where('{"filter-lang":"cql2-json","filter":{"op":"eq","args":[{"property":"test:platforms"},"sat1"]}}'::jsonb), E' \n'); $$,
    $$ SELECT $e$to_text_array(content->'properties'->'test:platforms') = to_text_array('"sat1"')$e$; $$,
    'an equality filter on a wrapperless array queryable wraps both sides with the inferred to_text_array'
);

DELETE FROM queryables WHERE name IN ('test:instruments', 'test:num', 'test:platforms');
SELECT delete_collection('pgstac-test-idxtypes');
