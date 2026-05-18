SELECT has_table('pgstac'::name, 'items'::name);
SELECT has_table('pgstac'::name, 'item_fragments'::name);
SELECT has_table('pgstac'::name, 'item_field_registry'::name);
SELECT has_table('pgstac'::name, 'items_deleted_log'::name);


SELECT is_indexed('pgstac'::name, 'items'::name, 'geometry');

SELECT is_partitioned('pgstac'::name,'items'::name);


SELECT has_function('pgstac'::name, 'get_item', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'delete_item', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'create_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'create_items', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_items', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'extract_fragment', ARRAY['jsonb', 'text[]']);
SELECT has_function('pgstac'::name, 'get_or_create_fragment', ARRAY['jsonb', 'text', 'text[]']);
SELECT has_function('pgstac'::name, 'gc_fragments', ARRAY['text', 'interval']);
SELECT has_function('pgstac'::name, 'update_field_registry_from_sample', ARRAY['text', 'jsonb[]']);
SELECT has_function('pgstac'::name, 'update_field_registry_from_items', ARRAY['text']);
SELECT has_function('pgstac'::name, 'refresh_field_registry', ARRAY['text', 'interval']);
SELECT has_function('pgstac'::name, 'gc_deleted_items_log', ARRAY['interval']);
SELECT has_function('pgstac'::name, 'gc_deleted_items_log', ARRAY['interval', 'integer']);
SELECT has_function('pgstac'::name, 'gc_deleted_items_log_batch', ARRAY['interval', 'integer']);


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

SELECT ok(
    (SELECT pgstac_updated_at IS NOT NULL FROM items WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'),
    'create_item populates pgstac_updated_at'
);
SELECT ok(
    (SELECT length(content_hash) = 64 FROM items WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'),
    'create_item generates sha256 content_hash'
);

SELECT update_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 29, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');

SELECT results_eq($$
    SELECT content->'properties'->>'eo:cloud_cover' FROM items WHERE collection='pgstac-test-collection';
    $$,$$
    SELECT '29';
    $$,
    'Test update_item function'
);

SELECT results_eq($$
    WITH old_row AS (
        SELECT pgstac_updated_at FROM items WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'
    ),
    updated AS (
        UPDATE items
        SET private = '{}'::jsonb
        WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'
        RETURNING pgstac_updated_at
    )
    SELECT (SELECT pgstac_updated_at FROM updated) >= (SELECT pgstac_updated_at FROM old_row);
    $$,$$
    SELECT TRUE;
    $$,
    'updates refresh pgstac_updated_at through items_touch_triggerfunc'
);
SELECT ok(
    (SELECT length(content_hash) = 64 FROM items WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'),
    'update path generates new sha256 content_hash'
);

select delete_item('pgstac-test-item-0003');

SELECT results_eq($$
    SELECT count(*) FROM items WHERE collection='pgstac-test-collection';
    $$,$$
    SELECT 0::bigint;
    $$,
    'Test delete_item function'
);

SELECT ok(
    EXISTS (
        SELECT 1
        FROM items_deleted_log
        WHERE item_id='pgstac-test-item-0003' AND collection='pgstac-test-collection'
    ),
    'delete_item writes tombstone rows to items_deleted_log'
);

SELECT lives_ok($$
    UPDATE items_deleted_log
    SET deleted_at = now() - '40 days'::interval
    WHERE item_id='pgstac-test-item-0003' AND collection='pgstac-test-collection';
$$, 'Age tombstone rows for gc_deleted_items_log test');

SELECT results_eq($$
    SELECT gc_deleted_items_log('30 days'::interval) > 0;
    $$,$$
    SELECT TRUE;
    $$,
    'gc_deleted_items_log removes aged tombstones'
);

SELECT lives_ok($$
    INSERT INTO items_deleted_log (
        item_id,
        collection,
        partition,
        datetime,
        end_datetime,
        content_hash,
        deleted_at
    )
    VALUES (
        'pgstac-test-item-0003',
        'pgstac-test-collection',
        NULL,
        now() - '41 days'::interval,
        now() - '41 days'::interval,
        repeat('a', 64),
        now() - '40 days'::interval
    );
$$, 'Insert aged tombstone row for batched gc_deleted_items_log test');

SELECT results_eq($$
    SELECT gc_deleted_items_log('30 days'::interval, 1) > 0;
    $$,$$
    SELECT TRUE;
    $$,
    'gc_deleted_items_log(interval, integer) removes aged tombstones in batches'
);

SELECT create_item('{
    "id": "pgstac-test-item-0004",
    "bbox": [-85.379245, 30.933949, -85.308201, 31.003555],
    "type": "Feature",
    "links": [],
    "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff"}},
    "geometry": {"type": "Point", "coordinates": [-85.309412, 30.933949]},
    "collection": "pgstac-test-collection",
    "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 31, "gsd": 5},
    "stac_version": "1.0.0"
}');

SELECT create_item('{
    "id": "pgstac-test-item-0005",
    "bbox": [-85.379245, 30.933949, -85.308201, 31.003555],
    "type": "Feature",
    "links": [],
    "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff"}},
    "geometry": {"type": "Point", "coordinates": [-85.309500, 30.934000]},
    "collection": "pgstac-test-collection",
    "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 31, "gsd": 5},
    "stac_version": "1.0.0"
}');

SELECT ok(
    (SELECT fragment_id IS NOT NULL FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item assigns fragment_id for split-storage rows'
);
SELECT ok(
    (SELECT bbox = '[-85.379245, 30.933949, -85.308201, 31.003555]'::jsonb FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item stores bbox in split column'
);
SELECT ok(
    (SELECT links = '[]'::jsonb FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item stores links in split column'
);
SELECT ok(
    (SELECT assets ? 'image' FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item stores assets in split column'
);
SELECT ok(
    (SELECT eo_cloud_cover = 31 FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item stores promoted columns'
);
SELECT results_eq($$
    SELECT count(DISTINCT fragment_id)::bigint
    FROM items
    WHERE id IN ('pgstac-test-item-0004', 'pgstac-test-item-0005')
      AND collection='pgstac-test-collection';
    $$,$$
    SELECT 1::bigint;
    $$,
    'identical split-storage items share one fragment_id'
);
SELECT results_eq($$
    SELECT get_item('pgstac-test-item-0004', 'pgstac-test-collection')->'properties'->>'eo:cloud_cover';
    $$,$$
    SELECT '31';
    $$,
    'get_item hydrates split-storage properties'
);
SELECT results_eq($$
    SELECT get_item('pgstac-test-item-0004', 'pgstac-test-collection')->'bbox';
    $$,$$
    SELECT '[-85.379245, 30.933949, -85.308201, 31.003555]'::jsonb;
    $$,
    'get_item hydrates split-storage bbox'
);

SELECT lives_ok($$
    DELETE FROM item_field_registry
    WHERE collection='pgstac-test-collection'
      AND path='registry_probe';
$$, 'clear registry probe row');

SELECT lives_ok($$
    SELECT update_field_registry_from_sample(
        'pgstac-test-collection',
        ARRAY['{"registry_probe":1}'::jsonb]
    );
$$, 'register numeric field kind from explicit sample');

SELECT lives_ok($$
    SELECT update_field_registry_from_sample(
        'pgstac-test-collection',
        ARRAY['{"registry_probe":"one"}'::jsonb]
    );
$$, 'merge string field kind from a second explicit sample');

SELECT results_eq($$
    SELECT string_agg(v, ',' ORDER BY v)
    FROM item_field_registry r,
         unnest(r.value_kinds) AS v
    WHERE r.collection='pgstac-test-collection'
      AND r.path='registry_probe';
    $$,$$
    SELECT 'number,string';
    $$,
    'update_field_registry_from_sample merges fresh value kinds without throttling away updates'
);

SELECT results_eq($$
    SELECT registered_paths > 1
    FROM update_field_registry_from_items('pgstac-test-collection');
    $$,$$
    SELECT TRUE;
    $$,
    'update_field_registry_from_items returns the true registered path count'
);
SELECT ok(
    EXISTS (
        SELECT 1
        FROM item_field_registry
        WHERE collection='pgstac-test-collection'
          AND path='properties.eo:cloud_cover'
    ),
    'update_field_registry_from_items records nested property paths'
);

SELECT lives_ok($$
    UPDATE item_fragments
    SET created_at = now() - '100 days'::interval
    WHERE id IN (
        SELECT DISTINCT fragment_id
        FROM items
        WHERE id IN ('pgstac-test-item-0004', 'pgstac-test-item-0005')
          AND collection='pgstac-test-collection'
    );
$$, 'age active fragment rows for gc_fragments test');

SELECT delete_item('pgstac-test-item-0004', 'pgstac-test-collection');
SELECT delete_item('pgstac-test-item-0005', 'pgstac-test-collection');

SELECT results_eq($$
    SELECT COALESCE(sum(fragments_removed), 0) > 0
    FROM gc_fragments('pgstac-test-collection', '90 days'::interval);
    $$,$$
    SELECT TRUE;
    $$,
    'gc_fragments removes orphaned dedup rows'
);

SELECT lives_ok($$
    WITH raw AS (
        SELECT '{
            "id": "pgstac-test-item-legacy",
            "bbox": [-85.0, 30.0, -84.0, 31.0],
            "type": "Feature",
            "links": [],
            "assets": {"image": {"href": "https://example.com/legacy.tif", "type": "image/tiff"}},
            "geometry": {"type": "Point", "coordinates": [-85.0, 30.0]},
            "collection": "pgstac-test-collection",
            "properties": {"datetime": "2012-01-01T00:00:00Z", "eo:cloud_cover": 44},
            "stac_version": "1.0.0"
        }'::jsonb AS content
    ),
    dehydrated AS (
        SELECT content_dehydrate(content) AS item FROM raw
    )
    INSERT INTO items (
        id,
        geometry,
        collection,
        datetime,
        end_datetime,
        pgstac_updated_at,
        content_hash,
        content,
        private
    )
    SELECT
        (item).id,
        (item).geometry,
        (item).collection,
        (item).datetime,
        (item).end_datetime,
        (item).pgstac_updated_at,
        (item).content_hash,
        (item).content,
        (item).private
    FROM dehydrated;
$$, 'insert a legacy-style row without split columns');

SELECT ok(
    (SELECT fragment_id IS NULL FROM items WHERE id='pgstac-test-item-legacy' AND collection='pgstac-test-collection'),
    'legacy rows keep a NULL fragment_id'
);
SELECT results_eq($$
    SELECT get_item('pgstac-test-item-legacy', 'pgstac-test-collection')->'properties'->>'eo:cloud_cover';
    $$,$$
    SELECT '44';
    $$,
    'legacy rows still hydrate through the content fallback path'
);

SELECT delete_item('pgstac-test-item-legacy', 'pgstac-test-collection');
