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
SELECT results_eq($$
        SELECT count(*)::bigint
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgstac'
            AND p.proname = 'get_or_create_fragment'
        $$,$$
        SELECT 0::bigint
        $$,
        'get_or_create_fragment remains intentionally removed after split-storage cleanup'
);
SELECT has_function('pgstac'::name, 'gc_fragments', ARRAY['text', 'interval']);
SELECT has_function('pgstac'::name, 'promoted_items_column_list', '{}'::text[]);
SELECT has_function('pgstac'::name, 'items_content_distinct_sql', ARRAY['text', 'text']);
SELECT has_function('pgstac'::name, 'items_content_changed', ARRAY['items', 'items']);
SELECT has_function('pgstac'::name, 'update_field_registry_from_sample', ARRAY['text', 'jsonb[]']);
SELECT has_function('pgstac'::name, 'update_field_registry_from_items', ARRAY['text']);
SELECT has_function('pgstac'::name, 'refresh_field_registry', ARRAY['text', 'interval']);
SELECT has_function('pgstac'::name, 'gc_deleted_items_log', ARRAY['interval']);
SELECT has_function('pgstac'::name, 'gc_deleted_items_log', ARRAY['interval', 'integer']);
SELECT has_function('pgstac'::name, 'gc_deleted_items_log_batch', ARRAY['interval', 'integer']);

SELECT results_eq($$
    SELECT to_jsonb(array_agg(column_name::text ORDER BY ordinal_position))
        FROM information_schema.columns
        WHERE table_schema = 'pgstac'
            AND table_name = 'items'
            AND column_name = ANY(pgstac.promoted_items_column_list())
        $$,$$
    SELECT to_jsonb(pgstac.promoted_items_column_list())
        $$,
        'promoted_items_column_list stays in sync with the promoted columns on items'
);

-- tools to update collection extents based on extents in items
SELECT has_function('pgstac'::name, 'collection_bbox', ARRAY['text']);
SELECT has_function('pgstac'::name, 'collection_temporal_extent', ARRAY['text']);
SELECT has_function('pgstac'::name, 'update_collection_extents', '{}'::text[]);

DELETE FROM collections WHERE id in ('pgstac-test-collection', 'pgstac-test-collection2');
\copy collections (content) FROM 'tests/testdata/collections.ndjson';
-- \copy bypasses create_collection so fragment_config is NULL; populate it now.
UPDATE collections SET fragment_config = collection_fragment_config_default(content) WHERE fragment_config IS NULL;

SELECT create_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 28, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');

SELECT results_eq($$
    SELECT get_item('pgstac-test-item-0003', 'pgstac-test-collection')->'properties'->>'eo:cloud_cover';
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
    (SELECT octet_length(item_hash) = 32 FROM items WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'),
    'create_item generates sha256 item_hash'
);

SELECT update_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 29, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');

SELECT results_eq($$
    SELECT get_item('pgstac-test-item-0003', 'pgstac-test-collection')->'properties'->>'eo:cloud_cover';
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
        SET extra = '{}'::jsonb
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
    (SELECT octet_length(item_hash) = 32 FROM items WHERE id='pgstac-test-item-0003' AND collection='pgstac-test-collection'),
    'update path generates new sha256 item_hash'
);

SELECT results_eq(
    $$ SELECT tstz_to_stac_text('2026-01-01 00:00:00+00'::timestamptz) $$,
    $$ SELECT '2026-01-01T00:00:00Z' $$,
    'tstz_to_stac_text omits fractional seconds when they are all zero'
);

SELECT results_eq(
    $$ SELECT tstz_to_stac_text('2026-01-01 00:00:00.120000+00'::timestamptz) $$,
    $$ SELECT '2026-01-01T00:00:00.12Z' $$,
    'tstz_to_stac_text trims only trailing fractional zeros'
);

SELECT results_eq(
    $$ SELECT tstz_to_stac_text('2026-01-01 01:30:00+01'::timestamptz) $$,
    $$ SELECT '2026-01-01T00:30:00Z' $$,
    'tstz_to_stac_text canonicalizes offset timestamps to UTC'
);

SELECT results_eq(
    $$ SELECT fragment_path_array(fragment_path_text(ARRAY['assets', 'my.asset', 'href'])) $$,
    $$ SELECT ARRAY['assets', 'my.asset', 'href']::text[] $$,
    'fragment path serialization round-trips keys containing dots'
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
        item_hash,
        deleted_at
    )
    VALUES (
        'pgstac-test-item-0003',
        'pgstac-test-collection',
        NULL,
        now() - '41 days'::interval,
        now() - '41 days'::interval,
        decode(repeat('aa', 32), 'hex'),
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
    (SELECT links IS NULL FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item stores NULL links in split column for fragment-backed rows'
);
SELECT ok(
    (SELECT get_item('pgstac-test-item-0004', 'pgstac-test-collection') -> 'assets' ? 'image'),
    'create_item stores assets accessible via hydrated item'
);
SELECT ok(
    (SELECT eo_cloud_cover = 31 FROM items WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'),
    'create_item stores promoted columns'
);
SELECT results_eq($$
    WITH old_row AS (
        SELECT pgstac_updated_at, item_hash
        FROM items
        WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'
    ), rewritten AS (
        UPDATE items
        SET datetime = datetime + interval '1 second'
        WHERE id='pgstac-test-item-0004' AND collection='pgstac-test-collection'
        RETURNING pgstac_updated_at, item_hash
    )
    -- pgstac_updated_at refreshes, but item_hash is the canonical hash of the
    -- item *as ingested* and must NOT change on a direct UPDATE (it stays
    -- externally reproducible). Re-ingest via upsert_item to refresh.
    SELECT (SELECT pgstac_updated_at FROM rewritten) >= (SELECT pgstac_updated_at FROM old_row)
        AND (SELECT item_hash FROM rewritten) = (SELECT item_hash FROM old_row);
    $$,$$
    SELECT TRUE;
    $$,
    'items_touch_triggerfunc refreshes pgstac_updated_at but leaves item_hash stable on direct updates'
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
SELECT lives_ok($$
    UPDATE collections
    SET fragment_config = ARRAY['["properties","eo:cloud_cover"]']
    WHERE id='pgstac-test-collection';
$$, 'set collection fragment_config for dedup granularity test');

SELECT create_item('{
    "id": "pgstac-test-fragment-key-0001",
    "bbox": [-85.379245, 30.933949, -85.308201, 31.003555],
    "type": "Feature",
    "links": [],
    "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff"}},
    "geometry": {"type": "Point", "coordinates": [-85.309600, 30.934100]},
    "collection": "pgstac-test-collection",
    "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 31, "gsd": 5},
    "stac_version": "1.0.0"
}');

SELECT create_item('{
    "id": "pgstac-test-fragment-key-0002",
    "bbox": [-85.379245, 30.933949, -85.308201, 31.003555],
    "type": "Feature",
    "links": [],
    "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff"}},
    "geometry": {"type": "Point", "coordinates": [-85.309700, 30.934200]},
    "collection": "pgstac-test-collection",
    "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 31, "gsd": 999},
    "stac_version": "1.0.0"
}');

SELECT results_eq($$
    SELECT count(DISTINCT fragment_id)::bigint
    FROM items
    WHERE id IN ('pgstac-test-fragment-key-0001', 'pgstac-test-fragment-key-0002')
      AND collection='pgstac-test-collection';
    $$,$$
    SELECT 1::bigint;
    $$,
    'collection fragment_prop_keys control fragment dedup granularity'
);
SELECT results_eq($$
    SELECT content->'properties'
    FROM item_fragments
    WHERE id = (
        SELECT fragment_id
        FROM items
        WHERE id='pgstac-test-fragment-key-0001' AND collection='pgstac-test-collection'
    );
    $$,$$
    SELECT '{"eo:cloud_cover":31}'::jsonb;
    $$,
    'fragment content keeps only configured properties'
);
SELECT ok(
    NOT COALESCE(
        (
            SELECT content->'properties' ? 'gsd'
            FROM item_fragments
            WHERE id = (
                SELECT fragment_id
                FROM items
                WHERE id='pgstac-test-fragment-key-0001' AND collection='pgstac-test-collection'
            )
        ),
        FALSE
    ),
    'fragment content excludes non-configured properties'
);
SELECT lives_ok($$
        UPDATE collections
        SET fragment_config = collection_fragment_config_default(content)
        WHERE id='pgstac-test-collection';
        DELETE FROM items
        WHERE id IN ('pgstac-test-fragment-key-0001', 'pgstac-test-fragment-key-0002')
            AND collection='pgstac-test-collection';
$$, 'clean up fragment-key tuning fixtures');
SELECT lives_ok($$
    UPDATE collections
    SET fragment_config = ARRAY['["stac_version"]', '["stac_extensions"]']
    WHERE id='pgstac-test-collection';
$$, 'set collection root fragment_config for root-key round-trip test');

SELECT create_item('{
    "id": "pgstac-test-root-fragment-0001",
    "bbox": [-85.379245, 30.933949, -85.308201, 31.003555],
    "type": "Feature",
    "links": [],
    "assets": {"image": {"href": "https://example.com/root-fragment.tif", "type": "image/tiff"}},
    "geometry": {"type": "Point", "coordinates": [-85.309650, 30.934150]},
    "collection": "pgstac-test-collection",
    "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 31, "gsd": 5},
    "stac_version": "1.0.0",
    "stac_extensions": ["proj", "view"]
}');

SELECT results_eq($$
    SELECT stac_version
    FROM items
    WHERE id='pgstac-test-root-fragment-0001' AND collection='pgstac-test-collection';
    $$,$$
    SELECT NULL::text;
    $$,
    'root fragment config strips stac_version from per-item storage'
);
SELECT results_eq($$
    SELECT stac_extensions
    FROM items
    WHERE id='pgstac-test-root-fragment-0001' AND collection='pgstac-test-collection';
    $$,$$
    SELECT '[]'::jsonb;
    $$,
    'root fragment config strips stac_extensions from per-item storage'
);
SELECT results_eq($$
    SELECT content
    FROM item_fragments
    WHERE id = (
        SELECT fragment_id
        FROM items
        WHERE id='pgstac-test-root-fragment-0001' AND collection='pgstac-test-collection'
    );
    $$,$$
    SELECT '{"stac_version":"1.0.0","stac_extensions":["proj","view"]}'::jsonb;
    $$,
    'root fragment content stores configured root keys'
);
SELECT results_eq($$
    SELECT get_item('pgstac-test-root-fragment-0001', 'pgstac-test-collection')->>'stac_version';
    $$,$$
    SELECT '1.0.0';
    $$,
    'get_item hydrates fragment-backed stac_version'
);
SELECT results_eq($$
    SELECT get_item('pgstac-test-root-fragment-0001', 'pgstac-test-collection')->'stac_extensions';
    $$,$$
    SELECT '["proj", "view"]'::jsonb;
    $$,
    'get_item hydrates fragment-backed stac_extensions'
);
SELECT lives_ok($$
        UPDATE collections
        SET fragment_config = collection_fragment_config_default(content)
        WHERE id='pgstac-test-collection';
        DELETE FROM items
        WHERE id='pgstac-test-root-fragment-0001'
            AND collection='pgstac-test-collection';
$$, 'clean up root-fragment fixtures');
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
    UPDATE collections
    SET fragment_config = ARRAY['["assets","image"]']
    WHERE id='pgstac-test-collection';
$$, 'set depth-2 fragment_config guard');

SELECT create_item('{
    "id": "pgstac-test-roundtrip-depth2-0001",
    "bbox": [-85.2, 30.2, -84.2, 31.2],
    "type": "Feature",
    "links": [],
    "assets": {
        "image": {
            "href": "https://example.com/depth2-a.tif",
            "type": "image/tiff",
            "roles": ["data"]
        }
    },
    "geometry": {"type": "Point", "coordinates": [-85.2, 30.2]},
    "collection": "pgstac-test-collection",
    "properties": {
        "datetime": "2012-02-01T00:00:00Z",
        "nested": {"k": "v"}
    },
    "stac_version": "1.0.0"
}');

SELECT results_eq($$
    SELECT get_item('pgstac-test-roundtrip-depth2-0001', 'pgstac-test-collection');
    $$,$$
    SELECT '{
        "id": "pgstac-test-roundtrip-depth2-0001",
        "bbox": [-85.2, 30.2, -84.2, 31.2],
        "type": "Feature",
        "links": [],
        "assets": {
            "image": {
                "href": "https://example.com/depth2-a.tif",
                "type": "image/tiff",
                "roles": ["data"]
            }
        },
        "geometry": {"type": "Point", "coordinates": [-85.2, 30.2]},
        "collection": "pgstac-test-collection",
        "properties": {
            "datetime": "2012-02-01T00:00:00Z",
            "nested": {"k": "v"}
        },
        "stac_version": "1.0.0"
    }'::jsonb;
    $$,
    'round-trip equality holds for depth-2 fragment config'
);

SELECT delete_item('pgstac-test-roundtrip-depth2-0001', 'pgstac-test-collection');

SELECT lives_ok($$
    UPDATE collections
    SET fragment_config = ARRAY['["assets","image","meta","checksum"]', '["properties","nested","level1"]']
    WHERE id='pgstac-test-collection';
$$, 'set depth-3 fragment_config guard');

SELECT create_item('{
    "id": "pgstac-test-roundtrip-depth3-0001",
    "bbox": [-85.21, 30.21, -84.21, 31.21],
    "type": "Feature",
    "links": [],
    "assets": {
        "image": {
            "href": "https://example.com/depth3-a.tif",
            "type": "image/tiff",
            "meta": {"checksum": "abc123", "etag": "keep-local"}
        }
    },
    "geometry": {"type": "Point", "coordinates": [-85.21, 30.21]},
    "collection": "pgstac-test-collection",
    "properties": {
        "datetime": "2012-03-01T00:00:00Z",
        "nested": {"level1": {"shared": true, "local": 7}}
    },
    "stac_version": "1.0.0"
}');

SELECT results_eq($$
    SELECT get_item('pgstac-test-roundtrip-depth3-0001', 'pgstac-test-collection');
    $$,$$
    SELECT '{
        "id": "pgstac-test-roundtrip-depth3-0001",
        "bbox": [-85.21, 30.21, -84.21, 31.21],
        "type": "Feature",
        "links": [],
        "assets": {
            "image": {
                "href": "https://example.com/depth3-a.tif",
                "type": "image/tiff",
                "meta": {"checksum": "abc123", "etag": "keep-local"}
            }
        },
        "geometry": {"type": "Point", "coordinates": [-85.21, 30.21]},
        "collection": "pgstac-test-collection",
        "properties": {
            "datetime": "2012-03-01T00:00:00Z",
            "nested": {"level1": {"shared": true, "local": 7}}
        },
        "stac_version": "1.0.0"
    }'::jsonb;
    $$,
    'round-trip equality holds for depth-3 fragment config'
);

SELECT delete_item('pgstac-test-roundtrip-depth3-0001', 'pgstac-test-collection');

SELECT lives_ok($$
    UPDATE collections
    SET fragment_config = ARRAY['["properties","deep","l1","l2","l3"]']
    WHERE id='pgstac-test-collection';
$$, 'set depth-4 fragment_config guard');

SELECT create_item('{
    "id": "pgstac-test-roundtrip-depth4-0001",
    "bbox": [-85.22, 30.22, -84.22, 31.22],
    "type": "Feature",
    "links": [],
    "assets": {
        "image": {
            "href": "https://example.com/depth4-a.tif",
            "type": "image/tiff"
        }
    },
    "geometry": {"type": "Point", "coordinates": [-85.22, 30.22]},
    "collection": "pgstac-test-collection",
    "properties": {
        "datetime": "2012-04-01T00:00:00Z",
        "deep": {"l1": {"l2": {"l3": {"x": 1, "y": 2}, "other": "k"}}}
    },
    "stac_version": "1.0.0"
}');

SELECT results_eq($$
    SELECT get_item('pgstac-test-roundtrip-depth4-0001', 'pgstac-test-collection');
    $$,$$
    SELECT '{
        "id": "pgstac-test-roundtrip-depth4-0001",
        "bbox": [-85.22, 30.22, -84.22, 31.22],
        "type": "Feature",
        "links": [],
        "assets": {
            "image": {
                "href": "https://example.com/depth4-a.tif",
                "type": "image/tiff"
            }
        },
        "geometry": {"type": "Point", "coordinates": [-85.22, 30.22]},
        "collection": "pgstac-test-collection",
        "properties": {
            "datetime": "2012-04-01T00:00:00Z",
            "deep": {"l1": {"l2": {"l3": {"x": 1, "y": 2}, "other": "k"}}}
        },
        "stac_version": "1.0.0"
    }'::jsonb;
    $$,
    'round-trip equality holds for depth-4 fragment config'
);

SELECT delete_item('pgstac-test-roundtrip-depth4-0001', 'pgstac-test-collection');

SELECT lives_ok($$
    UPDATE collections
    SET fragment_config = ARRAY[
        '["stac_version"]',
        '["stac_extensions"]',
        '["assets","image","meta","checksum"]',
        '["properties","deep","l1","l2","l3"]'
    ]
    WHERE id='pgstac-test-collection';
$$, 'set mixed root+deep fragment_config guard');

SELECT create_item('{
    "id": "pgstac-test-roundtrip-mixed-0001",
    "bbox": [-85.23, 30.23, -84.23, 31.23],
    "type": "Feature",
    "links": [],
    "assets": {
        "image": {
            "href": "https://example.com/mixed-a.tif",
            "type": "image/tiff",
            "meta": {"checksum": "deadbeef", "owner": "local-owner"}
        }
    },
    "geometry": {"type": "Point", "coordinates": [-85.23, 30.23]},
    "collection": "pgstac-test-collection",
    "properties": {
        "datetime": "2012-05-01T00:00:00Z",
        "deep": {"l1": {"l2": {"l3": {"v": 42}, "q": "kept"}}}
    },
    "stac_version": "1.0.0",
    "stac_extensions": ["proj", "view"]
}');

SELECT results_eq($$
    SELECT get_item('pgstac-test-roundtrip-mixed-0001', 'pgstac-test-collection');
    $$,$$
    SELECT '{
        "id": "pgstac-test-roundtrip-mixed-0001",
        "bbox": [-85.23, 30.23, -84.23, 31.23],
        "type": "Feature",
        "links": [],
        "assets": {
            "image": {
                "href": "https://example.com/mixed-a.tif",
                "type": "image/tiff",
                "meta": {"checksum": "deadbeef", "owner": "local-owner"}
            }
        },
        "geometry": {"type": "Point", "coordinates": [-85.23, 30.23]},
        "collection": "pgstac-test-collection",
        "properties": {
            "datetime": "2012-05-01T00:00:00Z",
            "deep": {"l1": {"l2": {"l3": {"v": 42}, "q": "kept"}}}
        },
        "stac_version": "1.0.0",
        "stac_extensions": ["proj", "view"]
    }'::jsonb;
    $$,
    'round-trip equality holds for mixed root and deep fragment config'
);

SELECT delete_item('pgstac-test-roundtrip-mixed-0001', 'pgstac-test-collection');

SELECT lives_ok($$
    UPDATE collections
    SET fragment_config = collection_fragment_config_default(content)
    WHERE id='pgstac-test-collection';
$$, 'restore default fragment_config after round-trip guard tests');

SELECT lives_ok($$
    WITH raw AS (
        SELECT '{
            "id": "pgstac-test-item-direct",
            "bbox": [-85.0, 30.0, -84.0, 31.0],
            "type": "Feature",
            "links": [],
            "assets": {"image": {"href": "https://example.com/direct.tif", "type": "image/tiff"}},
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
        datetime_is_range,
        stac_version,
        stac_extensions,
        pgstac_updated_at,
        item_hash,
        bbox,
        links,
        assets,
        properties,
        eo_cloud_cover,
        extra
    )
    SELECT
        (item).id,
        (item).geometry,
        (item).collection,
        (item).datetime,
        (item).end_datetime,
        (item).datetime_is_range,
        (item).stac_version,
        (item).stac_extensions,
        (item).pgstac_updated_at,
        (item).item_hash,
        (item).bbox,
        (item).links,
        (item).assets,
        (item).properties,
        (item).eo_cloud_cover,
        (item).extra
    FROM dehydrated;
$$, 'insert a row directly using split columns via content_dehydrate');

SELECT ok(
    (SELECT fragment_id IS NULL FROM items WHERE id='pgstac-test-item-direct' AND collection='pgstac-test-collection'),
    'directly inserted rows keep a NULL fragment_id'
);
SELECT results_eq($$
    SELECT get_item('pgstac-test-item-direct', 'pgstac-test-collection')->'properties'->>'eo:cloud_cover';
    $$,$$
    SELECT '44';
    $$,
    'directly inserted rows hydrate correctly from split columns'
);

SELECT delete_item('pgstac-test-item-direct', 'pgstac-test-collection');

-- ---------------------------------------------------------------------------
-- datetime: null round-trip — fixes #158 and #425
--
-- The STAC spec requires that items using start_datetime/end_datetime carry
-- an explicit "datetime": null in their properties. Earlier pgstac versions
-- used jsonb_strip_nulls on the full properties object, silently removing it
-- and producing invalid STAC output. The new split-storage hydration applies
-- jsonb_strip_nulls only to the promoted-property block, so datetime: null
-- is preserved end-to-end.
-- ---------------------------------------------------------------------------

SELECT create_item('{
  "id": "pgstac-test-range-datetime",
  "collection": "pgstac-test-collection",
  "type": "Feature",
  "stac_version": "1.0.0",
  "geometry": {"type": "Point", "coordinates": [0, 0]},
  "bbox": [0, 0, 0, 0],
  "links": [], "assets": {},
  "properties": {
    "datetime": null,
    "start_datetime": "2024-01-01T00:00:00Z",
    "end_datetime": "2024-01-02T00:00:00Z"
  }
}'::jsonb);

SELECT ok(
    (get_item('pgstac-test-range-datetime', 'pgstac-test-collection') -> 'properties') ? 'datetime',
    'get_item: properties.datetime key is present for a range item (not stripped)'
);

SELECT results_eq(
    $$ SELECT get_item('pgstac-test-range-datetime', 'pgstac-test-collection') -> 'properties' -> 'datetime' $$,
    $$ SELECT 'null'::jsonb $$,
    'get_item: properties.datetime is JSON null for a range item (fixes #425)'
);

SELECT ok(
    (search('{"ids": ["pgstac-test-range-datetime"]}') -> 'features' -> 0 -> 'properties') ? 'datetime',
    'search: properties.datetime key is present for a range item (not stripped)'
);

SELECT results_eq(
    $$ SELECT search('{"ids": ["pgstac-test-range-datetime"]}') -> 'features' -> 0 -> 'properties' -> 'datetime' $$,
    $$ SELECT 'null'::jsonb $$,
    'search: properties.datetime is JSON null for a range item (fixes #158)'
);

SELECT delete_item('pgstac-test-range-datetime', 'pgstac-test-collection');
