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
            WHEN 'pgstac-test-baseitem-a' THEN NOT f ? 'pgstac:base_item'
            ELSE (f->>'pgstac:base_item')::int = (
                SELECT id FROM base_items
                WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1
            )
        END
    ) FROM jsonb_array_elements(
        search('{"collections": ["pgstac-test-baseitems"], "conf": {"nohydrate": true}}')->'features'
    ) f;
    $$,$$
    SELECT true;
    $$,
    'A nohydrate search carries the tag on the tagged item only'
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

SELECT create_item('{"id": "pgstac-test-baseitem-d", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-baseitems", "pgstac:base_item": 42, "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff", "title": "Image"}}, "properties": {"datetime": "2011-06-01T00:00:00Z"}, "stac_extensions": []}');

SELECT results_eq($$
    SELECT (content->>'pgstac:base_item')::int FROM items WHERE id='pgstac-test-baseitem-d';
    $$,$$
    SELECT id FROM base_items WHERE collection='pgstac-test-baseitems' ORDER BY id DESC LIMIT 1;
    $$,
    'A tag supplied by the caller is discarded and replaced'
);

UPDATE items SET content = content || '{"pgstac:base_item": 999999}'::jsonb
WHERE id='pgstac-test-baseitem-d' AND collection='pgstac-test-baseitems';

SELECT throws_ok(
    $$ SELECT get_item('pgstac-test-baseitem-d', 'pgstac-test-baseitems') $$,
    'P0001',
    NULL,
    'An item tagged with a base item that does not exist fails loudly'
);

UPDATE items SET content = content || '{"pgstac:base_item": "notanint"}'::jsonb
WHERE id='pgstac-test-baseitem-d' AND collection='pgstac-test-baseitems';

SELECT throws_ok(
    $$ SELECT get_item('pgstac-test-baseitem-d', 'pgstac-test-baseitems') $$,
    '22P02',
    NULL,
    'An item whose tag is not an integer fails on the cast'
);

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
