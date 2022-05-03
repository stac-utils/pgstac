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
SELECT has_function('pgstac'::name, 'update_collection_extents', NULL);

DELETE FROM collections WHERE id = 'pgstac-test-collection';
\copy collections (content) FROM 'test/testdata/collections.ndjson';

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
