SELECT has_table('pgstac'::name, 'collections'::name);
SELECT col_is_pk('pgstac'::name, 'collections'::name, 'key', 'collections has primary key');

SELECT has_function('pgstac'::name, 'create_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'get_collection', ARRAY['text']);
SELECT has_function('pgstac'::name, 'delete_collection', ARRAY['text']);
SELECT has_function('pgstac'::name, 'all_collections', '{}'::text[]);

DELETE FROM collections WHERE id in ('pgstac-test-collection', 'pgstac-test-collection2');
\copy collections (content) FROM 'tests/testdata/collections.ndjson';
\copy items_staging (content) FROM 'tests/testdata/items.ndjson';

SELECT results_eq($$
    SELECT count(*) FROM partitions_view WHERE collection='pgstac-test-collection';
    $$,
    $$ SELECT 1::bigint $$,
    'Test that partition metadata only has one record after adding items data for one collection'
);

SELECT results_eq($$
    SELECT count(*) FROM partitions_view WHERE collection='pgstac-test-collection2';
    $$,
    $$ SELECT 0::bigint $$,
    'Test that partition metadata does not have collection 2 yet'
);

SELECT create_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection2", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 28, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');


SELECT results_eq($$
    SELECT count(*) FROM partitions_view WHERE collection='pgstac-test-collection2';
    $$,
    $$ SELECT 1::bigint $$,
    'Test that partition metadata only has record for collection 2'
);

DELETE FROM collections WHERE id='pgstac-test-collection2';

SELECT results_eq($$
    SELECT count(*) FROM partition_sys_meta WHERE collection='pgstac-test-collection2';
    $$,
    $$ SELECT 0::bigint $$,
    'Test that sys meta does not have for collection 2 record after removing collection 2'
);


DELETE FROM collections WHERE id='pgstac-test-collection';

SELECT results_eq($$
    SELECT count(*) FROM partition_sys_meta WHERE collection='pgstac-test-collection';
    $$,
    $$ SELECT 0::bigint $$,
    'Test that sys meta does not have for collection 1 record after removing collection 1'
);

\copy collections (content) FROM 'tests/testdata/collections.ndjson';
\copy items_staging (content) FROM 'tests/testdata/items.ndjson';

SELECT results_eq($$
    SELECT count(*) FROM partitions_view WHERE collection='pgstac-test-collection';
    $$,
    $$ SELECT 1::bigint $$,
    'Test that partition metadata only has one record after adding items data for one collection'
);

SELECT results_eq($$
    SELECT count(*) FROM partitions_view WHERE collection='pgstac-test-collection2';
    $$,
    $$ SELECT 0::bigint $$,
    'Test that partition metadata does not have collection 2 yet'
);

SELECT create_item('{"id": "pgstac-test-item-0003", "bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "type": "Feature", "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif", "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"], "title": "RGBIR COG tile", "eo:bands": [{"name": "Red", "common_name": "red"}, {"name": "Green", "common_name": "green"}, {"name": "Blue", "common_name": "blue"}, {"name": "NIR", "common_name": "nir", "description": "near-infrared"}]}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt", "type": "text/plain", "roles": ["metadata"], "title": "FGDC Metdata"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg", "type": "image/jpeg", "roles": ["thumbnail"], "title": "Thumbnail"}}, "geometry": {"type": "Polygon", "coordinates": [[[-85.309412, 30.933949], [-85.308201, 31.002658], [-85.378084, 31.003555], [-85.379245, 30.934843], [-85.309412, 30.933949]]]}, "collection": "pgstac-test-collection2", "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 28, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_version": "1.0.0-beta.2", "stac_extensions": ["eo", "projection"]}');


SELECT results_eq($$
    SELECT count(*) FROM partitions_view WHERE collection='pgstac-test-collection2';
    $$,
    $$ SELECT 1::bigint $$,
    'Test that partition metadata only has record for collection 2'
);
