SELECT has_function('pgstac'::name, 'geometrysearch', ARRAY['geometry','text','jsonb','int','int','interval','boolean','boolean']);
SELECT has_function('pgstac'::name, 'geojsonsearch', ARRAY['jsonb','text','jsonb','int','int','interval','boolean','boolean']);
SELECT has_function('pgstac'::name, 'xyzsearch', ARRAY['int','int','int','text','jsonb','int','int','interval','boolean','boolean']);


SELECT results_eq($$
    select s from xyzsearch(8615, 13418, 15, '4688c09f64bc21b5e4bb7ce19c755623', '{"include":["id"]}'::jsonb) s;
    $$,$$
    select '{"type": "FeatureCollection", "features": [{"id": "pgstac-test-item-0003"}]}'::jsonb
    $$,
    'Test xyzsearch to return feature collection with the only intersecting item'
);

SELECT results_eq($$
    select s from xyzsearch(1048, 1682, 12, '4688c09f64bc21b5e4bb7ce19c755623', '{"include":["id"]}'::jsonb) s;
    $$,$$
    select '{"type": "FeatureCollection", "features": [{"id": "pgstac-test-item-0050"}, {"id": "pgstac-test-item-0049"}, {"id": "pgstac-test-item-0048"}, {"id": "pgstac-test-item-0047"}, {"id": "pgstac-test-item-0100"}, {"id": "pgstac-test-item-0089"}]}'::jsonb
    $$,
    'Test xyzsearch to return feature collection with all intersecting items'
);

SELECT results_eq($$
    select s from xyzsearch(1048, 1682, 12, '4688c09f64bc21b5e4bb7ce19c755623', '{"include":["id"]}'::jsonb, NULL, 1) s;
    $$,$$
    select '{"type": "FeatureCollection", "features": [{"id": "pgstac-test-item-0050"}]}'::jsonb
    $$,
    'Test xyzsearch to return feature collection with all intersecting items but limit to 1'
);

SELECT results_eq($$
    select s from xyzsearch(16792, 26892, 16, '4688c09f64bc21b5e4bb7ce19c755623', '{"include":["id"]}'::jsonb, exitwhenfull => true) s;
    $$,$$
    select '{"type": "FeatureCollection", "features": [{"id": "pgstac-test-item-0098"}, {"id": "pgstac-test-item-0097"}]}'::jsonb
    $$,
    'Test xyzsearch to return feature collection with all intersecting items but exits when tile is filled'
);

SELECT results_eq($$
    select s from xyzsearch(16792, 26892, 16, '4688c09f64bc21b5e4bb7ce19c755623', '{"include":["id"]}'::jsonb, exitwhenfull => false, skipcovered => false) s;
    $$,$$
    select '{"type": "FeatureCollection", "features": [{"id": "pgstac-test-item-0098"}, {"id": "pgstac-test-item-0097"}, {"id": "pgstac-test-item-0091"}]}'::jsonb
    $$,
    'Test xyzsearch to return feature collection with all intersecting items but exits continue even if tile is filled'
);
