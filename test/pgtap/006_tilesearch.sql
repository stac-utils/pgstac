SELECT has_function('pgstac'::name, 'geometry_search', ARRAY['geometry','text','jsonb','int','int','interval','boolean']);
SELECT has_function('pgstac'::name, 'geojson_search', ARRAY['jsonb','text','jsonb','int','int','interval','boolean']);
SELECT has_function('pgstac'::name, 'xyz_search', ARRAY['int','int','int','text','jsonb','int','int','interval','boolean']);
