SELECT has_function('pgstac'::name, 'geometrysearch', ARRAY['geometry','text','jsonb','int','int','interval','boolean']);
SELECT has_function('pgstac'::name, 'geojsonsearch', ARRAY['jsonb','text','jsonb','int','int','interval','boolean']);
SELECT has_function('pgstac'::name, 'xyzsearch', ARRAY['int','int','int','text','jsonb','int','int','interval','boolean']);
