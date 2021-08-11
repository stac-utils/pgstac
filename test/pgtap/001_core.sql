-- Check that schema exists
SELECT has_schema('pgstac'::name);

-- Check that PostGIS extension are installed and available on the path
SELECT has_extension('postgis');

SELECT has_table('pgstac'::name, 'migrations'::name);


SELECT has_function('pgstac'::name, 'textarr', ARRAY['jsonb']);
SELECT results_eq(
    $$ SELECT textarr('["a","b","c"]'::jsonb) $$,
    $$ SELECT '{a,b,c}'::text[] $$,
    'textarr returns text[] from jsonb array'
);


SELECT has_function('pgstac'::name, 'estimated_count', ARRAY['text']);
