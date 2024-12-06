-- Check that schema exists
SELECT has_schema('pgstac'::name);

-- Check that PostGIS extension are installed and available on the path
SELECT has_extension('postgis');

SELECT has_table('pgstac'::name, 'migrations'::name);


SELECT has_function('pgstac'::name, 'to_text_array', ARRAY['jsonb']);
SELECT results_eq(
    $$ SELECT to_text_array('["a","b","c"]'::jsonb) $$,
    $$ SELECT '{a,b,c}'::text[] $$,
    'to_text_array returns text[] from jsonb array'
);

SET pgstac.readonly to 'false';

SELECT results_eq(
    $$ SELECT pgstac.readonly(); $$,
    $$ SELECT FALSE; $$,
    'Readonly is set to false'
);

SELECT lives_ok(
    $$ SELECT search('{}'); $$,
    'Search works with readonly mode set to off in readwrite mode.'
);

RESET pgstac.context;
SELECT is_definer('update_partition_stats');
SELECT is_definer('partition_after_triggerfunc');
SELECT is_definer('drop_table_constraints');
SELECT is_definer('create_table_constraints');
SELECT is_definer('check_partition');
SELECT is_definer('repartition');
SELECT is_definer('where_stats');
SELECT is_definer('search_query');
SELECT is_definer('format_item');
SELECT is_definer('maintain_index');
