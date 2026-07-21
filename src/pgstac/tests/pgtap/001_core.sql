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

SELECT has_function('pgstac'::name, 'pgstac_hash', ARRAY['text']);
SELECT results_eq(
    $$ SELECT pgstac_hash('abc') $$,
    $$ SELECT 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'::text $$,
    'pgstac_hash returns the expected sha256 hex digest'
);
SELECT is(
    pgstac_hash(NULL),
    NULL,
    'pgstac_hash is strict and returns NULL for NULL input'
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
SELECT is_definer('widen_partition_stats');
SELECT is_definer('tighten_partition_stats');
SELECT is_definer('ensure_fragments');
SELECT is_definer('make_binary_staging');
SELECT is_definer('flush_items_staging_binary');
SELECT is_definer('delete_item');
SELECT is_definer('items_staging_triggerfunc');
SELECT is_definer('update_field_registry_from_sample');
SELECT is_definer('refresh_field_registry');
SELECT is_definer('gc_fragments');
SELECT is_definer('check_partition');
SELECT is_definer('repartition');
SELECT is_definer('where_stats');
SELECT is_definer('search_query');
SELECT is_definer('maintain_index');
