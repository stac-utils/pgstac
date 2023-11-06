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

SET transaction_read_only TO 'on';

SELECT results_eq(
    $$ SHOW transaction_read_only; $$,
    $$ SELECT 'on'; $$,
    'Transaction set to read only'
);

SELECT throws_ok(
    $$ SELECT search('{}'); $$,
    '25006'
);

SET pgstac.readonly to 'true';
SELECT results_eq(
    $$ SELECT pgstac.readonly(); $$,
    $$ SELECT TRUE; $$,
    'Readonly is set to true'
);

SELECT lives_ok(
    $$ SELECT search('{}'); $$,
    'Search works with readonly mode set to on in readonly mode.'
);
RESET transaction_read_only;
RESET pgstac.readonly;
