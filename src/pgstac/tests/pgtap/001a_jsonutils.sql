SELECT has_function('pgstac'::name, 'to_text_array', ARRAY['jsonb']);

SELECT results_eq(
    $$ SELECT to_text_array('["a","b","c"]'::jsonb) $$,
    $$ SELECT '{a,b,c}'::text[] $$,
    'textarr returns text[] from jsonb array'
);
