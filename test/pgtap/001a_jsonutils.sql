SELECT has_function('pgstac'::name, 'textarr', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_obj_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_val_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'path_includes', ARRAY['text[]', 'text[]']);
SELECT has_function('pgstac'::name, 'path_excludes', ARRAY['text[]', 'text[]']);
SELECT has_function('pgstac'::name, 'jsonb_obj_paths_filtered', ARRAY['jsonb','text[]','text[]']);
SELECT has_function('pgstac'::name, 'filter_jsonb', ARRAY['jsonb','text[]','text[]']);


SELECT results_eq(
    $$ SELECT textarr('["a","b","c"]'::jsonb) $$,
    $$ SELECT '{a,b,c}'::text[] $$,
    'textarr returns text[] from jsonb array'
);

SELECT results_eq(
    $$ SELECT filter_jsonb('{"a":1,"b":2,"c":3}'::jsonb, includes=>'{a,c}') $$,
    $$ SELECT '{"a":1,"c":3}'::jsonb $$,
    'filter_jsonb includes work'
);
SELECT results_eq(
    $$ SELECT filter_jsonb('{"a":1,"b":2,"c":3}'::jsonb, includes=>NULL, excludes=>'{a,c}') $$,
    $$ SELECT '{"b":2}'::jsonb $$,
    'filter_jsonb excludes work'
);
