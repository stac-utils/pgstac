SELECT has_function('pgstac'::name, 'textarr', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_obj_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_val_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'path_includes', ARRAY['text[]', 'text[]']);
SELECT has_function('pgstac'::name, 'path_excludes', ARRAY['text[]', 'text[]']);
SELECT has_function('pgstac'::name, 'jsonb_obj_paths_filtered', ARRAY['jsonb','text[]','text[]']);
SELECT has_function('pgstac'::name, 'filter_jsonb', ARRAY['jsonb','text[]','text[]']);
