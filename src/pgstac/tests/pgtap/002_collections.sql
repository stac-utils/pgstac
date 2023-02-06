SELECT has_table('pgstac'::name, 'collections'::name);
SELECT col_is_pk('pgstac'::name, 'collections'::name, 'key', 'collections has primary key');

SELECT has_function('pgstac'::name, 'create_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'get_collection', ARRAY['text']);
SELECT has_function('pgstac'::name, 'delete_collection', ARRAY['text']);
SELECT has_function('pgstac'::name, 'all_collections', '{}'::text[]);
