SELECT has_table('pgstac'::name, 'items'::name);


SELECT is_indexed('pgstac'::name, 'items'::name, 'geometry');

SELECT is_partitioned('pgstac'::name,'items'::name);


SELECT has_function('pgstac'::name, 'get_item', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'delete_item', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'create_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'create_items', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_items', ARRAY['jsonb']);


-- tools to update collection extents based on extents in items
SELECT has_function('pgstac'::name, 'collection_bbox', ARRAY['text']);
SELECT has_function('pgstac'::name, 'collection_temporal_extent', ARRAY['text']);
SELECT has_function('pgstac'::name, 'update_collection_extents', NULL);
