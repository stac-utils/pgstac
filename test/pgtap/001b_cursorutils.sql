--create_cursor
SELECT has_function('pgstac'::name, 'create_cursor', ARRAY['text']);

--partition_cursor
SELECT has_function('pgstac'::name, 'partition_cursor', ARRAY['text', 'text','tstzrange']);
