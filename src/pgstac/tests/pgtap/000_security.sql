-- A SECURITY DEFINER function runs with the privileges of its owner, and
-- PostgreSQL grants EXECUTE to PUBLIC on every new function. Without an
-- explicit REVOKE, any role in the database can use one to act as the schema
-- owner.

SELECT is_empty(
    $$
    SELECT proname FROM pg_proc
    WHERE pronamespace='pgstac'::regnamespace
      AND prosecdef
      AND has_function_privilege('public', oid, 'EXECUTE')
    $$,
    'no SECURITY DEFINER function in pgstac is executable by PUBLIC'
);

-- CREATE TRIGGER checks EXECUTE on the function by the creating role, so a
-- definer trigger function these roles can execute is one they can attach to a
-- table of their own and run as the schema owner.
SELECT is_empty(
    $$
    SELECT proname FROM pg_proc
    WHERE pronamespace='pgstac'::regnamespace
      AND prosecdef
      AND prorettype='trigger'::regtype
      AND (
        has_function_privilege('pgstac_ingest', oid, 'EXECUTE')
        OR has_function_privilege('pgstac_read', oid, 'EXECUTE')
      )
    $$,
    'no SECURITY DEFINER trigger function in pgstac is executable by pgstac_ingest or pgstac_read'
);

-- Without a pinned search_path a definer function resolves unqualified names
-- against whatever the caller set.
SELECT is_empty(
    $$
    SELECT proname FROM pg_proc
    WHERE pronamespace='pgstac'::regnamespace
      AND prosecdef
      AND NOT COALESCE(proconfig::text ~ 'search_path', FALSE)
    $$,
    'every SECURITY DEFINER function in pgstac pins its search_path'
);

-- maintain_index runs elevated, so it takes a queryables row and builds the
-- index statement itself rather than accepting one.
SELECT has_function(
    'pgstac'::name,
    'maintain_index'::name,
    ARRAY['text','text','bigint','boolean','boolean']
);
SELECT hasnt_function(
    'pgstac'::name,
    'maintain_index'::name,
    ARRAY['text','text','boolean','boolean','boolean']
);

-- queryable_indexes and the views over it are read paths: they perform no DDL,
-- so a reference index dropped by hand stays missing until maintain_partitions.
CREATE FUNCTION pg_temp.template_indexes() RETURNS oid[] AS $$
    SELECT array_agg(indexrelid ORDER BY indexrelid) FROM pg_index
    WHERE indrelid = 'pgstac.queryable_index_template'::regclass;
$$ LANGUAGE SQL;
DO $$ BEGIN
    EXECUTE format('DROP INDEX pgstac.%I', (SELECT reference_index_name(q) FROM queryables q WHERE name = 'eo:cloud_cover'));
END $$;
SELECT pg_temp.template_indexes() AS template_indexes_before \gset

-- What a read only role may do. The first is the premise of the two that
-- follow: pgstac_read has no write privileges of its own, so anything it
-- manages to change it changed through a definer function.
SET ROLE pgstac_read;

SELECT lives_ok(
    $$ SELECT * FROM pgstac.pgstac_indexes $$,
    'pgstac_read can select from pgstac_indexes'
);

SELECT lives_ok(
    $$ SELECT * FROM pgstac.pgstac_indexes_stats $$,
    'pgstac_read can select from pgstac_indexes_stats'
);

SELECT lives_ok(
    $$ SELECT * FROM pgstac.queryable_indexes('items') $$,
    'pgstac_read can call queryable_indexes'
);

SELECT is(
    pg_temp.template_indexes(),
    :'template_indexes_before'::oid[],
    'a select on queryable_indexes performs no DDL: the hand-dropped reference index is still missing'
);

SELECT throws_ok(
    'DELETE FROM pgstac.collections',
    '42501',
    NULL,
    'pgstac_read cannot write collections directly'
);

SELECT throws_ok(
    $$ SELECT pgstac.delete_collection('anything') $$,
    '42501',
    NULL,
    'pgstac_read cannot drop a collection through delete_collection'
);

SELECT throws_ok(
    $$ SELECT pgstac.maintain_index('_items_1', NULL, NULL) $$,
    '42501',
    NULL,
    'pgstac_read cannot run DDL through maintain_index'
);

-- where_stats, search_query and format_item run as invokers, so pgstac_read
-- needs direct grants on the caches they maintain.
SELECT lives_ok(
    $$ SELECT pgstac.search('{}') $$,
    'pgstac_read can still search'
);

RESET ROLE;

SELECT maintain_partitions();
SELECT is(
    (SELECT count(*)::int FROM pg_index WHERE indrelid = 'pgstac.queryable_index_template'::regclass),
    cardinality(:'template_indexes_before'::oid[]) + 1,
    'maintain_partitions rebuilds the hand-dropped reference index'
);

-- Dropping a collection's partitions needs an ownership pgstac_ingest does not
-- have, so the delete trigger runs elevated.
SELECT create_collection('{"id": "pgstac-test-ingestdelete", "type": "Collection", "stac_version": "1.0.0", "description": "ingest role delete", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2011-01-01T00:00:00Z", "2011-12-31T00:00:00Z"]]}}, "item_assets": {"image": {"type": "image/tiff", "title": "Image"}}}');
SELECT create_item('{"id": "pgstac-test-ingestdelete-a", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-ingestdelete", "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {"image": {"href": "https://example.com/a.tif", "type": "image/tiff", "title": "Image"}}, "properties": {"datetime": "2011-06-01T00:00:00Z"}, "stac_extensions": []}');
-- An edited collection also has base_items rows for the trigger to clear.
SELECT update_collection('{"id": "pgstac-test-ingestdelete", "type": "Collection", "stac_version": "1.0.0", "description": "ingest role delete", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [["2011-01-01T00:00:00Z", "2011-12-31T00:00:00Z"]]}}, "item_assets": {"image": {"type": "image/tiff", "title": "Image"}, "thumbnail": {"type": "image/jpeg", "title": "Thumbnail"}}}');

SET ROLE pgstac_ingest;

SELECT lives_ok(
    $$ DELETE FROM pgstac.collections WHERE id='pgstac-test-ingestdelete' $$,
    'pgstac_ingest can delete a collection that has items'
);

-- Registering a wrapper names a function the queryables trigger will resolve, so it is left to the admin.
SELECT throws_ok(
    $$ INSERT INTO pgstac.queryable_wrappers (name) VALUES ('to_anything') $$,
    '42501',
    NULL,
    'pgstac_ingest cannot register a queryable wrapper'
);

RESET ROLE;

SELECT is_empty(
    $$ SELECT * FROM pgstac.partition_sys_meta WHERE collection='pgstac-test-ingestdelete' $$,
    'a collection deleted by pgstac_ingest leaves no partition behind'
);

SELECT is_empty(
    $$ SELECT * FROM pgstac.partition_stats WHERE collection='pgstac-test-ingestdelete' $$,
    'a collection deleted by pgstac_ingest leaves no partition_stats rows behind'
);

SELECT is_empty(
    $$ SELECT * FROM pgstac.base_items WHERE collection='pgstac-test-ingestdelete' $$,
    'a collection deleted by pgstac_ingest leaves no base_items rows behind'
);

-- The elevated DDL functions decide what they may alter by asking
-- partition_catalog_meta, so it must describe partitions of items and nothing
-- else.
SELECT is_empty(
    $$ SELECT * FROM pgstac.partition_catalog_meta('collections') $$,
    'partition_catalog_meta rejects a table that is not a partition of items'
);
SELECT is_empty(
    $$ SELECT * FROM pgstac.partition_catalog_meta('queryables') $$,
    'partition_catalog_meta rejects queryables'
);

-- The queue runners are admin-only; retire_queued_queries deletes from the queue and has to
-- be held to the same line, or any role could drop pending maintenance.
SELECT is(
    has_function_privilege('pgstac_read', 'pgstac.retire_queued_queries()', 'EXECUTE'),
    false,
    'pgstac_read cannot execute retire_queued_queries'
);
SELECT results_eq(
    $$ SELECT has_function_privilege(r, 'pgstac.retire_queued_queries()', 'EXECUTE')
       FROM unnest(ARRAY['pgstac_read', 'pgstac_ingest', 'pgstac_admin']) r ORDER BY r $$,
    $$ SELECT has_function_privilege(r, 'pgstac.run_queued_query()', 'EXECUTE')
       FROM unnest(ARRAY['pgstac_read', 'pgstac_ingest', 'pgstac_admin']) r ORDER BY r $$,
    'retire_queued_queries is executable by exactly the roles that can run the queue'
);
