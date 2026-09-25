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
    ARRAY['text','text','bigint','boolean','boolean','boolean']
);
SELECT hasnt_function(
    'pgstac'::name,
    'maintain_index'::name,
    ARRAY['text','text','boolean','boolean','boolean']
);

-- What a read only role may do. The first is the premise of the two that
-- follow: pgstac_read has no write privileges of its own, so anything it
-- manages to change it changed through a definer function.
SET ROLE pgstac_read;

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
