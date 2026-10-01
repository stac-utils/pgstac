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

-- PGTap runs inside a transaction, so exercise the transactional queue runner.
-- Both queue runners keep the same error variable across loop iterations.

-- One attempt each, so the counts below stay about the error variable rather than
-- about how many times a failure is retried, which is exercised further down.
SET pgstac.queue_retries TO '1';
DELETE FROM query_queue;
DELETE FROM query_queue_history;
INSERT INTO query_queue (query, added) VALUES
    ('SELECT 1 /* queue error reset success */', '2000-01-01 00:00:00+00'),
    ('SELECT 1 / 0 /* queue error reset failure */', '2000-01-02 00:00:00+00');

SELECT is(
    run_queued_queries_intransaction(),
    2,
    'run_queued_queries_intransaction processes both queued statements'
);
SELECT is(
    (
        SELECT count(*)::integer
        FROM query_queue_history
        WHERE query LIKE '%queue error reset%'
    ),
    2,
    'queue history records both statements'
);
SELECT is(
    (
        SELECT count(*)::integer
        FROM query_queue_history
        WHERE query LIKE '%queue error reset%'
          AND error IS NOT NULL
    ),
    1,
    'a successful statement after a failure has no inherited error'
);
DELETE FROM query_queue_history WHERE query LIKE '%queue error reset%';
RESET pgstac.queue_retries;

CREATE TABLE zz_conc (a int);
INSERT INTO query_queue (query) VALUES ('CREATE INDEX zz_conc_idx ON pgstac.zz_conc (a)');
SELECT is(run_queued_queries_intransaction(), 1, 'a queued index build is run');
SELECT matches(
    (SELECT prosrc FROM pg_proc WHERE pronamespace = 'pgstac'::regnamespace AND proname = 'run_queued_queries' AND prokind = 'p'),
    'run_queued_query\(\)',
    'the run_queued_queries procedure shares run_queued_query()'
);
-- results_eq, not is(): a scalar subquery matching no row is also NULL, so the old form passed
-- when the statement was recorded under different text -- the very thing it is checking.
SELECT results_eq(
    $$ SELECT error FROM query_queue_history WHERE query LIKE 'CREATE INDEX zz_conc_idx%' $$,
    $$ VALUES (NULL::text) $$,
    'the queued statement is recorded exactly once, as it was queued, without an error'
);
SELECT has_index('pgstac', 'zz_conc', 'zz_conc_idx', ARRAY['a']);
DROP TABLE zz_conc;
DELETE FROM query_queue_history WHERE query LIKE '%zz_conc_idx%';

-- Signatures pgstac no longer defines, which must be absent from a fresh install and
-- from a migrated database alike.
SELECT is(
    (
        SELECT count(*)::int
        FROM unnest(ARRAY[
            'update_partition_stats(text, boolean)',
            'maintain_index(text, text, boolean, boolean, boolean)',
            'sort_dir_to_op(text, boolean)',
            'sort_sqlorderby(jsonb, boolean)',
            'get_sort_dir(jsonb)',
            'paging_collections(jsonb)',
            'queryable_signature(text, text[])',
            'queryable_name_spellings(text)',
            'normalize_indexdef(text)',
            'unnest_collection(text[])',
            'indexdef_field(text, text)',
            'indexdef_queryable_name(text)',
            'queryable_field(text[])',
            'indexdef_unnamed(text)',
            'indexdef(queryables)',
            'array_to_path(text[])',
            'key_literal(text)',
            'canonical_property_path(text[])',
            'property_path_keys(text)',
            'queryable_index_expression(text[])',
            'queryable_keys(text, text)',
            'upsert_queryable(text, jsonb, text, text, text[], text)'
        ]) s
        WHERE to_regprocedure('pgstac.' || s) IS NOT NULL
    ),
    0,
    'superseded function signatures are absent'
);
SELECT has_function('pgstac', 'get_token_filter', ARRAY['jsonb', 'items', 'boolean', 'boolean', 'text[]']);

RESET pgstac.context;
SELECT is_definer('drop_table_constraints');
SELECT is_definer('create_table_constraints');
SELECT is_definer('check_partition');
SELECT is_definer('repartition');
SELECT is_definer('maintain_index');
SELECT is_definer('maintain_reference_index');
SELECT is_definer('delete_collection');
SELECT is_definer('collection_delete_trigger_func');

-- Everything that does not need ownership of pgstac_admin's objects runs as
-- the invoker and relies on table privileges instead.
SELECT isnt_definer('update_partition_stats');
SELECT isnt_definer('partition_after_triggerfunc');
SELECT isnt_definer('sync_partition_stats');
SELECT isnt_definer('where_stats');
SELECT isnt_definer('search_query');
SELECT isnt_definer('format_item');

-- A statement that cannot succeed is retried before it is given up on, so a failure that
-- would have succeeded on a second try -- a deadlock victim -- is not lost on the first.
SET pgstac.queue_retries TO '2';
DELETE FROM query_queue;
DELETE FROM query_queue_history;
INSERT INTO query_queue (query) VALUES ('SELECT 1/0;');

SELECT is(run_queued_query(), true, 'a failing queued statement counts as run');
SELECT is(
    (SELECT attempts FROM query_queue WHERE query = 'SELECT 1/0;'),
    1,
    'it stays in the queue with the attempt recorded'
);

SELECT is(run_queued_query(), true, 'it is claimed again while attempts remain');
SELECT is(
    (SELECT count(*)::int FROM query_queue WHERE query = 'SELECT 1/0;'),
    0,
    'the attempt that uses up its budget retires it, so the queue can reach empty'
);
SELECT is(run_queued_query(), false, 'nothing is left to claim');
SELECT is(
    (SELECT count(*)::int FROM query_queue_history
      WHERE query = 'SELECT 1/0;' AND error IS NOT NULL),
    2,
    'every attempt is in the history with the error that stopped it'
);

-- A fresh request supersedes the state the earlier attempts were spent on.
INSERT INTO query_queue (query) VALUES ('SELECT 1/0;');
UPDATE query_queue SET attempts = 2 WHERE query = 'SELECT 1/0;';
SET pgstac.use_queue TO TRUE;
SELECT run_or_queue('SELECT 1/0;');
RESET pgstac.use_queue;
SELECT is(
    (SELECT attempts FROM query_queue WHERE query = 'SELECT 1/0;'),
    0,
    're-queueing a statement gives it a fresh budget'
);

-- An orphan: attempts spent without the row being retired, which is what a backend killed
-- mid-statement or a lowered queue_retries leaves behind. Nothing would ever claim it again.
UPDATE query_queue SET attempts = 99 WHERE query = 'SELECT 1/0;';
SELECT is(run_queued_query(), false, 'a row with no attempts left is never claimed');
SELECT results_eq(
    $$ SELECT query FROM retire_queued_queries() $$,
    $$ VALUES ('SELECT 1/0;'::text) $$,
    'retire_queued_queries reports the orphan it removed'
);
SELECT is(
    (SELECT count(*)::int FROM query_queue),
    0,
    'and the queue is empty afterwards'
);
SELECT is(
    (SELECT count(*)::int FROM query_queue_history
      WHERE query = 'SELECT 1/0;' AND attempts = 99 AND error IS NOT NULL),
    1,
    'the retired orphan is recorded in the history'
);

DELETE FROM query_queue;
INSERT INTO query_queue (query) VALUES ('SELECT 1;');
SELECT is(run_queued_query(), true, 'a statement that succeeds runs');
SELECT is_empty(
    $$ SELECT query FROM query_queue WHERE query = 'SELECT 1;' $$,
    'and is removed from the queue'
);
DELETE FROM query_queue;
RESET pgstac.queue_retries;
