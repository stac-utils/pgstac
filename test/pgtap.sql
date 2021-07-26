\unset ECHO
\set QUIET 1
-- Turn off echo and keep things quiet.

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager off
\timing off

-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- Load the TAP functions.
BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SET SEARCH_PATH TO pgstac, pgtap, public;

-- Plan the tests.
SELECT plan(62);
--SELECT * FROM no_plan();

-- Run the tests.

-- Core

-- Check that schema exists
SELECT has_schema('pgstac'::name);

-- Check that PostGIS and PG_Partman extensions are installed and available on the path
SELECT has_extension('postgis');
SELECT has_extension('pg_partman');

SELECT has_table('pgstac'::name, 'migrations'::name);


SELECT has_function('pgstac'::name, 'textarr', ARRAY['jsonb']);
SELECT results_eq(
    $$ SELECT textarr('["a","b","c"]'::jsonb) $$,
    $$ SELECT '{a,b,c}'::text[] $$,
    'textarr returns text[] from jsonb array'
);

SELECT has_function('pgstac'::name, 'array_idents', ARRAY['jsonb']);
SELECT results_eq(
    $$ SELECT array_idents('["a","b","c"]'::jsonb) $$,
    $$ SELECT 'a,b,c' $$,
    'array_idents returns csv double quoted from jsonb array'
);

SELECT has_function('pgstac'::name, 'properties_idx', ARRAY['jsonb']);
SELECT results_eq(
    $$ SELECT properties_idx('{"a":1,"b":"B","c":[1,2]}'::jsonb) $$,
    $$ SELECT '{"a":1,"b":"b"}'::jsonb $$,
    'properties_idx returns slimmed lower case jsonb'
);

SELECT has_function('pgstac'::name, 'stac_geom', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'stac_datetime', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_obj_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_val_paths', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'path_includes', ARRAY['text[]','text[]']);
SELECT has_function('pgstac'::name, 'path_excludes', ARRAY['text[]','text[]']);
SELECT has_function('pgstac'::name, 'jsonb_obj_paths_filtered', ARRAY['jsonb','text[]','text[]']);
SELECT has_function('pgstac'::name, 'empty_arr', ARRAY['anyarray']);
SELECT has_function('pgstac'::name, 'filter_jsonb', ARRAY['jsonb','text[]','text[]']);


-- Collections
SELECT has_table('pgstac'::name, 'collections'::name);
SELECT col_is_pk('pgstac'::name, 'collections'::name, 'id', 'collections has primary key');

SELECT has_function('pgstac'::name, 'create_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_collection', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'get_collection', ARRAY['text']);
SELECT has_function('pgstac'::name, 'delete_collection', ARRAY['text']);
SELECT has_function('pgstac'::name, 'all_collections', NULL);





-- Items
SELECT has_table('pgstac'::name, 'items'::name);

SELECT is_indexed('pgstac'::name, 'items'::name, ARRAY['datetime','id']);
SELECT is_indexed('pgstac'::name, 'items'::name, 'properties');
SELECT is_indexed('pgstac'::name, 'items'::name, 'geometry');
SELECT is_indexed('pgstac'::name, 'items'::name, 'collection_id');

SELECT has_type('pgstac'::name, 'item'::name);
SELECT is_partitioned('pgstac'::name,'items'::name);


SELECT has_function('pgstac'::name, 'get_item', ARRAY['text']);
SELECT has_function('pgstac'::name, 'delete_item', ARRAY['text']);
SELECT has_function('pgstac'::name, 'create_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'create_items', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'update_item', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'upsert_item', ARRAY['jsonb']);



SELECT has_function('pgstac'::name, 'analyze_empty_partitions', NULL);
SELECT has_function('pgstac'::name, 'backfill_partitions', NULL);
SELECT has_function('pgstac'::name, 'items_trigger_stmt_func', NULL);


SELECT has_view('pgstac'::name, 'all_items_partitions'::name, 'all_items_partitions view exists');
SELECT has_view('pgstac'::name, 'items_partitions'::name, 'items_partitions view exists');

-- tools to update collection extents based on extents in items
SELECT has_function('pgstac'::name, 'collection_bbox', ARRAY['text']);
SELECT has_function('pgstac'::name, 'collection_temporal_extent', ARRAY['text']);
SELECT has_function('pgstac'::name, 'update_collection_extents', NULL);



-- Search

SELECT has_function('pgstac'::name, 'items_by_partition', ARRAY['text','tstzrange','text','int']);
SELECT has_function('pgstac'::name, 'split_stac_path', ARRAY['text']);
SELECT has_function('pgstac'::name, 'sort_base', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'sort', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'rsort', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'bbox_geom', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'in_array_q', ARRAY['text','jsonb']);
SELECT has_function('pgstac'::name, 'count_by_delim', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'stac_query_op', ARRAY['text','text','jsonb']);
SELECT has_function('pgstac'::name, 'stac_query', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'filter_by_order', ARRAY['text','jsonb','text']);
SELECT has_function('pgstac'::name, 'filter_by_order', ARRAY['item','jsonb','text']);
SELECT has_function('pgstac'::name, 'search_dtrange', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'search', ARRAY['jsonb']);

-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
