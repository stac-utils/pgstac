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
SET CLIENT_MIN_MESSAGES TO 'warning';

-- Plan the tests.
SELECT plan(127);
--SELECT * FROM no_plan();

-- Run the tests.

-- Core
\i test//pgtap/001_core.sql
\i test/pgtap/001a_jsonutils.sql
\i test/pgtap/001b_cursorutils.sql
\i test/pgtap/001s_stacutils.sql
\i test/pgtap/002_collections.sql
\i test/pgtap/003_items.sql
\i test/pgtap/004_search.sql
\i test/pgtap/005_tileutils.sql
\i test/pgtap/006_tilesearch.sql
\i test/pgtap/999_version.sql

-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
