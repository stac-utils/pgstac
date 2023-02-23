\unset ECHO
\set QUIET 1
-- Turn off echo and keep things quiet.

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager off
\timing off

-- Revert all changes on failure.
\set ON_ERROR_STOP true

-- Load the TAP functions.
BEGIN;
CREATE EXTENSION IF NOT EXISTS pgtap;
SET SEARCH_PATH TO pgstac, pgtap, public;
SET CLIENT_MIN_MESSAGES TO 'warning';

-- Plan the tests.
SELECT plan(80);
--SELECT * FROM no_plan();

-- Run the tests.

-- Core
\i tests/pgtap/001_core.sql
\i tests/pgtap/001a_jsonutils.sql
\i tests/pgtap/001b_cursorutils.sql
\i tests/pgtap/001s_stacutils.sql
\i tests/pgtap/002_collections.sql
\i tests/pgtap/002a_queryables.sql
\i tests/pgtap/003_items.sql
\i tests/pgtap/004_search.sql
\i tests/pgtap/005_tileutils.sql
\i tests/pgtap/006_tilesearch.sql
\i tests/pgtap/999_version.sql

-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
