BEGIN;
\i sql/001_core.sql
\i sql/001a_jsonutils.sql
\i sql/001b_cursorutils.sql
\i sql/001s_stacutils.sql
\i sql/002_collections.sql
\i sql/003_items.sql
\i sql/004_search.sql
\i sql/999_version.sql
--\i test/pgtap.sql
COMMIT;
