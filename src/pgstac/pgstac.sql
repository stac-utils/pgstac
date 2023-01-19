BEGIN;
\i sql/000_idempotent_pre.sql
\i sql/001_core.sql
\i sql/001a_jsonutils.sql
\i sql/001s_stacutils.sql
\i sql/002_collections.sql
\i sql/002a_queryables.sql
\i sql/002b_cql.sql
\i sql/003_items.sql
\i sql/003a_partitions.sql
\i sql/004_search.sql
\i sql/005_tileutils.sql
\i sql/006_tilesearch.sql
\i sql/997_maintenance.sql
\i sql/998_idempotent_post.sql
\i sql/999_version.sql
COMMIT;
