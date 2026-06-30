DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

-- Register promoted native-column queryables.
-- Each entry maps a STAC property name to the promoted items column via property_path.
-- CQL2 queries and auto-created indexes will use the native column, not JSONB extraction.
-- The seed data lives in promoted_queryables_defaults() (002a_queryables.sql) so it
-- only needs to be maintained in one place.
--
-- Pass 1: insert rows that do not yet exist.
INSERT INTO queryables (name, definition, property_path, property_wrapper)
SELECT p.name, p.definition, p.property_path, p.property_wrapper
FROM promoted_queryables_defaults() p
WHERE NOT EXISTS (
    SELECT 1 FROM queryables q WHERE q.name = p.name
);

-- Pass 2: backfill property_path on older rows and normalize promoted wrappers
-- to the defaults (NULL for native promoted columns).
UPDATE queryables q
SET property_path = CASE
      WHEN q.property_index_type IS NULL THEN COALESCE(q.property_path, p.property_path)
      ELSE q.property_path
    END,
    property_wrapper = CASE
      WHEN q.property_index_type IS NULL THEN p.property_wrapper
      ELSE q.property_wrapper
    END,
    definition = COALESCE(q.definition, p.definition)
FROM promoted_queryables_defaults() p
WHERE q.name = p.name;

DELETE FROM queryables a USING queryables b
  WHERE a.name = b.name AND a.collection_ids IS NOT DISTINCT FROM b.collection_ids AND a.id > b.id;


INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default_filter_lang', 'cql2-json'),
  ('additional_properties', 'true'),
  ('use_queue', 'false'),
  ('queue_timeout', '10 minutes'),
  ('update_collection_extent', 'false'),
  ('format_cache', 'false'),
  ('readonly', 'false')
ON CONFLICT DO NOTHING
;


INSERT INTO cql2_ops (op, template, types) VALUES
    ('eq', '%s = %s', NULL),
    ('neq', '%s != %s', NULL),
    ('ne', '%s != %s', NULL),
    ('!=', '%s != %s', NULL),
    ('<>', '%s != %s', NULL),
    ('lt', '%s < %s', NULL),
    ('lte', '%s <= %s', NULL),
    ('gt', '%s > %s', NULL),
    ('gte', '%s >= %s', NULL),
    ('le', '%s <= %s', NULL),
    ('ge', '%s >= %s', NULL),
    ('=', '%s = %s', NULL),
    ('<', '%s < %s', NULL),
    ('<=', '%s <= %s', NULL),
    ('>', '%s > %s', NULL),
    ('>=', '%s >= %s', NULL),
    ('like', '%s LIKE %s', NULL),
    ('ilike', '%s ILIKE %s', NULL),
    ('+', '%s + %s', NULL),
    ('-', '%s - %s', NULL),
    ('*', '%s * %s', NULL),
    ('/', '%s / %s', NULL),
    ('not', 'NOT (%s)', NULL),
    ('between', '%s BETWEEN %s AND %s', NULL),
    ('isnull', '%s IS NULL', NULL),
    ('upper', 'upper(%s)', NULL),
    ('lower', 'lower(%s)', NULL),
    ('casei', 'upper(%s)', NULL),
    ('accenti', 'unaccent(%s)', NULL)
ON CONFLICT (op) DO UPDATE
    SET
        template = EXCLUDED.template
;


ALTER FUNCTION to_text COST 5000;
ALTER FUNCTION to_float COST 5000;
ALTER FUNCTION to_int COST 5000;
ALTER FUNCTION to_tstz COST 5000;
ALTER FUNCTION to_text_array COST 5000;

-- SECURITY DEFINER is declared INLINE in each function's CREATE (the single source of truth),
-- not re-applied here. Functions that create partitions/indexes/constraints declare it inline so
-- the created objects are owned by pgstac_admin; functions that write the search cache from the
-- read path declare it inline too. Pure helpers stay SECURITY INVOKER. Keeping a separate ALTER
-- list here only let it drift from the definitions (stale/duplicate/wrong-signature entries).

-- Schema USAGE for pgstac_read / pgstac_ingest is granted in 000_idempotent_pre.sql; pgstac_admin
-- owns the schema. Not re-granted here.

-- pgstac_read API surface. Functions are EXECUTE-able by PUBLIC by default, so these grants are not
-- required for access today; they document the intended top-level read API (and would be the point to
-- enforce from if EXECUTE were ever revoked from PUBLIC). Internal helpers (keyset_*, partition_bounds,
-- cql2_*, next_band, ...) are deliberately NOT listed — read reaches them only inside these entry points.
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
GRANT EXECUTE ON FUNCTION content_hydrate TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_page TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_plan TO pgstac_read;
GRANT EXECUTE ON FUNCTION collection_search_plan TO pgstac_read;
GRANT EXECUTE ON FUNCTION collection_search TO pgstac_read;
GRANT EXECUTE ON FUNCTION geometrysearch TO pgstac_read;
GRANT EXECUTE ON FUNCTION geojsonsearch TO pgstac_read;
GRANT EXECUTE ON FUNCTION xyzsearch TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_from_json(jsonb, jsonb) TO pgstac_read;
-- Tables are NOT readable by PUBLIC; read needs an explicit SELECT grant.
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

-- Privilege wall (INV-1): the connecting role (inheriting pgstac_ingest) may READ these tables but may
-- only MUTATE them through the SECURITY DEFINER write functions (check_partition, widen/tighten_partition_stats,
-- ensure_fragments, make_binary_staging, flush_items_staging_binary, the items_staging trigger, create/
-- update/upsert/delete_item, the field-registry/fragment helpers). This makes an un-widened or
-- envelope-narrowing direct write structurally impossible. New partitions are created SELECT-only for
-- pgstac_ingest by check_partition; the items parent is revoked here. Staging tables (items_staging*) stay
-- writable so the SQL-only ingest path can COPY into them.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON items, partition_stats, item_fragments FROM pgstac_ingest;
-- item_field_registry is the one exception to the wall: the loader WIDENS it directly with an add-only
-- INSERT ... ON CONFLICT DO UPDATE (no SD function), so INSERT + UPDATE stay granted. DELETE/TRUNCATE remain
-- revoked, so even a direct write cannot NARROW the registry — INV-1 (registry is a superset of the data)
-- holds structurally against narrowing, while the cheap widen stays on the hot load path.
REVOKE DELETE, TRUNCATE ON item_field_registry FROM pgstac_ingest;

-- pgstac_load is the explicit up-privilege hole in the wall above: the Rust loader does `SET ROLE pgstac_load`
-- to binary-COPY into items and write partition_stats + item_fragments. These are the table/schema PRIVILEGE
-- grants on the role (the role MEMBERSHIP grants — who may SET ROLE pgstac_load — live in 000_idempotent_pre,
-- where the install's superuser context can make them; pgstac_admin here cannot grant a role it lacks ADMIN
-- on). SELECT is included so the binary-COPY column describe (SELECT * FROM items WHERE false) works.
GRANT USAGE ON SCHEMA pgstac TO pgstac_load;
GRANT SELECT, INSERT, UPDATE, DELETE ON items, partition_stats, item_fragments TO pgstac_load;

REVOKE ALL PRIVILEGES ON PROCEDURE run_queued_queries FROM public;
GRANT ALL ON PROCEDURE run_queued_queries TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_queries_intransaction FROM public;
GRANT ALL ON FUNCTION run_queued_queries_intransaction TO pgstac_admin;

REVOKE ALL PRIVILEGES ON PROCEDURE gc_deleted_items_log_committed(interval, integer) FROM public;
GRANT ALL ON PROCEDURE gc_deleted_items_log_committed(interval, integer) TO pgstac_admin;

RESET ROLE;

-- (No install-time stats seeding: a fresh install has no partitions, and partition_stats rows are now
-- seeded by check_partition as partitions are created. Exact stats come from tighten_partition_stats via
-- the maintenance sweeps.)
