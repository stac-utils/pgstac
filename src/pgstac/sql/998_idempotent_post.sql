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

-- Register promoted native-column queryables (v0.10 split schema).
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
  ('search_gc_retention_interval', '7 days'),
  ('default_filter_lang', 'cql2-json'),
  -- target page size for streaming/search_page when the caller gives no limit; the
  -- chunker uses it for fast start + a bounded memory footprint. A caller-provided
  -- limit is still honored as the max rows returned.
  ('default_page_size', '250'),
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

ALTER FUNCTION update_partition_stats SECURITY DEFINER;
ALTER FUNCTION partition_after_triggerfunc SECURITY DEFINER;
ALTER FUNCTION drop_table_constraints SECURITY DEFINER;
ALTER FUNCTION create_table_constraints SECURITY DEFINER;
ALTER FUNCTION check_partition SECURITY DEFINER;
ALTER FUNCTION repartition SECURITY DEFINER;
ALTER FUNCTION where_stats(text, text, boolean, jsonb) SECURITY DEFINER;
ALTER FUNCTION search_query SECURITY DEFINER;
ALTER FUNCTION name_search SECURITY DEFINER;
ALTER FUNCTION rename_search SECURITY DEFINER;
ALTER FUNCTION unname_search SECURITY DEFINER;
ALTER FUNCTION pin_search SECURITY DEFINER;
ALTER FUNCTION unpin_search SECURITY DEFINER;
ALTER FUNCTION gc_anonymous_searches(interval, jsonb) SECURITY DEFINER;
ALTER FUNCTION gc_search_caches(interval, jsonb) SECURITY DEFINER;
ALTER FUNCTION gc_deleted_items_log_batch(interval, integer) SECURITY DEFINER;
ALTER FUNCTION gc_deleted_items_log(interval, integer) SECURITY DEFINER;
ALTER FUNCTION gc_deleted_items_log(interval) SECURITY DEFINER;
ALTER FUNCTION maintain_index SECURITY DEFINER;
ALTER FUNCTION pgstac.jsonb_hash(jsonb) SECURITY DEFINER;
ALTER FUNCTION promoted_items_column_list() SECURITY DEFINER;
ALTER FUNCTION items_content_distinct_sql(text, text) SECURITY DEFINER;
ALTER FUNCTION items_content_changed(items, items) SECURITY DEFINER;
ALTER FUNCTION items_touch_triggerfunc SECURITY DEFINER;
ALTER FUNCTION items_delete_log_trigger SECURITY DEFINER;
ALTER FUNCTION strip_promoted_properties(jsonb) SECURITY DEFINER;
ALTER FUNCTION tstz_to_stac_text(timestamptz) SECURITY DEFINER;
ALTER FUNCTION temporal_properties_from_item(items) SECURITY DEFINER;
ALTER FUNCTION promoted_properties_from_item(items) SECURITY DEFINER;
ALTER FUNCTION extract_fragment(jsonb, text[]) SECURITY DEFINER;
ALTER FUNCTION pgstac_hash_fragment(jsonb) SECURITY DEFINER;
ALTER FUNCTION gc_fragments(text, interval) SECURITY DEFINER;
ALTER FUNCTION strip_fragment_col(jsonb, text, text[]) SECURITY DEFINER;
ALTER FUNCTION update_field_registry_from_sample(text, jsonb[]) SECURITY DEFINER;
ALTER FUNCTION update_field_registry_from_items(text) SECURITY DEFINER;
ALTER FUNCTION refresh_field_registry(text, interval) SECURITY DEFINER;
ALTER FUNCTION register_field_paths(text, jsonb) SECURITY DEFINER;
GRANT EXECUTE ON FUNCTION register_field_paths(text, jsonb) TO pgstac_ingest;
ALTER FUNCTION collection_fragment_config_default(jsonb) SECURITY DEFINER;
ALTER FUNCTION jsonb_leaf_rows(jsonb, text) SECURITY DEFINER;
ALTER FUNCTION jsonb_common_values(jsonb, jsonb) SECURITY DEFINER;
ALTER FUNCTION fragment_path_text(text[]) SECURITY DEFINER;
ALTER FUNCTION fragment_path_array(text) SECURITY DEFINER;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
GRANT EXECUTE ON FUNCTION content_hydrate(items, jsonb, item_fragments) TO pgstac_read;
GRANT EXECUTE ON FUNCTION pgstac.jsonb_hash(jsonb) TO pgstac_read;
-- Streaming search functions
GRANT EXECUTE ON FUNCTION search_sql(jsonb, text) TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_cursor(jsonb, text, refcursor) TO pgstac_read;
GRANT EXECUTE ON FUNCTION fields_to_columns(jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION fields_to_rowjsonb(jsonb, text[]) TO pgstac_read;
GRANT EXECUTE ON FUNCTION collection_fragments_properties(text) TO pgstac_read;
ALTER FUNCTION collection_fragments_properties(text) SECURITY DEFINER;
-- v0.10 streaming engine: keyset pagination + search_page
GRANT EXECUTE ON FUNCTION keyset_encode(text[]) TO pgstac_read;
GRANT EXECUTE ON FUNCTION keyset_decode(text) TO pgstac_read;
GRANT EXECUTE ON FUNCTION keyset_sortkeys(jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION keyset_where(jsonb, text[], boolean) TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_page(jsonb, integer, text, boolean, text) TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_rows(jsonb, integer, text, boolean) TO pgstac_read;
-- v0.10 partition_stats discovery: envelope extraction + indexed band chunker
GRANT EXECUTE ON FUNCTION search_to_cql2(jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_envelope(jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION cql2_envelope(jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION cql2_collection_set(text, jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION q_op_query(jsonb) TO pgstac_read;
GRANT EXECUTE ON FUNCTION chunker(pred_envelope) TO pgstac_read;
GRANT EXECUTE ON FUNCTION sync_partition_stats() TO pgstac_admin;
GRANT EXECUTE ON FUNCTION partition_sync_meta(text) TO pgstac_read;
GRANT EXECUTE ON FUNCTION partition_hashes(text) TO pgstac_read;
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

REVOKE ALL PRIVILEGES ON PROCEDURE run_queued_queries FROM public;
GRANT ALL ON PROCEDURE run_queued_queries TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_queries_intransaction FROM public;
GRANT ALL ON FUNCTION run_queued_queries_intransaction TO pgstac_admin;

REVOKE ALL PRIVILEGES ON PROCEDURE gc_deleted_items_log_committed(interval, integer) FROM public;
GRANT ALL ON PROCEDURE gc_deleted_items_log_committed(interval, integer) TO pgstac_admin;

RESET ROLE;

SET ROLE pgstac_ingest;
SELECT update_partition_stats_q(partition) FROM partitions_view;
