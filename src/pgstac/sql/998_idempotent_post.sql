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

ALTER FUNCTION drop_table_constraints SECURITY DEFINER;
ALTER FUNCTION create_table_constraints SECURITY DEFINER;
ALTER FUNCTION check_partition SECURITY DEFINER;
ALTER FUNCTION repartition SECURITY DEFINER;
ALTER FUNCTION maintain_index SECURITY DEFINER;

-- Elevated only where ownership of pgstac_admin's objects is required.
-- Everything else relies on table privileges, which any role inheriting
-- pgstac_ingest or pgstac_read already has.
ALTER FUNCTION where_stats SECURITY INVOKER;
ALTER FUNCTION search_query SECURITY INVOKER;
ALTER FUNCTION format_item SECURITY INVOKER;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;

-- The caches maintained by where_stats, search_query and format_item.
GRANT SELECT, INSERT, UPDATE ON search_wheres TO pgstac_read;
GRANT SELECT, INSERT, UPDATE ON searches TO pgstac_read;
GRANT SELECT, INSERT, UPDATE ON format_item_cache TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

REVOKE ALL PRIVILEGES ON PROCEDURE run_queued_queries FROM public;
GRANT ALL ON PROCEDURE run_queued_queries TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_queries_intransaction FROM public;
GRANT ALL ON FUNCTION run_queued_queries_intransaction TO pgstac_admin;

-- PostgreSQL grants EXECUTE to PUBLIC on every new function, so each definer
-- is revoked and granted back to the roles that need it. Keep in step with the
-- ALTER FUNCTION ... SECURITY DEFINER statements above; the pgtap suite fails
-- if a definer is left PUBLIC executable.
REVOKE ALL PRIVILEGES ON FUNCTION
    drop_table_constraints,
    create_table_constraints,
    check_partition,
    repartition,
    maintain_index,
    delete_collection
FROM public;

GRANT EXECUTE ON FUNCTION
    drop_table_constraints,
    create_table_constraints,
    check_partition,
    repartition,
    maintain_index,
    delete_collection
TO pgstac_ingest;

RESET ROLE;

SET ROLE pgstac_ingest;

-- Search finds partitions through partition_stats, so this must be synchronous
-- rather than queued.
SELECT sync_partition_stats();

-- Repairs observed ranges and CHECK constraints for every partition, ordered
-- by partition as every other writer of these rows is. The most expensive part
-- of an install on a large catalog: run with pgstac.use_queue on (pypgstac
-- --usequeue) and drain with pypgstac runqueue to keep it off the migration.
SELECT update_partition_stats_q(partition) FROM partitions_view ORDER BY partition;
