-- Before the queryables below: their trigger accepts only a registered wrapper.
INSERT INTO queryable_wrappers (name) VALUES
  ('to_int'), ('to_float'), ('to_tstz'), ('to_text'), ('to_text_array')
ON CONFLICT DO NOTHING;

INSERT INTO queryables (name, definition)
  SELECT * FROM (VALUES
    ('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}'::jsonb),
    ('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json#/properties/geometry"}'),
    ('datetime', '{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}')
  ) v (name, definition)
  WHERE NOT EXISTS (SELECT FROM queryables WHERE name = v.name);

-- Rewrites rows an older release stored in another spelling; a no-op otherwise.
SELECT canonicalize_queryables();

-- Point the geometry queryable at the Feature's geometry if it is set to the whole Feature.
UPDATE queryables
SET definition = '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json#/properties/geometry"}'
WHERE name = 'geometry' AND collection_ids IS NULL
  AND definition = '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}'::jsonb;

-- Reference indexes for rows that predate them; a row whose index cannot be built is warned about.
SELECT maintain_reference_index();


INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default_filter_lang', 'cql2-json'),
  ('additional_properties', 'true'),
  ('use_queue', 'false'),
  ('queue_timeout', '10 minutes'),
  ('queue_retries', '3'),
  ('update_collection_extent', 'false'),
  ('format_cache', 'false'),
  ('readonly', 'false')
ON CONFLICT DO NOTHING
;


INSERT INTO cql2_ops (op, template) VALUES
    ('eq', '%s = %s'),
    ('neq', '%s != %s'),
    ('ne', '%s != %s'),
    ('!=', '%s != %s'),
    ('<>', '%s != %s'),
    ('lt', '%s < %s'),
    ('lte', '%s <= %s'),
    ('gt', '%s > %s'),
    ('gte', '%s >= %s'),
    ('le', '%s <= %s'),
    ('ge', '%s >= %s'),
    ('=', '%s = %s'),
    ('<', '%s < %s'),
    ('<=', '%s <= %s'),
    ('>', '%s > %s'),
    ('>=', '%s >= %s'),
    ('like', '%s LIKE %s'),
    ('ilike', '%s ILIKE %s'),
    ('+', '%s + %s'),
    ('-', '%s - %s'),
    ('*', '%s * %s'),
    ('/', '%s / %s'),
    ('not', 'NOT (%s)'),
    ('between', '%s BETWEEN %s AND %s'),
    ('isnull', '%s IS NULL'),
    ('upper', 'upper(%s)'),
    ('lower', 'lower(%s)'),
    ('casei', 'upper(%s)'),
    ('accenti', 'unaccent(%s)')
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
ALTER FUNCTION maintain_reference_index SECURITY DEFINER;
ALTER FUNCTION collection_delete_trigger_func SECURITY DEFINER;

-- Created SECURITY INVOKER; these reset databases migrated from a release
-- that created them SECURITY DEFINER.
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

-- Registering a wrapper is an admin action, and the index template is written by nobody;
-- pgstac_ingest keeps SELECT on both through pgstac_read.
REVOKE ALL ON queryable_wrappers, queryable_index_template FROM pgstac_ingest;

REVOKE ALL PRIVILEGES ON PROCEDURE run_queued_queries FROM public;
GRANT ALL ON PROCEDURE run_queued_queries TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_queries_intransaction FROM public;
GRANT ALL ON FUNCTION run_queued_queries_intransaction TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_query FROM public;
GRANT ALL ON FUNCTION run_queued_query TO pgstac_admin;

-- Deletes from the queue, so it is admin-only like the runners that call it.
REVOKE ALL PRIVILEGES ON FUNCTION retire_queued_queries FROM public;
GRANT ALL ON FUNCTION retire_queued_queries TO pgstac_admin;

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
    maintain_reference_index,
    delete_collection,
    collection_delete_trigger_func
FROM public;

-- A role that can execute a definer trigger function can attach it to its own
-- table. Only the owner's trigger on collections runs this one: CREATE TRIGGER
-- checks EXECUTE, firing does not.
REVOKE ALL PRIVILEGES ON FUNCTION collection_delete_trigger_func FROM pgstac_ingest, pgstac_read;

RESET ROLE;

SET ROLE pgstac_ingest;

-- Search finds partitions through partition_stats, so this must be synchronous
-- rather than queued.
SELECT sync_partition_stats();

-- Repairs observed ranges and CHECK constraints for every partition, ordered
-- by partition as every other writer of these rows is. Queued whatever use_queue
-- says: run inline, each partition's SHARE UPDATE EXCLUSIVE lock is held to the
-- end of the migration, where it deadlocks against autovacuum's ANALYZE and takes
-- the whole upgrade with it. Queued, each is its own short transaction, retried on
-- failure. pypgstac migrate drains the queue once the schema change has committed.
SET pgstac.use_queue TO TRUE;
SELECT update_partition_stats_q(partition) FROM partitions_view ORDER BY partition;
RESET pgstac.use_queue;
