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
-- First block: insert new rows only. Second block below: update existing rows that have
-- property_path=NULL (handles databases upgraded from pre-v0.10 without re-installing).
WITH promoted_queryables(name, definition, property_path, property_wrapper) AS (
    VALUES
  ('stac_version', '{"description": "STAC specification version","type": "string","title": "STAC Version"}'::jsonb, 'stac_version', 'to_text'),
  ('stac_extensions', '{"description": "List of STAC extension schema URIs","type": "array","title": "STAC Extensions"}'::jsonb, 'stac_extensions', 'to_text'),
  ('created', '{"description": "Metadata creation timestamp","type": "string","format": "date-time","title": "Created"}'::jsonb, 'created', 'to_tstz'),
  ('updated', '{"description": "Metadata update timestamp","type": "string","format": "date-time","title": "Updated"}'::jsonb, 'updated', 'to_tstz'),
  ('platform', '{"description": "Platform name","type": "string","title": "Platform"}'::jsonb, 'platform', 'to_text'),
  ('instruments', '{"description": "Instrument names","type": "array","title": "Instruments"}'::jsonb, 'instruments', 'to_text_array'),
  ('constellation', '{"description": "Constellation name","type": "string","title": "Constellation"}'::jsonb, 'constellation', 'to_text'),
  ('mission', '{"description": "Mission name","type": "string","title": "Mission"}'::jsonb, 'mission', 'to_text'),
        ('eo:cloud_cover', '{"description": "EO cloud cover percentage","type": "number","title": "Cloud Cover"}'::jsonb, 'eo_cloud_cover', 'to_float'),
  ('eo:bands', '{"description": "EO band metadata","type": "array","title": "EO Bands"}'::jsonb, 'eo_bands', 'to_text'),
        ('eo:snow_cover', '{"description": "EO snow cover percentage","type": "number","title": "Snow Cover"}'::jsonb, 'eo_snow_cover', 'to_float'),
        ('gsd', '{"description": "Ground sample distance","type": "number","title": "Ground Sample Distance"}'::jsonb, 'gsd', 'to_float'),
  ('proj:epsg', '{"description": "EPSG code","type": "integer","title": "Projection EPSG"}'::jsonb, 'proj_epsg', 'to_int'),
  ('proj:wkt2', '{"description": "WKT2 CRS definition","type": "string","title": "Projection WKT2"}'::jsonb, 'proj_wkt2', 'to_text'),
  ('proj:projjson', '{"description": "PROJJSON CRS definition","type": ["object", "string"],"title": "Projection PROJJSON"}'::jsonb, 'proj_projjson', 'to_text'),
  ('proj:bbox', '{"description": "Projection bbox","type": "array","title": "Projection BBOX"}'::jsonb, 'proj_bbox', 'to_text'),
  ('proj:centroid', '{"description": "Projection centroid","type": "object","title": "Projection Centroid"}'::jsonb, 'proj_centroid', 'to_text'),
  ('proj:shape', '{"description": "Projection shape","type": "array","title": "Projection Shape"}'::jsonb, 'proj_shape', 'to_text'),
  ('proj:transform', '{"description": "Projection affine transform","type": "array","title": "Projection Transform"}'::jsonb, 'proj_transform', 'to_text'),
  ('sci:doi', '{"description": "Scientific DOI","type": "string","title": "Scientific DOI"}'::jsonb, 'sci_doi', 'to_text'),
  ('sci:citation', '{"description": "Scientific citation","type": "string","title": "Scientific Citation"}'::jsonb, 'sci_citation', 'to_text'),
  ('sci:publications', '{"description": "Scientific publications","type": "array","title": "Scientific Publications"}'::jsonb, 'sci_publications', 'to_text'),
        ('view:off_nadir', '{"description": "Viewing angle off nadir","type": "number","title": "View Off Nadir"}'::jsonb, 'view_off_nadir', 'to_float'),
  ('view:incidence_angle', '{"description": "View incidence angle","type": "number","title": "View Incidence Angle"}'::jsonb, 'view_incidence_angle', 'to_float'),
  ('view:azimuth', '{"description": "View azimuth angle","type": "number","title": "View Azimuth"}'::jsonb, 'view_azimuth', 'to_float'),
        ('view:sun_azimuth', '{"description": "Sun azimuth angle","type": "number","title": "View Sun Azimuth"}'::jsonb, 'view_sun_azimuth', 'to_float'),
  ('view:sun_elevation', '{"description": "Sun elevation angle","type": "number","title": "View Sun Elevation"}'::jsonb, 'view_sun_elevation', 'to_float'),
  ('file:size', '{"description": "File size in bytes","type": "integer","title": "File Size"}'::jsonb, 'file_size', 'to_int'),
  ('file:header_size', '{"description": "File header size in bytes","type": "integer","title": "File Header Size"}'::jsonb, 'file_header_size', 'to_int'),
  ('file:checksum', '{"description": "File checksum","type": "string","title": "File Checksum"}'::jsonb, 'file_checksum', 'to_text'),
  ('file:byte_order', '{"description": "File byte order","type": "string","title": "File Byte Order"}'::jsonb, 'file_byte_order', 'to_text'),
  ('file:values_regex', '{"description": "File values regex","type": "string","title": "File Values Regex"}'::jsonb, 'file_values_regex', 'to_text'),
  ('sat:orbit_state', '{"description": "Satellite orbit state","type": "string","title": "Orbit State"}'::jsonb, 'sat_orbit_state', 'to_text'),
  ('sat:relative_orbit', '{"description": "Satellite relative orbit","type": "integer","title": "Relative Orbit"}'::jsonb, 'sat_relative_orbit', 'to_int'),
  ('sat:absolute_orbit', '{"description": "Satellite absolute orbit","type": "integer","title": "Absolute Orbit"}'::jsonb, 'sat_absolute_orbit', 'to_int')
), inserted AS (
    INSERT INTO queryables (name, definition, property_path, property_wrapper)
    SELECT p.name, p.definition, p.property_path, p.property_wrapper
    FROM promoted_queryables p
    WHERE NOT EXISTS (
        SELECT 1
        FROM queryables q
        WHERE q.name = p.name
    )
    RETURNING name
)
SELECT count(*) FROM inserted;

WITH promoted_queryables(name, definition, property_path, property_wrapper) AS (
    VALUES
  ('stac_version', '{"description": "STAC specification version","type": "string","title": "STAC Version"}'::jsonb, 'stac_version', 'to_text'),
  ('stac_extensions', '{"description": "List of STAC extension schema URIs","type": "array","title": "STAC Extensions"}'::jsonb, 'stac_extensions', 'to_text'),
  ('created', '{"description": "Metadata creation timestamp","type": "string","format": "date-time","title": "Created"}'::jsonb, 'created', 'to_tstz'),
  ('updated', '{"description": "Metadata update timestamp","type": "string","format": "date-time","title": "Updated"}'::jsonb, 'updated', 'to_tstz'),
  ('platform', '{"description": "Platform name","type": "string","title": "Platform"}'::jsonb, 'platform', 'to_text'),
  ('instruments', '{"description": "Instrument names","type": "array","title": "Instruments"}'::jsonb, 'instruments', 'to_text_array'),
  ('constellation', '{"description": "Constellation name","type": "string","title": "Constellation"}'::jsonb, 'constellation', 'to_text'),
  ('mission', '{"description": "Mission name","type": "string","title": "Mission"}'::jsonb, 'mission', 'to_text'),
        ('eo:cloud_cover', '{"description": "EO cloud cover percentage","type": "number","title": "Cloud Cover"}'::jsonb, 'eo_cloud_cover', 'to_float'),
  ('eo:bands', '{"description": "EO band metadata","type": "array","title": "EO Bands"}'::jsonb, 'eo_bands', 'to_text'),
        ('eo:snow_cover', '{"description": "EO snow cover percentage","type": "number","title": "Snow Cover"}'::jsonb, 'eo_snow_cover', 'to_float'),
        ('gsd', '{"description": "Ground sample distance","type": "number","title": "Ground Sample Distance"}'::jsonb, 'gsd', 'to_float'),
  ('proj:epsg', '{"description": "EPSG code","type": "integer","title": "Projection EPSG"}'::jsonb, 'proj_epsg', 'to_int'),
  ('proj:wkt2', '{"description": "WKT2 CRS definition","type": "string","title": "Projection WKT2"}'::jsonb, 'proj_wkt2', 'to_text'),
  ('proj:projjson', '{"description": "PROJJSON CRS definition","type": ["object", "string"],"title": "Projection PROJJSON"}'::jsonb, 'proj_projjson', 'to_text'),
  ('proj:bbox', '{"description": "Projection bbox","type": "array","title": "Projection BBOX"}'::jsonb, 'proj_bbox', 'to_text'),
  ('proj:centroid', '{"description": "Projection centroid","type": "object","title": "Projection Centroid"}'::jsonb, 'proj_centroid', 'to_text'),
  ('proj:shape', '{"description": "Projection shape","type": "array","title": "Projection Shape"}'::jsonb, 'proj_shape', 'to_text'),
  ('proj:transform', '{"description": "Projection affine transform","type": "array","title": "Projection Transform"}'::jsonb, 'proj_transform', 'to_text'),
  ('sci:doi', '{"description": "Scientific DOI","type": "string","title": "Scientific DOI"}'::jsonb, 'sci_doi', 'to_text'),
  ('sci:citation', '{"description": "Scientific citation","type": "string","title": "Scientific Citation"}'::jsonb, 'sci_citation', 'to_text'),
  ('sci:publications', '{"description": "Scientific publications","type": "array","title": "Scientific Publications"}'::jsonb, 'sci_publications', 'to_text'),
        ('view:off_nadir', '{"description": "Viewing angle off nadir","type": "number","title": "View Off Nadir"}'::jsonb, 'view_off_nadir', 'to_float'),
  ('view:incidence_angle', '{"description": "View incidence angle","type": "number","title": "View Incidence Angle"}'::jsonb, 'view_incidence_angle', 'to_float'),
  ('view:azimuth', '{"description": "View azimuth angle","type": "number","title": "View Azimuth"}'::jsonb, 'view_azimuth', 'to_float'),
        ('view:sun_azimuth', '{"description": "Sun azimuth angle","type": "number","title": "View Sun Azimuth"}'::jsonb, 'view_sun_azimuth', 'to_float'),
  ('view:sun_elevation', '{"description": "Sun elevation angle","type": "number","title": "View Sun Elevation"}'::jsonb, 'view_sun_elevation', 'to_float'),
  ('file:size', '{"description": "File size in bytes","type": "integer","title": "File Size"}'::jsonb, 'file_size', 'to_int'),
  ('file:header_size', '{"description": "File header size in bytes","type": "integer","title": "File Header Size"}'::jsonb, 'file_header_size', 'to_int'),
  ('file:checksum', '{"description": "File checksum","type": "string","title": "File Checksum"}'::jsonb, 'file_checksum', 'to_text'),
  ('file:byte_order', '{"description": "File byte order","type": "string","title": "File Byte Order"}'::jsonb, 'file_byte_order', 'to_text'),
  ('file:values_regex', '{"description": "File values regex","type": "string","title": "File Values Regex"}'::jsonb, 'file_values_regex', 'to_text'),
  ('sat:orbit_state', '{"description": "Satellite orbit state","type": "string","title": "Orbit State"}'::jsonb, 'sat_orbit_state', 'to_text'),
  ('sat:relative_orbit', '{"description": "Satellite relative orbit","type": "integer","title": "Relative Orbit"}'::jsonb, 'sat_relative_orbit', 'to_int'),
  ('sat:absolute_orbit', '{"description": "Satellite absolute orbit","type": "integer","title": "Absolute Orbit"}'::jsonb, 'sat_absolute_orbit', 'to_int')
)
UPDATE queryables q
SET property_path = CASE
      WHEN q.property_index_type IS NULL THEN COALESCE(q.property_path, p.property_path)
      ELSE q.property_path
    END,
    property_wrapper = CASE
      WHEN q.property_index_type IS NULL THEN COALESCE(q.property_wrapper, p.property_wrapper)
      ELSE q.property_wrapper
    END,
    definition = COALESCE(q.definition, p.definition)
FROM promoted_queryables p
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
ALTER FUNCTION format_item SECURITY DEFINER;
ALTER FUNCTION maintain_index SECURITY DEFINER;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
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
