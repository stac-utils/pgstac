SET client_min_messages TO WARNING;
SET SEARCH_PATH to pgstac, public;
RESET ROLE;
DO $$
DECLARE
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis') THEN
    CREATE EXTENSION IF NOT EXISTS postgis;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='btree_gist') THEN
    CREATE EXTENSION IF NOT EXISTS btree_gist;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='unaccent') THEN
    CREATE EXTENSION IF NOT EXISTS unaccent;
  END IF;
END;
$$ LANGUAGE PLPGSQL;

DO $$
  BEGIN
    CREATE ROLE pgstac_admin;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    CREATE ROLE pgstac_read;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    CREATE ROLE pgstac_ingest;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;


GRANT pgstac_admin TO current_user;

-- Function to make sure pgstac_admin is the owner of items
CREATE OR REPLACE FUNCTION pgstac_admin_owns() RETURNS VOID AS $$
DECLARE
  f RECORD;
BEGIN
  FOR f IN (
    SELECT
      concat(
        oid::regproc::text,
        '(',
        coalesce(pg_get_function_identity_arguments(oid),''),
        ')'
      ) AS name,
      CASE prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' WHEN 'a' THEN 'AGGREGATE' END as typ
    FROM pg_proc
    WHERE
      pronamespace=to_regnamespace('pgstac')
      AND proowner != to_regrole('pgstac_admin')
      AND proname NOT LIKE 'pg_stat%'
  )
  LOOP
    BEGIN
      EXECUTE format('ALTER %s %s OWNER TO pgstac_admin;', f.typ, f.name);
    EXCEPTION WHEN others THEN
      RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
  END LOOP;
  FOR f IN (
    SELECT
      oid::regclass::text as name,
      CASE relkind
        WHEN 'i' THEN 'INDEX'
        WHEN 'I' THEN 'INDEX'
        WHEN 'p' THEN 'TABLE'
        WHEN 'r' THEN 'TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'S' THEN 'SEQUENCE'
        ELSE NULL
      END as typ
    FROM pg_class
    WHERE relnamespace=to_regnamespace('pgstac') and relowner != to_regrole('pgstac_admin') AND relkind IN ('r','p','v','S') AND relname NOT LIKE 'pg_stat'
  )
  LOOP
    BEGIN
      EXECUTE format('ALTER %s %s OWNER TO pgstac_admin;', f.typ, f.name);
    EXCEPTION WHEN others THEN
      RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;
SELECT pgstac_admin_owns();

CREATE SCHEMA IF NOT EXISTS pgstac AUTHORIZATION pgstac_admin;

GRANT ALL ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_admin;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_admin;

ALTER ROLE pgstac_admin SET SEARCH_PATH TO pgstac, public;
ALTER ROLE pgstac_read SET SEARCH_PATH TO pgstac, public;
ALTER ROLE pgstac_ingest SET SEARCH_PATH TO pgstac, public;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;

GRANT pgstac_read TO pgstac_ingest;
GRANT ALL ON SCHEMA pgstac TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;

SET ROLE pgstac_admin;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;
RESET ROLE;

SET ROLE pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;
RESET ROLE;

SET SEARCH_PATH TO pgstac, public;
SET ROLE pgstac_admin;

DO $$
  BEGIN
    DROP FUNCTION IF EXISTS analyze_items;
  EXCEPTION WHEN others THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;
DO $$
  BEGIN
    DROP FUNCTION IF EXISTS validate_constraints;
  EXCEPTION WHEN others THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

-- Install these idempotently as migrations do not put them before trying to modify the collections table


CREATE OR REPLACE FUNCTION collection_geom(content jsonb)
RETURNS geometry AS $$
    WITH box AS (SELECT content->'extent'->'spatial'->'bbox'->0 as box)
    SELECT
        st_makeenvelope(
            (box->>0)::float,
            (box->>1)::float,
            (box->>2)::float,
            (box->>3)::float,
            4326
        )
    FROM box;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION collection_datetime(content jsonb)
RETURNS timestamptz AS $$
    SELECT
        CASE
            WHEN
                (content->'extent'->'temporal'->'interval'->0->>0) IS NULL
            THEN '-infinity'::timestamptz
            ELSE
                (content->'extent'->'temporal'->'interval'->0->>0)::timestamptz
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION collection_enddatetime(content jsonb)
RETURNS timestamptz AS $$
    SELECT
        CASE
            WHEN
                (content->'extent'->'temporal'->'interval'->0->>1) IS NULL
            THEN 'infinity'::timestamptz
            ELSE
                (content->'extent'->'temporal'->'interval'->0->>1)::timestamptz
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;
-- BEGIN migra calculated SQL
drop materialized view if exists "pgstac"."partition_steps";

drop view if exists "pgstac"."partition_sys_meta";

drop materialized view if exists "pgstac"."partitions";

drop view if exists "pgstac"."partitions_view";

drop function if exists "pgstac"."dt_constraint"(coid oid, OUT dt tstzrange, OUT edt tstzrange);

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.get_partition_name(relid regclass)
 RETURNS text
 LANGUAGE sql
 STABLE STRICT
AS $function$
    SELECT (parse_ident(relid::text))[cardinality(parse_ident(relid::text))];
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_tstz_constraint(reloid oid, colname text)
 RETURNS tstzrange
 LANGUAGE plpgsql
 STABLE STRICT
AS $function$
DECLARE
    expr text := NULL;
    m text[];
    ts_lower timestamptz := NULL;
    ts_upper timestamptz := NULL;
    lower_inclusive text := '[';
    upper_inclusive text := ']';
    ts timestamptz;
BEGIN
    SELECT INTO expr
        string_agg(def, ' AND ')
    FROM pg_constraint JOIN LATERAL pg_get_constraintdef(oid) AS def ON TRUE
    WHERE
        conrelid = reloid
        AND contype = 'c'
        AND def LIKE '%' || colname || '%'
    ;

    IF expr IS NULL THEN
        RETURN NULL;
    END IF;

    RAISE DEBUG 'Constraint expression for % on %: %', colname, reloid::regclass, expr;
    -- collect all constraints for the specified column
    FOR m IN SELECT regexp_matches(expr, '[ (]' || colname || $expr$\s*([<>=]{1,2})\s*'([0-9 :+\-]+)'$expr$, 'g') LOOP
        ts := m[2]::timestamptz;
        IF m[1] IN ('>', '>=')
        THEN
            IF ts_lower IS NULL OR ts > ts_lower OR (ts = ts_lower AND m[1] = '>') THEN
                ts_lower := ts;
                lower_inclusive := CASE WHEN m[1] = '>' THEN '(' ELSE '[' END;
            END IF;
        ELSIF m[1] IN ('<', '<=')
        THEN
            IF ts_upper IS NULL OR ts < ts_upper OR (ts = ts_upper AND m[1] = '<') THEN
                ts_upper := ts;
                upper_inclusive := CASE WHEN m[1] = '<' THEN ')' ELSE ']' END;
            END IF;
        END IF;
    END LOOP;
    RAISE DEBUG 'Constraint % for %: % %', colname, reloid::regclass, ts_lower, ts_upper;
    RETURN tstzrange(ts_lower, ts_upper, lower_inclusive || upper_inclusive);
END;
$function$
;

create or replace view "pgstac"."partition_sys_meta" as  SELECT partition.partition,
    replace(replace(
        CASE
            WHEN (pg_partition_tree.level = 1) THEN partition_expr.partition_expr
            ELSE parent_partition_expr.parent_partition_expr
        END, 'FOR VALUES IN ('''::text, ''::text), ''')'::text, ''::text) AS collection,
    pg_partition_tree.level,
    c.reltuples,
    c.relhastriggers,
    partition_dtrange.partition_dtrange,
    COALESCE(get_tstz_constraint(c.oid, 'datetime'::text), partition_dtrange.partition_dtrange, inf_range.inf_range) AS constraint_dtrange,
    COALESCE(get_tstz_constraint(c.oid, 'end_datetime'::text), inf_range.inf_range) AS constraint_edtrange
   FROM ((((((((((pg_partition_tree('items'::regclass) pg_partition_tree(relid, parentrelid, isleaf, level)
     JOIN pg_class c ON (((pg_partition_tree.relid)::oid = c.oid)))
     JOIN pg_class parent ON ((((pg_partition_tree.parentrelid)::oid = parent.oid) AND pg_partition_tree.isleaf)))
     LEFT JOIN pg_constraint edt ON (((edt.conrelid = c.oid) AND (edt.contype = 'c'::"char"))))
     JOIN LATERAL get_partition_name(pg_partition_tree.relid) partition(partition) ON (true))
     JOIN LATERAL pg_get_expr(c.relpartbound, c.oid) partition_expr(partition_expr) ON (true))
     JOIN LATERAL pg_get_expr(parent.relpartbound, parent.oid) parent_partition_expr(parent_partition_expr) ON (true))
     JOIN LATERAL tstzrange('-infinity'::timestamp with time zone, 'infinity'::timestamp with time zone, '[]'::text) inf_range(inf_range) ON (true))
     JOIN LATERAL COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), inf_range.inf_range) partition_dtrange(partition_dtrange) ON (true))
     JOIN LATERAL get_tstz_constraint(c.oid, 'datetime'::text) datetime_constraint(datetime_constraint) ON (true))
     JOIN LATERAL get_tstz_constraint(c.oid, 'end_datetime'::text) end_datetime_constraint(end_datetime_constraint) ON (true))
  WHERE pg_partition_tree.isleaf;


create or replace view "pgstac"."partitions_view" as  SELECT (parse_ident((pg_partition_tree.relid)::text))[cardinality(parse_ident((pg_partition_tree.relid)::text))] AS partition,
    replace(replace(
        CASE
            WHEN (pg_partition_tree.level = 1) THEN partition_expr.partition_expr
            ELSE parent_partition_expr.parent_partition_expr
        END, 'FOR VALUES IN ('''::text, ''::text), ''')'::text, ''::text) AS collection,
    pg_partition_tree.level,
    c.reltuples,
    c.relhastriggers,
    partition_dtrange.partition_dtrange,
    COALESCE(get_tstz_constraint(c.oid, 'datetime'::text), partition_dtrange.partition_dtrange, inf_range.inf_range) AS constraint_dtrange,
    COALESCE(get_tstz_constraint(c.oid, 'end_datetime'::text), inf_range.inf_range) AS constraint_edtrange,
    partition_stats.dtrange,
    partition_stats.edtrange,
    partition_stats.spatial,
    partition_stats.last_updated
   FROM (((((((((((pg_partition_tree('items'::regclass) pg_partition_tree(relid, parentrelid, isleaf, level)
     JOIN pg_class c ON (((pg_partition_tree.relid)::oid = c.oid)))
     JOIN pg_class parent ON ((((pg_partition_tree.parentrelid)::oid = parent.oid) AND pg_partition_tree.isleaf)))
     LEFT JOIN pg_constraint edt ON (((edt.conrelid = c.oid) AND (edt.contype = 'c'::"char"))))
     JOIN LATERAL get_partition_name(pg_partition_tree.relid) partition(partition) ON (true))
     JOIN LATERAL pg_get_expr(c.relpartbound, c.oid) partition_expr(partition_expr) ON (true))
     JOIN LATERAL pg_get_expr(parent.relpartbound, parent.oid) parent_partition_expr(parent_partition_expr) ON (true))
     JOIN LATERAL tstzrange('-infinity'::timestamp with time zone, 'infinity'::timestamp with time zone, '[]'::text) inf_range(inf_range) ON (true))
     JOIN LATERAL COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), inf_range.inf_range) partition_dtrange(partition_dtrange) ON (true))
     JOIN LATERAL get_tstz_constraint(c.oid, 'datetime'::text) datetime_constraint(datetime_constraint) ON (true))
     JOIN LATERAL get_tstz_constraint(c.oid, 'end_datetime'::text) end_datetime_constraint(end_datetime_constraint) ON (true))
     LEFT JOIN partition_stats USING (partition))
  WHERE pg_partition_tree.isleaf;


create materialized view "pgstac"."partition_steps" as  SELECT partition AS name,
    date_trunc('month'::text, lower(partition_dtrange)) AS sdate,
    (date_trunc('month'::text, upper(partition_dtrange)) + '1 mon'::interval) AS edate
   FROM partitions_view
  WHERE ((partition_dtrange IS NOT NULL) AND (partition_dtrange <> 'empty'::tstzrange))
  ORDER BY dtrange;


create materialized view "pgstac"."partitions" as  SELECT partition,
    collection,
    level,
    reltuples,
    relhastriggers,
    partition_dtrange,
    constraint_dtrange,
    constraint_edtrange,
    dtrange,
    edtrange,
    spatial,
    last_updated
   FROM partitions_view;



-- END migra calculated SQL
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

ALTER FUNCTION update_partition_stats SECURITY DEFINER;
ALTER FUNCTION partition_after_triggerfunc SECURITY DEFINER;
ALTER FUNCTION drop_table_constraints SECURITY DEFINER;
ALTER FUNCTION create_table_constraints SECURITY DEFINER;
ALTER FUNCTION check_partition SECURITY DEFINER;
ALTER FUNCTION repartition SECURITY DEFINER;
ALTER FUNCTION where_stats SECURITY DEFINER;
ALTER FUNCTION search_query SECURITY DEFINER;
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

RESET ROLE;

SET ROLE pgstac_ingest;
SELECT update_partition_stats_q(partition) FROM partitions_view;
SELECT set_version('0.9.10');
