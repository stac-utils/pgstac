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

CREATE TABLE IF NOT EXISTS migrations (
  version text PRIMARY KEY,
  datetime timestamptz DEFAULT clock_timestamp() NOT NULL
);

CREATE OR REPLACE FUNCTION get_version() RETURNS text AS $$
  SELECT version FROM pgstac.migrations ORDER BY datetime DESC, version DESC LIMIT 1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION set_version(text) RETURNS text AS $$
  INSERT INTO pgstac.migrations (version) VALUES ($1)
  ON CONFLICT DO NOTHING
  RETURNING version;
$$ LANGUAGE SQL;


CREATE TABLE IF NOT EXISTS pgstac_settings (
  name text PRIMARY KEY,
  value text NOT NULL
);

CREATE OR REPLACE FUNCTION table_empty(text) RETURNS boolean AS $$
DECLARE
    retval boolean;
BEGIN
    EXECUTE format($q$
        SELECT NOT EXISTS (SELECT 1 FROM %I LIMIT 1)
        $q$,
        $1
    ) INTO retval;
    RETURN retval;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION get_setting(IN _setting text, IN conf jsonb DEFAULT NULL) RETURNS text AS $$
SELECT COALESCE(
  nullif(conf->>_setting, ''),
  nullif(current_setting(concat('pgstac.',_setting), TRUE),''),
  nullif((SELECT value FROM pgstac.pgstac_settings WHERE name=_setting),'')
);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_setting_bool(IN _setting text, IN conf jsonb DEFAULT NULL) RETURNS boolean AS $$
SELECT COALESCE(
  nullif(conf->>_setting, ''),
  nullif(current_setting(concat('pgstac.',_setting), TRUE),''),
  nullif((SELECT value FROM pgstac.pgstac_settings WHERE name=_setting),''),
  'FALSE'
)::boolean;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION base_url(conf jsonb DEFAULT NULL) RETURNS text AS $$
  SELECT COALESCE(pgstac.get_setting('base_url', conf), '.');
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION additional_properties() RETURNS boolean AS $$
    SELECT pgstac.get_setting_bool('additional_properties');
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION readonly(conf jsonb DEFAULT NULL) RETURNS boolean AS $$
    SELECT pgstac.get_setting_bool('readonly', conf);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context(conf jsonb DEFAULT NULL) RETURNS text AS $$
  SELECT pgstac.get_setting('context', conf);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context_estimated_count(conf jsonb DEFAULT NULL) RETURNS int AS $$
  SELECT pgstac.get_setting('context_estimated_count', conf)::int;
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_estimated_cost();
CREATE OR REPLACE FUNCTION context_estimated_cost(conf jsonb DEFAULT NULL) RETURNS float AS $$
  SELECT pgstac.get_setting('context_estimated_cost', conf)::float;
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_stats_ttl();
CREATE OR REPLACE FUNCTION context_stats_ttl(conf jsonb DEFAULT NULL) RETURNS interval AS $$
  SELECT pgstac.get_setting('context_stats_ttl', conf)::interval;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION t2s(text) RETURNS text AS $$
    SELECT extract(epoch FROM $1::interval)::text || ' s';
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION age_ms(a timestamptz, b timestamptz DEFAULT clock_timestamp()) RETURNS float AS $$
    SELECT abs(extract(epoch from age(a,b)) * 1000);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION queue_timeout() RETURNS interval AS $$
    SELECT t2s(coalesce(
            get_setting('queue_timeout'),
            '1h'
        ))::interval;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION notice(VARIADIC text[]) RETURNS boolean AS $$
DECLARE
debug boolean := current_setting('pgstac.debug', true);
BEGIN
    IF debug THEN
        RAISE NOTICE 'NOTICE FROM FUNC: %  >>>>> %', concat_ws(' | ', $1), clock_timestamp();
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION empty_arr(ANYARRAY) RETURNS BOOLEAN AS $$
SELECT CASE
  WHEN $1 IS NULL THEN TRUE
  WHEN cardinality($1)<1 THEN TRUE
ELSE FALSE
END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_intersection(_a ANYARRAY, _b ANYARRAY) RETURNS ANYARRAY AS $$
  SELECT ARRAY ( SELECT unnest(_a) INTERSECT SELECT UNNEST(_b) );
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION array_map_ident(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_ident(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_map_literal(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_literal(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE SQL STRICT IMMUTABLE;

DROP TABLE IF EXISTS query_queue;
CREATE TABLE query_queue (
    query text PRIMARY KEY,
    added timestamptz DEFAULT now()
);

DROP TABLE IF EXISTS query_queue_history;
CREATE TABLE query_queue_history(
    query text,
    added timestamptz NOT NULL,
    finished timestamptz NOT NULL DEFAULT now(),
    error text
);

CREATE OR REPLACE PROCEDURE run_queued_queries() AS $$
DECLARE
    qitem query_queue%ROWTYPE;
    timeout_ts timestamptz;
    error text;
    cnt int := 0;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        DELETE FROM query_queue WHERE query = (SELECT query FROM query_queue ORDER BY added DESC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING * INTO qitem;
        IF NOT FOUND THEN
            EXIT;
        END IF;
        cnt := cnt + 1;
        BEGIN
            RAISE NOTICE 'RUNNING QUERY: %', qitem.query;
            EXECUTE qitem.query;
            EXCEPTION WHEN others THEN
                error := format('%s | %s', SQLERRM, SQLSTATE);
        END;
        INSERT INTO query_queue_history (query, added, finished, error)
            VALUES (qitem.query, qitem.added, clock_timestamp(), error);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION run_queued_queries_intransaction() RETURNS int AS $$
DECLARE
    qitem query_queue%ROWTYPE;
    timeout_ts timestamptz;
    error text;
    cnt int := 0;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        DELETE FROM query_queue WHERE query = (SELECT query FROM query_queue ORDER BY added DESC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING * INTO qitem;
        IF NOT FOUND THEN
            RETURN cnt;
        END IF;
        cnt := cnt + 1;
        BEGIN
            qitem.query := regexp_replace(qitem.query, 'CONCURRENTLY', '');
            RAISE NOTICE 'RUNNING QUERY: %', qitem.query;

            EXECUTE qitem.query;
            EXCEPTION WHEN others THEN
                error := format('%s | %s', SQLERRM, SQLSTATE);
                RAISE WARNING '%', error;
        END;
        INSERT INTO query_queue_history (query, added, finished, error)
            VALUES (qitem.query, qitem.added, clock_timestamp(), error);
    END LOOP;
    RETURN cnt;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION run_or_queue(query text) RETURNS VOID AS $$
DECLARE
    use_queue text := COALESCE(get_setting('use_queue'), 'FALSE')::boolean;
BEGIN
    IF get_setting_bool('debug') THEN
        RAISE NOTICE '%', query;
    END IF;
    IF use_queue THEN
        INSERT INTO query_queue (query) VALUES (query) ON CONFLICT DO NOTHING;
    ELSE
        EXECUTE query;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;



DROP FUNCTION IF EXISTS check_pgstac_settings;
CREATE OR REPLACE FUNCTION check_pgstac_settings(_sysmem text DEFAULT NULL) RETURNS VOID AS $$
DECLARE
    settingval text;
    sysmem bigint := pg_size_bytes(_sysmem);
    effective_cache_size bigint := pg_size_bytes(current_setting('effective_cache_size', TRUE));
    shared_buffers bigint := pg_size_bytes(current_setting('shared_buffers', TRUE));
    work_mem bigint := pg_size_bytes(current_setting('work_mem', TRUE));
    max_connections int := current_setting('max_connections', TRUE);
    maintenance_work_mem bigint := pg_size_bytes(current_setting('maintenance_work_mem', TRUE));
    seq_page_cost float := current_setting('seq_page_cost', TRUE);
    random_page_cost float := current_setting('random_page_cost', TRUE);
    temp_buffers bigint := pg_size_bytes(current_setting('temp_buffers', TRUE));
    r record;
BEGIN
    IF _sysmem IS NULL THEN
      RAISE NOTICE 'Call function with the size of your system memory `SELECT check_pgstac_settings(''4GB'')` to get pg system setting recommendations.';
    ELSE
        IF effective_cache_size < (sysmem * 0.5) THEN
            RAISE WARNING 'effective_cache_size of % is set low for a system with %. Recomended value between % and %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.5), pg_size_pretty(sysmem * 0.75);
        ELSIF effective_cache_size > (sysmem * 0.75) THEN
            RAISE WARNING 'effective_cache_size of % is set high for a system with %. Recomended value between % and %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.5), pg_size_pretty(sysmem * 0.75);
        ELSE
            RAISE NOTICE 'effective_cache_size of % is set appropriately for a system with %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem);
        END IF;

        IF shared_buffers < (sysmem * 0.2) THEN
            RAISE WARNING 'shared_buffers of % is set low for a system with %. Recomended value between % and %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.2), pg_size_pretty(sysmem * 0.3);
        ELSIF shared_buffers > (sysmem * 0.3) THEN
            RAISE WARNING 'shared_buffers of % is set high for a system with %. Recomended value between % and %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.2), pg_size_pretty(sysmem * 0.3);
        ELSE
            RAISE NOTICE 'shared_buffers of % is set appropriately for a system with %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem);
        END IF;
        shared_buffers = sysmem * 0.3;
        IF maintenance_work_mem < (sysmem * 0.2) THEN
            RAISE WARNING 'maintenance_work_mem of % is set low for shared_buffers of %. Recomended value between % and %', pg_size_pretty(maintenance_work_mem), pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers * 0.2), pg_size_pretty(shared_buffers * 0.3);
        ELSIF maintenance_work_mem > (shared_buffers * 0.3) THEN
            RAISE WARNING 'maintenance_work_mem of % is set high for shared_buffers of %. Recomended value between % and %', pg_size_pretty(maintenance_work_mem), pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers * 0.2), pg_size_pretty(shared_buffers * 0.3);
        ELSE
            RAISE NOTICE 'maintenance_work_mem of % is set appropriately for shared_buffers of %', pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers);
        END IF;

        IF work_mem * max_connections > shared_buffers THEN
            RAISE WARNING 'work_mem setting of % is set high for % max_connections please reduce work_mem to % or decrease max_connections to %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers/max_connections), floor(shared_buffers/work_mem);
        ELSIF work_mem * max_connections < (shared_buffers * 0.75) THEN
            RAISE WARNING 'work_mem setting of % is set low for % max_connections you may consider raising work_mem to % or increasing max_connections to %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers/max_connections), floor(shared_buffers/work_mem);
        ELSE
            RAISE NOTICE 'work_mem setting of % and max_connections of % are adequate for shared_buffers of %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers);
        END IF;

        IF random_page_cost / seq_page_cost != 1.1 THEN
            RAISE WARNING 'random_page_cost (%) /seq_page_cost (%) should be set to 1.1 for SSD. Change random_page_cost to %', random_page_cost, seq_page_cost, 1.1 * seq_page_cost;
        ELSE
            RAISE NOTICE 'random_page_cost and seq_page_cost set appropriately for SSD';
        END IF;

        IF temp_buffers < greatest(pg_size_bytes('128MB'),(maintenance_work_mem / 2)) THEN
            RAISE WARNING 'pgstac makes heavy use of temp tables, consider raising temp_buffers from % to %', pg_size_pretty(temp_buffers), greatest('128MB', pg_size_pretty((shared_buffers / 16)));
        END IF;
    END IF;

    RAISE NOTICE 'VALUES FOR PGSTAC VARIABLES';
    RAISE NOTICE 'These can be set either as GUC system variables or by setting in the pgstac_settings table.';

    FOR r IN SELECT name, get_setting(name) as setting, CASE WHEN current_setting(concat('pgstac.',name), TRUE) IS NOT NULL THEN concat('pgstac.',name, ' GUC') WHEN value IS NOT NULL THEN 'pgstac_settings table' ELSE 'Not Set' END as loc FROM pgstac_settings LOOP
      RAISE NOTICE '% is set to % from the %', r.name, r.setting, r.loc;
    END LOOP;

    SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pg_cron';
    IF NOT FOUND OR settingval IS NULL THEN
        RAISE NOTICE 'Consider intalling pg_cron which can be used to automate tasks';
    ELSE
        RAISE NOTICE 'pg_cron % is installed', settingval;
    END IF;

    SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pgstattuple';
    IF NOT FOUND OR settingval IS NULL THEN
        RAISE NOTICE 'Consider installing the pgstattuple extension which can be used to help maintain tables and indexes.';
    ELSE
        RAISE NOTICE 'pgstattuple % is installed', settingval;
    END IF;

    SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pg_stat_statements';
    IF NOT FOUND OR settingval IS NULL THEN
        RAISE NOTICE 'Consider installing the pg_stat_statements extension which is very helpful for tracking the types of queries on the system';
    ELSE
        RAISE NOTICE 'pg_stat_statements % is installed', settingval;
        IF current_setting('pg_stat_statements.track_statements', TRUE) IS DISTINCT FROM 'all' THEN
            RAISE WARNING 'SET pg_stat_statements.track_statements TO ''all''; --In order to track statements within functions.';
        END IF;
    END IF;

END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public SET CLIENT_MIN_MESSAGES TO NOTICE;
CREATE OR REPLACE FUNCTION to_int(jsonb) RETURNS int AS $$
    SELECT floor(($1->>0)::float)::int;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_float(jsonb) RETURNS float AS $$
    SELECT ($1->>0)::float;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_tstz(jsonb) RETURNS timestamptz AS $$
    SELECT ($1->>0)::timestamptz;
$$ LANGUAGE SQL IMMUTABLE STRICT SET TIME ZONE 'UTC' COST 5000 PARALLEL SAFE;


CREATE OR REPLACE FUNCTION to_text(jsonb) RETURNS text AS $$
    SELECT CASE WHEN jsonb_typeof($1) IN ('array','object') THEN $1::text ELSE $1->>0 END;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_text_array(jsonb) RETURNS text[] AS $$
    SELECT
        CASE jsonb_typeof($1)
            WHEN 'array' THEN ARRAY(SELECT jsonb_array_elements_text($1))
            ELSE ARRAY[$1->>0]
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS geometry AS $$
SELECT CASE jsonb_array_length(_bbox)
    WHEN 4 THEN
        ST_SetSRID(ST_MakeEnvelope(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float,
            (_bbox->>3)::float
        ),4326)
    WHEN 6 THEN
    ST_SetSRID(ST_3DMakeBox(
        ST_MakePoint(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float
        ),
        ST_MakePoint(
            (_bbox->>3)::float,
            (_bbox->>4)::float,
            (_bbox->>5)::float
        )
    ),4326)
    ELSE null END;
;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION geom_bbox(_geom geometry) RETURNS jsonb AS $$
    SELECT jsonb_build_array(
        st_xmin(_geom),
        st_ymin(_geom),
        st_xmax(_geom),
        st_ymax(_geom)
    );
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION flip_jsonb_array(j jsonb) RETURNS jsonb AS $$
    SELECT jsonb_agg(value) FROM (SELECT value FROM jsonb_array_elements(j) WITH ORDINALITY ORDER BY ordinality DESC) as t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION explode_dotpaths(j jsonb) RETURNS SETOF text[] AS $$
    SELECT string_to_array(p, '.') as e FROM jsonb_array_elements_text(j) p;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION explode_dotpaths_recurse(IN j jsonb) RETURNS SETOF text[] AS $$
    WITH RECURSIVE t AS (
        SELECT e FROM explode_dotpaths(j) e
        UNION ALL
        SELECT e[1:cardinality(e)-1]
        FROM t
        WHERE cardinality(e)>1
    ) SELECT e FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_set_nested(j jsonb, path text[], val jsonb) RETURNS jsonb AS $$
DECLARE
BEGIN
    IF cardinality(path) > 1 THEN
        FOR i IN 1..(cardinality(path)-1) LOOP
            IF j #> path[:i] IS NULL THEN
                j := jsonb_set_lax(j, path[:i], '{}', TRUE);
            END IF;
        END LOOP;
    END IF;
    RETURN jsonb_set_lax(j, path, val, true);

END;
$$ LANGUAGE PLPGSQL IMMUTABLE;



CREATE OR REPLACE FUNCTION jsonb_include(j jsonb, f jsonb) RETURNS jsonb AS $$
DECLARE
    includes jsonb := f-> 'include';
    outj jsonb := '{}'::jsonb;
    path text[];
BEGIN
    IF
        includes IS NULL
        OR jsonb_array_length(includes) = 0
    THEN
        RETURN j;
    ELSE
        includes := includes || '["id","collection"]'::jsonb;
        FOR path IN SELECT explode_dotpaths(includes) LOOP
            outj := jsonb_set_nested(outj, path, j #> path);
        END LOOP;
    END IF;
    RETURN outj;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_exclude(j jsonb, f jsonb) RETURNS jsonb AS $$
DECLARE
    excludes jsonb := f-> 'exclude';
    outj jsonb := j;
    path text[];
BEGIN
    IF
        excludes IS NULL
        OR jsonb_array_length(excludes) = 0
    THEN
        RETURN j;
    ELSE
        FOR path IN SELECT explode_dotpaths(excludes) LOOP
            outj := outj #- path;
        END LOOP;
    END IF;
    RETURN outj;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_fields(j jsonb, f jsonb DEFAULT '{"fields":[]}') RETURNS jsonb AS $$
    SELECT jsonb_exclude(jsonb_include(j, f), f);
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION merge_jsonb(_a jsonb, _b jsonb) RETURNS jsonb AS $$
    SELECT
    CASE
        WHEN _a = '"𒍟※"'::jsonb THEN NULL
        WHEN _a IS NULL OR jsonb_typeof(_a) = 'null' THEN _b
        WHEN jsonb_typeof(_a) = 'object' AND jsonb_typeof(_b) = 'object' THEN
            (
                SELECT
                    jsonb_strip_nulls(
                        jsonb_object_agg(
                            key,
                            merge_jsonb(a.value, b.value)
                        )
                    )
                FROM
                    jsonb_each(coalesce(_a,'{}'::jsonb)) as a
                FULL JOIN
                    jsonb_each(coalesce(_b,'{}'::jsonb)) as b
                USING (key)
            )
        WHEN
            jsonb_typeof(_a) = 'array'
            AND jsonb_typeof(_b) = 'array'
            AND jsonb_array_length(_a) = jsonb_array_length(_b)
        THEN
            (
                SELECT jsonb_agg(m) FROM
                    ( SELECT
                        merge_jsonb(
                            jsonb_array_elements(_a),
                            jsonb_array_elements(_b)
                        ) as m
                    ) as l
            )
        ELSE _a
    END
    ;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION strip_jsonb(_a jsonb, _b jsonb) RETURNS jsonb AS $$
    SELECT
    CASE

        WHEN (_a IS NULL OR jsonb_typeof(_a) = 'null') AND _b IS NOT NULL AND jsonb_typeof(_b) != 'null' THEN '"𒍟※"'::jsonb
        WHEN _b IS NULL OR jsonb_typeof(_a) = 'null' THEN _a
        WHEN _a = _b AND jsonb_typeof(_a) = 'object' THEN '{}'::jsonb
        WHEN _a = _b THEN NULL
        WHEN jsonb_typeof(_a) = 'object' AND jsonb_typeof(_b) = 'object' THEN
            (
                SELECT
                    jsonb_strip_nulls(
                        jsonb_object_agg(
                            key,
                            strip_jsonb(a.value, b.value)
                        )
                    )
                FROM
                    jsonb_each(_a) as a
                FULL JOIN
                    jsonb_each(_b) as b
                USING (key)
            )
        WHEN
            jsonb_typeof(_a) = 'array'
            AND jsonb_typeof(_b) = 'array'
            AND jsonb_array_length(_a) = jsonb_array_length(_b)
        THEN
            (
                SELECT jsonb_agg(m) FROM
                    ( SELECT
                        strip_jsonb(
                            jsonb_array_elements(_a),
                            jsonb_array_elements(_b)
                        ) as m
                    ) as l
            )
        ELSE _a
    END
    ;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION nullif_jsonbnullempty(j jsonb) RETURNS jsonb AS $$
    SELECT nullif(nullif(nullif(j,'null'::jsonb),'{}'::jsonb),'[]'::jsonb);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION jsonb_array_unique(j jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(jsonb_agg(DISTINCT a)) v FROM jsonb_array_elements(j) a;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_concat_ignorenull(a jsonb, b jsonb) RETURNS jsonb AS $$
    SELECT coalesce(a,'[]'::jsonb) || coalesce(b,'[]'::jsonb);
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_least(a jsonb, b jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(least(nullif_jsonbnullempty(a), nullif_jsonbnullempty(b)));
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_greatest(a jsonb, b jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(greatest(a, b));
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION first_notnull_sfunc(anyelement, anyelement) RETURNS anyelement AS $$
    SELECT COALESCE($1,$2);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE AGGREGATE first_notnull(anyelement)(
    SFUNC = first_notnull_sfunc,
    STYPE = anyelement
);

CREATE OR REPLACE AGGREGATE jsonb_array_unique_merge(jsonb) (
    STYPE = jsonb,
    SFUNC = jsonb_concat_ignorenull,
    FINALFUNC = jsonb_array_unique
);

CREATE OR REPLACE AGGREGATE jsonb_min(jsonb) (
    STYPE = jsonb,
    SFUNC = jsonb_least
);

CREATE OR REPLACE AGGREGATE jsonb_max(jsonb) (
    STYPE = jsonb,
    SFUNC = jsonb_greatest
);
/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
    CASE
            WHEN value ? 'intersects' THEN
                ST_GeomFromGeoJSON(value->>'intersects')
            WHEN value ? 'geometry' THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value ? 'bbox' THEN
                pgstac.bbox_geom(value->'bbox')
            ELSE NULL
        END as geometry
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION stac_daterange(
    value jsonb
) RETURNS tstzrange AS $$
DECLARE
    props jsonb := value;
    dt timestamptz;
    edt timestamptz;
BEGIN
    IF props ? 'properties' THEN
        props := props->'properties';
    END IF;
    IF
        props ? 'start_datetime'
        AND props->>'start_datetime' IS NOT NULL
        AND props ? 'end_datetime'
        AND props->>'end_datetime' IS NOT NULL
    THEN
        dt := props->>'start_datetime';
        edt := props->>'end_datetime';
        IF dt > edt THEN
            RAISE EXCEPTION 'start_datetime must be < end_datetime';
        END IF;
    ELSE
        dt := props->>'datetime';
        edt := props->>'datetime';
    END IF;
    IF dt is NULL OR edt IS NULL THEN
        RAISE NOTICE 'DT: %, EDT: %', dt, edt;
        RAISE EXCEPTION 'Either datetime (%) or both start_datetime (%) and end_datetime (%) must be set.', props->>'datetime',props->>'start_datetime',props->>'end_datetime';
    END IF;
    RETURN tstzrange(dt, edt, '[]');
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_datetime(value jsonb) RETURNS timestamptz AS $$
    SELECT lower(stac_daterange(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_end_datetime(value jsonb) RETURNS timestamptz AS $$
    SELECT upper(stac_daterange(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE TABLE IF NOT EXISTS stac_extensions(
    url text PRIMARY KEY,
    content jsonb
);
CREATE OR REPLACE FUNCTION collection_base_item(content jsonb) RETURNS jsonb AS $$
    SELECT jsonb_build_object(
        'type', 'Feature',
        'stac_version', content->'stac_version',
        'assets', content->'item_assets',
        'collection', content->'id'
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE TABLE IF NOT EXISTS collections (
    key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id text GENERATED ALWAYS AS (content->>'id') STORED UNIQUE NOT NULL,
    content JSONB NOT NULL,
    base_item jsonb GENERATED ALWAYS AS (pgstac.collection_base_item(content)) STORED,
    geometry geometry GENERATED ALWAYS AS (pgstac.collection_geom(content)) STORED,
    datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_datetime(content)) STORED,
    end_datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_enddatetime(content)) STORED,
    private jsonb,
    partition_trunc text CHECK (partition_trunc IN ('year', 'month'))
);

CREATE OR REPLACE FUNCTION collection_base_item(cid text) RETURNS jsonb AS $$
    SELECT pgstac.collection_base_item(content) FROM pgstac.collections WHERE id = cid LIMIT 1;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION create_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION update_collection(data jsonb) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    UPDATE collections SET content=data WHERE id = data->>'id' RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ON CONFLICT (id) DO
    UPDATE
        SET content=EXCLUDED.content
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION delete_collection(_id text) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    DELETE FROM collections WHERE id = _id RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
    SELECT content FROM collections
    WHERE id=$1
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION all_collections() RETURNS jsonb AS $$
    SELECT coalesce(jsonb_agg(content), '[]'::jsonb) FROM collections;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION collection_delete_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    collection_base_partition text := concat('_items_', OLD.key);
BEGIN
    EXECUTE format($q$
        DELETE FROM partition_stats WHERE partition IN (
            SELECT partition FROM partition_sys_meta
            WHERE collection=%L
        );
        DROP TABLE IF EXISTS %I CASCADE;
        $q$,
        OLD.id,
        collection_base_partition
    );
    RETURN OLD;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER collection_delete_trigger BEFORE DELETE ON collections
FOR EACH ROW EXECUTE FUNCTION collection_delete_trigger_func();
CREATE OR REPLACE FUNCTION queryable_signature(n text, c text[]) RETURNS text AS $$
    SELECT concat(n, c);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TABLE queryables (
    id bigint GENERATED ALWAYS AS identity PRIMARY KEY,
    name text NOT NULL,
    collection_ids text[], -- used to determine what partitions to create indexes on
    definition jsonb,
    property_path text,
    property_wrapper text,
    property_index_type text
);
CREATE INDEX queryables_name_idx ON queryables (name);
CREATE INDEX queryables_collection_idx ON queryables USING GIN (collection_ids);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);

CREATE OR REPLACE FUNCTION pgstac.queryables_constraint_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    allcollections text[];
BEGIN
    RAISE NOTICE 'Making sure that name/collection is unique for queryables %', NEW;
    IF NEW.collection_ids IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
                FROM unnest(NEW.collection_ids) c
                LEFT JOIN
                collections
                ON (collections.id = c)
                WHERE collections.id IS NULL
        ) THEN
            RAISE foreign_key_violation USING MESSAGE = format(
                'One or more collections in %s do not exist.', NEW.collection_ids
            );
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF EXISTS (
            SELECT 1 FROM queryables q
            WHERE
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                )
        ) THEN
            RAISE unique_violation USING MESSAGE = format(
                'There is already a queryable for %s for a collection in %s: %s',
                NEW.name,
                NEW.collection_ids,
				(SELECT json_agg(row_to_json(q)) FROM queryables q WHERE
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                ))
            );
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF EXISTS (
            SELECT 1 FROM queryables q
            WHERE
                q.id != NEW.id
                AND
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                )
        ) THEN
            RAISE unique_violation
            USING MESSAGE = format(
                'There is already a queryable for %s for a collection in %s',
                NEW.name,
                NEW.collection_ids
            );
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER queryables_constraint_insert_trigger
BEFORE INSERT ON queryables
FOR EACH ROW EXECUTE PROCEDURE queryables_constraint_triggerfunc();

CREATE TRIGGER queryables_constraint_update_trigger
BEFORE UPDATE ON queryables
FOR EACH ROW
WHEN (NEW.name = OLD.name AND NEW.collection_ids IS DISTINCT FROM OLD.collection_ids)
EXECUTE PROCEDURE queryables_constraint_triggerfunc();


CREATE OR REPLACE FUNCTION array_to_path(arr text[]) RETURNS text AS $$
    SELECT string_agg(
        quote_literal(v),
        '->'
    ) FROM unnest(arr) v;
$$ LANGUAGE SQL IMMUTABLE STRICT;




CREATE OR REPLACE FUNCTION queryable(
    IN dotpath text,
    OUT path text,
    OUT expression text,
    OUT wrapper text,
    OUT nulled_wrapper text
) AS $$
DECLARE
    q RECORD;
    path_elements text[];
BEGIN
    IF dotpath IN ('id', 'geometry', 'datetime', 'end_datetime', 'collection') THEN
        path := dotpath;
        expression := dotpath;
        wrapper := NULL;
        RETURN;
    END IF;
    SELECT * INTO q FROM queryables
        WHERE
            name=dotpath
            OR name = 'properties.' || dotpath
            OR name = replace(dotpath, 'properties.', '')
    ;
    IF q.property_wrapper IS NULL THEN
        IF q.definition->>'type' = 'number' THEN
            wrapper := 'to_float';
            nulled_wrapper := wrapper;
        ELSIF q.definition->>'format' = 'date-time' THEN
            wrapper := 'to_tstz';
            nulled_wrapper := wrapper;
        ELSE
            nulled_wrapper := NULL;
            wrapper := 'to_text';
        END IF;
    ELSE
        wrapper := q.property_wrapper;
        nulled_wrapper := wrapper;
    END IF;
    IF q.property_path IS NOT NULL THEN
        path := q.property_path;
    ELSE
        path_elements := string_to_array(dotpath, '.');
        IF path_elements[1] IN ('links', 'assets', 'stac_version', 'stac_extensions') THEN
            path := format('content->%s', array_to_path(path_elements));
        ELSIF path_elements[1] = 'properties' THEN
            path := format('content->%s', array_to_path(path_elements));
        ELSE
            path := format($F$content->'properties'->%s$F$, array_to_path(path_elements));
        END IF;
    END IF;
    expression := format('%I(%s)', wrapper, path);
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;

CREATE OR REPLACE FUNCTION unnest_collection(collection_ids text[] DEFAULT NULL) RETURNS SETOF text AS $$
    DECLARE
    BEGIN
        IF collection_ids IS NULL THEN
            RETURN QUERY SELECT id FROM collections;
        END IF;
        RETURN QUERY SELECT unnest(collection_ids);
    END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION normalize_indexdef(def text) RETURNS text AS $$
DECLARE
BEGIN
    def := btrim(def, ' \n\t');
	def := regexp_replace(def, '^CREATE (UNIQUE )?INDEX ([^ ]* )?ON (ONLY )?([^ ]* )?', '', 'i');
    RETURN def;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION indexdef(q queryables) RETURNS text AS $$
    DECLARE
        out text;
    BEGIN
        IF q.name = 'id' THEN
            out := 'CREATE UNIQUE INDEX ON %I USING btree (id)';
        ELSIF q.name = 'datetime' THEN
            out := 'CREATE INDEX ON %I USING btree (datetime DESC, end_datetime)';
        ELSIF q.name = 'geometry' THEN
            out := 'CREATE INDEX ON %I USING gist (geometry)';
        ELSE
            out := format($q$CREATE INDEX ON %%I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
                lower(COALESCE(q.property_index_type, 'BTREE')),
                lower(COALESCE(q.property_wrapper, 'to_text')),
                q.name
            );
        END IF;
        RETURN btrim(out, ' \n\t');
    END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

DROP VIEW IF EXISTS pgstac_indexes;
CREATE VIEW pgstac_indexes AS
SELECT
    i.schemaname,
    i.tablename,
    i.indexname,
    regexp_replace(btrim(replace(replace(indexdef, i.indexname, ''),'pgstac.',''),' \t\n'), '[ ]+', ' ', 'g') as idx,
    COALESCE(
        (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
        (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'))[1],
        CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime' ELSE NULL END
    ) AS field,
    pg_table_size(i.indexname::text) as index_size,
    pg_size_pretty(pg_table_size(i.indexname::text)) as index_size_pretty
FROM
    pg_indexes i
WHERE i.schemaname='pgstac' and i.tablename ~ '_items_' AND indexdef !~* ' only ';

DROP VIEW IF EXISTS pgstac_index_stats;
CREATE VIEW pgstac_indexes_stats AS
SELECT
    i.schemaname,
    i.tablename,
    i.indexname,
    indexdef,
    COALESCE(
        (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
        (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_]+)''::text'))[1],
        CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime_end_datetime' ELSE NULL END
    ) AS field,
    pg_table_size(i.indexname::text) as index_size,
    pg_size_pretty(pg_table_size(i.indexname::text)) as index_size_pretty,
    n_distinct,
    most_common_vals::text::text[],
    most_common_freqs::text::text[],
    histogram_bounds::text::text[],
    correlation
FROM
    pg_indexes i
    LEFT JOIN pg_stats s ON (s.tablename = i.indexname)
WHERE i.schemaname='pgstac' and i.tablename ~ '_items_';

CREATE OR REPLACE FUNCTION queryable_indexes(
    IN treeroot text DEFAULT 'items',
    IN changes boolean DEFAULT FALSE,
    OUT collection text,
    OUT partition text,
    OUT field text,
    OUT indexname text,
    OUT existing_idx text,
    OUT queryable_idx text
) RETURNS SETOF RECORD AS $$
WITH p AS (
        SELECT
            relid::text as partition,
            replace(replace(
                CASE
                    WHEN parentrelid::regclass::text='items' THEN pg_get_expr(c.relpartbound, c.oid)
                    ELSE pg_get_expr(parent.relpartbound, parent.oid)
                END,
                'FOR VALUES IN (''',''), ''')',
                ''
            ) AS collection
        FROM pg_partition_tree(treeroot)
        JOIN pg_class c ON (relid::regclass = c.oid)
        JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    ), i AS (
        SELECT
            partition,
            indexname,
            regexp_replace(btrim(replace(replace(indexdef, indexname, ''),'pgstac.',''),' \t\n'), '[ ]+', ' ', 'g') as iidx,
            COALESCE(
                (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
                (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'))[1],
                CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime' ELSE NULL END
            ) AS field
        FROM
            pg_indexes
            JOIN p ON (tablename=partition)
    ), q AS (
        SELECT
            name AS field,
            collection,
            partition,
            format(indexdef(queryables), partition) as qidx
        FROM queryables, unnest_collection(queryables.collection_ids) collection
            JOIN p USING (collection)
        WHERE property_index_type IS NOT NULL OR name IN ('datetime','geometry','id')
    )
    SELECT
        collection,
        partition,
        field,
        indexname,
        iidx as existing_idx,
        qidx as queryable_idx
    FROM i FULL JOIN q USING (field, partition)
    WHERE CASE WHEN changes THEN lower(iidx) IS DISTINCT FROM lower(qidx) ELSE TRUE END;
;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION maintain_index(
    indexname text,
    queryable_idx text,
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE,
    idxconcurrently boolean DEFAULT FALSE
) RETURNS VOID AS $$
DECLARE
BEGIN
    IF indexname IS NOT NULL THEN
        IF dropindexes OR queryable_idx IS NOT NULL THEN
            EXECUTE format('DROP INDEX IF EXISTS %I;', indexname);
        ELSIF rebuildindexes THEN
            IF idxconcurrently THEN
                EXECUTE format('REINDEX INDEX CONCURRENTLY %I;', indexname);
            ELSE
                EXECUTE format('REINDEX INDEX CONCURRENTLY %I;', indexname);
            END IF;
        END IF;
    END IF;
    IF queryable_idx IS NOT NULL THEN
        IF idxconcurrently THEN
            EXECUTE replace(queryable_idx, 'INDEX', 'INDEX CONCURRENTLY');
        ELSE EXECUTE queryable_idx;
        END IF;
    END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;



set check_function_bodies to off;
CREATE OR REPLACE FUNCTION maintain_partition_queries(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE,
    idxconcurrently boolean DEFAULT FALSE
) RETURNS SETOF text AS $$
DECLARE
   rec record;
   q text;
BEGIN
    FOR rec IN (
        SELECT * FROM queryable_indexes(part,true)
    ) LOOP
        q := format(
            'SELECT maintain_index(
                %L,%L,%L,%L,%L
            );',
            rec.indexname,
            rec.queryable_idx,
            dropindexes,
            rebuildindexes,
            idxconcurrently
        );
        RAISE NOTICE 'Q: %s', q;
        RETURN NEXT q;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION maintain_partitions(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partition_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION queryables_trigger_func() RETURNS TRIGGER AS $$
DECLARE
BEGIN
    PERFORM maintain_partitions();
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER queryables_trigger AFTER INSERT OR UPDATE ON queryables
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();


CREATE OR REPLACE FUNCTION get_queryables(_collection_ids text[] DEFAULT NULL) RETURNS jsonb AS $$
DECLARE
BEGIN
    -- Build up queryables if the input contains valid collection ids or is empty
    IF EXISTS (
        SELECT 1 FROM collections
        WHERE
            _collection_ids IS NULL
            OR cardinality(_collection_ids) = 0
            OR id = ANY(_collection_ids)
    )
    THEN
        RETURN (
            WITH base AS (
                SELECT
                    unnest(collection_ids) as collection_id,
                    name,
                    coalesce(definition, '{"type":"string"}'::jsonb) as definition
                FROM queryables
                WHERE
                    _collection_ids IS NULL OR
                    _collection_ids = '{}'::text[] OR
                    _collection_ids && collection_ids
                UNION ALL
                SELECT null, name, coalesce(definition, '{"type":"string"}'::jsonb) as definition
                FROM queryables WHERE collection_ids IS NULL OR collection_ids = '{}'::text[]
            ), g AS (
                SELECT
                    name,
                    first_notnull(definition) as definition,
                    jsonb_array_unique_merge(definition->'enum') as enum,
                    jsonb_min(definition->'minimum') as minimum,
                    jsonb_min(definition->'maxiumn') as maximum
                FROM base
                GROUP BY 1
            )
            SELECT
                jsonb_build_object(
                    '$schema', 'http://json-schema.org/draft-07/schema#',
                    '$id', '',
                    'type', 'object',
                    'title', 'STAC Queryables.',
                    'properties', jsonb_object_agg(
                        name,
                        definition
                        ||
                        jsonb_strip_nulls(jsonb_build_object(
                            'enum', enum,
                            'minimum', minimum,
                            'maximum', maximum
                        ))
                    ),
                    'additionalProperties', pgstac.additional_properties()
                )
                FROM g
        );
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables(_collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT
        CASE
            WHEN _collection IS NULL THEN get_queryables(NULL::text[])
            ELSE get_queryables(ARRAY[_collection])
        END
    ;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_queryables() RETURNS jsonb AS $$
    SELECT get_queryables(NULL::text[]);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION schema_qualify_refs(url text, j jsonb) returns jsonb as $$
    SELECT regexp_replace(j::text, '"\$ref": "#', concat('"$ref": "', url, '#'), 'g')::jsonb;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE VIEW stac_extension_queryables AS
SELECT DISTINCT key as name, schema_qualify_refs(e.url, j.value) as definition FROM stac_extensions e, jsonb_each(e.content->'definitions'->'fields'->'properties') j;


CREATE OR REPLACE FUNCTION missing_queryables(_collection text, _tablesample float DEFAULT 5, minrows float DEFAULT 10) RETURNS TABLE(collection text, name text, definition jsonb, property_wrapper text) AS $$
DECLARE
    q text;
    _partition text;
    explain_json json;
    psize float;
    estrows float;
BEGIN
    SELECT format('_items_%s', key) INTO _partition FROM collections WHERE id=_collection;

    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM %I;', _partition)
    INTO explain_json;
    psize := explain_json->0->'Plan'->'Plan Rows';
    estrows := _tablesample * .01 * psize;
    IF estrows < minrows THEN
        _tablesample := least(100,greatest(_tablesample, (estrows / psize) / 100));
        RAISE NOTICE '%', (psize / estrows) / 100;
    END IF;
    RAISE NOTICE 'Using tablesample % to find missing queryables from % % that has ~% rows estrows: %', _tablesample, _collection, _partition, psize, estrows;

    q := format(
        $q$
            WITH q AS (
                SELECT * FROM queryables
                WHERE
                    collection_ids IS NULL
                    OR %L = ANY(collection_ids)
            ), t AS (
                SELECT
                    content->'properties' AS properties
                FROM
                    %I
                TABLESAMPLE SYSTEM(%L)
            ), p AS (
                SELECT DISTINCT ON (key)
                    key,
                    value,
                    s.definition
                FROM t
                JOIN LATERAL jsonb_each(properties) ON TRUE
                LEFT JOIN q ON (q.name=key)
                LEFT JOIN stac_extension_queryables s ON (s.name=key)
                WHERE q.definition IS NULL
            )
            SELECT
                %L,
                key,
                COALESCE(definition, jsonb_build_object('type',jsonb_typeof(value))) as definition,
                CASE
                    WHEN definition->>'type' = 'integer' THEN 'to_int'
                    WHEN COALESCE(definition->>'type', jsonb_typeof(value)) = 'number' THEN 'to_float'
                    WHEN COALESCE(definition->>'type', jsonb_typeof(value)) = 'array' THEN 'to_text_array'
                    ELSE 'to_text'
                END
            FROM p;
        $q$,
        _collection,
        _partition,
        _tablesample,
        _collection
    );
    RETURN QUERY EXECUTE q;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION missing_queryables(_tablesample float DEFAULT 5) RETURNS TABLE(collection_ids text[], name text, definition jsonb, property_wrapper text) AS $$
    SELECT
        array_agg(collection),
        name,
        definition,
        property_wrapper
    FROM
        collections
        JOIN LATERAL
        missing_queryables(id, _tablesample) c
        ON TRUE
    GROUP BY
        2,3,4
    ORDER BY 2,1
    ;
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate jsonb,
    relative_base timestamptz DEFAULT date_trunc('hour', CURRENT_TIMESTAMP)
) RETURNS tstzrange AS $$
DECLARE
    timestrs text[];
    s timestamptz;
    e timestamptz;
BEGIN
    timestrs :=
    CASE
        WHEN _indate ? 'timestamp' THEN
            ARRAY[_indate->>'timestamp']
        WHEN _indate ? 'interval' THEN
            to_text_array(_indate->'interval')
        WHEN jsonb_typeof(_indate) = 'array' THEN
            to_text_array(_indate)
        ELSE
            regexp_split_to_array(
                _indate->>0,
                '/'
            )
    END;
    RAISE NOTICE 'TIMESTRS %', timestrs;
    IF cardinality(timestrs) = 1 THEN
        IF timestrs[1] ILIKE 'P%' THEN
            RETURN tstzrange(relative_base - upper(timestrs[1])::interval, relative_base, '[)');
        END IF;
        s := timestrs[1]::timestamptz;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) != 2 THEN
        RAISE EXCEPTION 'Timestamp cannot have more than 2 values';
    END IF;

    IF timestrs[1] = '..' OR timestrs[1] = '' THEN
        s := '-infinity'::timestamptz;
        e := timestrs[2]::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] = '..' OR timestrs[2] = '' THEN
        s := timestrs[1]::timestamptz;
        e := 'infinity'::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[1] ILIKE 'P%' AND timestrs[2] NOT ILIKE 'P%' THEN
        e := timestrs[2]::timestamptz;
        s := e - upper(timestrs[1])::interval;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] ILIKE 'P%' AND timestrs[1] NOT ILIKE 'P%' THEN
        s := timestrs[1]::timestamptz;
        e := s + upper(timestrs[2])::interval;
        RETURN tstzrange(s,e,'[)');
    END IF;

    s := timestrs[1]::timestamptz;
    e := timestrs[2]::timestamptz;

    RETURN tstzrange(s,e,'[)');

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL STABLE STRICT PARALLEL SAFE SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate text,
    relative_base timestamptz DEFAULT CURRENT_TIMESTAMP
) RETURNS tstzrange AS $$
    SELECT parse_dtrange(to_jsonb(_indate), relative_base);
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION temporal_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    ll text := 'datetime';
    lh text := 'end_datetime';
    rrange tstzrange;
    rl text;
    rh text;
    outq text;
BEGIN
    rrange := parse_dtrange(args->1);
    RAISE NOTICE 'Constructing temporal query OP: %, ARGS: %, RRANGE: %', op, args, rrange;
    op := lower(op);
    rl := format('%L::timestamptz', lower(rrange));
    rh := format('%L::timestamptz', upper(rrange));
    outq := CASE op
        WHEN 't_before'       THEN 'lh < rl'
        WHEN 't_after'        THEN 'll > rh'
        WHEN 't_meets'        THEN 'lh = rl'
        WHEN 't_metby'        THEN 'll = rh'
        WHEN 't_overlaps'     THEN 'll < rl AND rl < lh < rh'
        WHEN 't_overlappedby' THEN 'rl < ll < rh AND lh > rh'
        WHEN 't_starts'       THEN 'll = rl AND lh < rh'
        WHEN 't_startedby'    THEN 'll = rl AND lh > rh'
        WHEN 't_during'       THEN 'll > rl AND lh < rh'
        WHEN 't_contains'     THEN 'll < rl AND lh > rh'
        WHEN 't_finishes'     THEN 'll > rl AND lh = rh'
        WHEN 't_finishedby'   THEN 'll < rl AND lh = rh'
        WHEN 't_equals'       THEN 'll = rl AND lh = rh'
        WHEN 't_disjoint'     THEN 'NOT (ll <= rh AND lh >= rl)'
        WHEN 't_intersects'   THEN 'll <= rh AND lh >= rl'
        WHEN 'anyinteracts'   THEN 'll <= rh AND lh >= rl'
    END;
    outq := regexp_replace(outq, '\mll\M', ll);
    outq := regexp_replace(outq, '\mlh\M', lh);
    outq := regexp_replace(outq, '\mrl\M', rl);
    outq := regexp_replace(outq, '\mrh\M', rh);
    outq := format('(%s)', outq);
    RETURN outq;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;



CREATE OR REPLACE FUNCTION spatial_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    geom text;
    j jsonb := args->1;
BEGIN
    op := lower(op);
    RAISE NOTICE 'Constructing spatial query OP: %, ARGS: %', op, args;
    IF op NOT IN ('s_equals','s_disjoint','s_touches','s_within','s_overlaps','s_crosses','s_intersects','intersects','s_contains') THEN
        RAISE EXCEPTION 'Spatial Operator % Not Supported', op;
    END IF;
    op := regexp_replace(op, '^s_', 'st_');
    IF op = 'intersects' THEN
        op := 'st_intersects';
    END IF;
    -- Convert geometry to WKB string
    IF j ? 'type' AND j ? 'coordinates' THEN
        geom := st_geomfromgeojson(j)::text;
    ELSIF jsonb_typeof(j) = 'array' THEN
        geom := bbox_geom(j)::text;
    END IF;

    RETURN format('%s(geometry, %L::geometry)', op, geom);
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION query_to_cql2(q jsonb) RETURNS jsonb AS $$
-- Translates anything passed in through the deprecated "query" into equivalent CQL2
WITH t AS (
    SELECT key as property, value as ops
        FROM jsonb_each(q)
), t2 AS (
    SELECT property, (jsonb_each(ops)).*
        FROM t WHERE jsonb_typeof(ops) = 'object'
    UNION ALL
    SELECT property, 'eq', ops
        FROM t WHERE jsonb_typeof(ops) != 'object'
)
SELECT
    jsonb_strip_nulls(jsonb_build_object(
        'op', 'and',
        'args', jsonb_agg(
            jsonb_build_object(
                'op', key,
                'args', jsonb_build_array(
                    jsonb_build_object('property',property),
                    value
                )
            )
        )
    )
) as qcql FROM t2
;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION cql1_to_cql2(j jsonb) RETURNS jsonb AS $$
DECLARE
    args jsonb;
    ret jsonb;
BEGIN
    RAISE NOTICE 'CQL1_TO_CQL2: %', j;
    IF j ? 'filter' THEN
        RETURN cql1_to_cql2(j->'filter');
    END IF;
    IF j ? 'property' THEN
        RETURN j;
    END IF;
    IF jsonb_typeof(j) = 'array' THEN
        SELECT jsonb_agg(cql1_to_cql2(el)) INTO args FROM jsonb_array_elements(j) el;
        RETURN args;
    END IF;
    IF jsonb_typeof(j) = 'number' THEN
        RETURN j;
    END IF;
    IF jsonb_typeof(j) = 'string' THEN
        RETURN j;
    END IF;

    IF jsonb_typeof(j) = 'object' THEN
        SELECT jsonb_build_object(
                'op', key,
                'args', cql1_to_cql2(value)
            ) INTO ret
        FROM jsonb_each(j)
        WHERE j IS NOT NULL;
        RETURN ret;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT;

CREATE TABLE cql2_ops (
    op text PRIMARY KEY,
    template text,
    types text[]
);
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
    ('lower', 'lower(%s)', NULL)
ON CONFLICT (op) DO UPDATE
    SET
        template = EXCLUDED.template
;


CREATE OR REPLACE FUNCTION cql2_query(j jsonb, wrapper text DEFAULT NULL) RETURNS text AS $$
#variable_conflict use_variable
DECLARE
    args jsonb := j->'args';
    arg jsonb;
    op text := lower(j->>'op');
    cql2op RECORD;
    literal text;
    _wrapper text;
    leftarg text;
    rightarg text;
    prop text;
    extra_props bool := pgstac.additional_properties();
BEGIN
    IF j IS NULL OR (op IS NOT NULL AND args IS NULL) THEN
        RETURN NULL;
    END IF;
    RAISE NOTICE 'CQL2_QUERY: %', j;

    -- check if all properties are represented in the queryables
    IF NOT extra_props THEN
        FOR prop IN
            SELECT DISTINCT p->>0
            FROM jsonb_path_query(j, 'strict $.**.property') p
            WHERE p->>0 NOT IN ('id', 'datetime', 'geometry', 'end_datetime', 'collection')
        LOOP
            IF (queryable(prop)).nulled_wrapper IS NULL THEN
                RAISE EXCEPTION 'Term % is not found in queryables.', prop;
            END IF;
        END LOOP;
    END IF;

    IF j ? 'filter' THEN
        RETURN cql2_query(j->'filter');
    END IF;

    IF j ? 'upper' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'upper', 'args', j->'upper'));
    END IF;

    IF j ? 'lower' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'lower', 'args', j->'lower'));
    END IF;

    -- Temporal Query
    IF op ilike 't_%' or op = 'anyinteracts' THEN
        RETURN temporal_op_query(op, args);
    END IF;

    -- If property is a timestamp convert it to text to use with
    -- general operators
    IF j ? 'timestamp' THEN
        RETURN format('%L::timestamptz', to_tstz(j->'timestamp'));
    END IF;
    IF j ? 'interval' THEN
        RAISE EXCEPTION 'Please use temporal operators when using intervals.';
        RETURN NONE;
    END IF;

    -- Spatial Query
    IF op ilike 's_%' or op = 'intersects' THEN
        RETURN spatial_op_query(op, args);
    END IF;

    IF op IN ('a_equals','a_contains','a_contained_by','a_overlaps') THEN
        IF args->0 ? 'property' THEN
            leftarg := format('to_text_array(%s)', (queryable(args->0->>'property')).path);
        END IF;
        IF args->1 ? 'property' THEN
            rightarg := format('to_text_array(%s)', (queryable(args->1->>'property')).path);
        END IF;
        RETURN FORMAT(
            '%s %s %s',
            COALESCE(leftarg, quote_literal(to_text_array(args->0))),
            CASE op
                WHEN 'a_equals' THEN '='
                WHEN 'a_contains' THEN '@>'
                WHEN 'a_contained_by' THEN '<@'
                WHEN 'a_overlaps' THEN '&&'
            END,
            COALESCE(rightarg, quote_literal(to_text_array(args->1)))
        );
    END IF;

    IF op = 'in' THEN
        RAISE NOTICE 'IN : % % %', args, jsonb_build_array(args->0), args->1;
        args := jsonb_build_array(args->0) || (args->1);
        RAISE NOTICE 'IN2 : %', args;
    END IF;



    IF op = 'between' THEN
        args = jsonb_build_array(
            args->0,
            args->1->0,
            args->1->1
        );
    END IF;

    -- Make sure that args is an array and run cql2_query on
    -- each element of the array
    RAISE NOTICE 'ARGS PRE: %', args;
    IF j ? 'args' THEN
        IF jsonb_typeof(args) != 'array' THEN
            args := jsonb_build_array(args);
        END IF;

        IF jsonb_path_exists(args, '$[*] ? (@.property == "id" || @.property == "datetime" || @.property == "end_datetime" || @.property == "collection")') THEN
            wrapper := NULL;
        ELSE
            -- if any of the arguments are a property, try to get the property_wrapper
            FOR arg IN SELECT jsonb_path_query(args, '$[*] ? (@.property != null)') LOOP
                RAISE NOTICE 'Arg: %', arg;
                wrapper := (queryable(arg->>'property')).nulled_wrapper;
                RAISE NOTICE 'Property: %, Wrapper: %', arg, wrapper;
                IF wrapper IS NOT NULL THEN
                    EXIT;
                END IF;
            END LOOP;

            -- if the property was not in queryables, see if any args were numbers
            IF
                wrapper IS NULL
                AND jsonb_path_exists(args, '$[*] ? (@.type()=="number")')
            THEN
                wrapper := 'to_float';
            END IF;
            wrapper := coalesce(wrapper, 'to_text');
        END IF;

        SELECT jsonb_agg(cql2_query(a, wrapper))
            INTO args
        FROM jsonb_array_elements(args) a;
    END IF;
    RAISE NOTICE 'ARGS: %', args;

    IF op IN ('and', 'or') THEN
        RETURN
            format(
                '(%s)',
                array_to_string(to_text_array(args), format(' %s ', upper(op)))
            );
    END IF;

    IF op = 'in' THEN
        RAISE NOTICE 'IN --  % %', args->0, to_text(args->0);
        RETURN format(
            '%s IN (%s)',
            to_text(args->0),
            array_to_string((to_text_array(args))[2:], ',')
        );
    END IF;

    -- Look up template from cql2_ops
    IF j ? 'op' THEN
        SELECT * INTO cql2op FROM cql2_ops WHERE  cql2_ops.op ilike op;
        IF FOUND THEN
            -- If specific index set in queryables for a property cast other arguments to that type

            RETURN format(
                cql2op.template,
                VARIADIC (to_text_array(args))
            );
        ELSE
            RAISE EXCEPTION 'Operator % Not Supported.', op;
        END IF;
    END IF;


    IF wrapper IS NOT NULL THEN
        RAISE NOTICE 'Wrapping % with %', j, wrapper;
        IF j ? 'property' THEN
            RETURN format('%I(%s)', wrapper, (queryable(j->>'property')).path);
        ELSE
            RETURN format('%I(%L)', wrapper, j);
        END IF;
    ELSIF j ? 'property' THEN
        RETURN quote_ident(j->>'property');
    END IF;

    RETURN quote_literal(to_text(j));
END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION paging_dtrange(
    j jsonb
) RETURNS tstzrange AS $$
DECLARE
    op text;
    filter jsonb := j->'filter';
    dtrange tstzrange := tstzrange('-infinity'::timestamptz,'infinity'::timestamptz);
    sdate timestamptz := '-infinity'::timestamptz;
    edate timestamptz := 'infinity'::timestamptz;
    jpitem jsonb;
BEGIN

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        sdate := lower(dtrange);
        edate := upper(dtrange);
    END IF;
    IF NOT (filter  @? '$.**.op ? (@ == "or" || @ == "not")') THEN
        FOR jpitem IN SELECT j FROM jsonb_path_query(filter,'strict $.** ? (@.args[*].property == "datetime")'::jsonpath) j LOOP
            op := lower(jpitem->>'op');
            dtrange := parse_dtrange(jpitem->'args'->1);
            IF op IN ('<=', 'lt', 'lte', '<', 'le', 't_before') THEN
                sdate := greatest(sdate,'-infinity');
                edate := least(edate, upper(dtrange));
            ELSIF op IN ('>=', '>', 'gt', 'gte', 'ge', 't_after') THEN
                edate := least(edate, 'infinity');
                sdate := greatest(sdate, lower(dtrange));
            ELSIF op IN ('=', 'eq') THEN
                edate := least(edate, upper(dtrange));
                sdate := greatest(sdate, lower(dtrange));
            END IF;
            RAISE NOTICE '2 OP: %, ARGS: %, DTRANGE: %, SDATE: %, EDATE: %', op, jpitem->'args'->1, dtrange, sdate, edate;
        END LOOP;
    END IF;
    IF sdate > edate THEN
        RETURN 'empty'::tstzrange;
    END IF;
    RETURN tstzrange(sdate,edate, '[]');
END;
$$ LANGUAGE PLPGSQL STABLE STRICT SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION paging_collections(
    IN j jsonb
) RETURNS text[] AS $$
DECLARE
    filter jsonb := j->'filter';
    jpitem jsonb;
    op text;
    args jsonb;
    arg jsonb;
    collections text[];
BEGIN
    IF j ? 'collections' THEN
        collections := to_text_array(j->'collections');
    END IF;
    IF NOT (filter  @? '$.**.op ? (@ == "or" || @ == "not")') THEN
        FOR jpitem IN SELECT j FROM jsonb_path_query(filter,'strict $.** ? (@.args[*].property == "collection")'::jsonpath) j LOOP
            RAISE NOTICE 'JPITEM: %', jpitem;
            op := jpitem->>'op';
            args := jpitem->'args';
            IF op IN ('=', 'eq', 'in') THEN
                FOR arg IN SELECT a FROM jsonb_array_elements(args) a LOOP
                    IF jsonb_typeof(arg) IN ('string', 'array') THEN
                        RAISE NOTICE 'arg: %, collections: %', arg, collections;
                        IF collections IS NULL OR collections = '{}'::text[] THEN
                            collections := to_text_array(arg);
                        ELSE
                            collections := array_intersection(collections, to_text_array(arg));
                        END IF;
                    END IF;
                END LOOP;
            END IF;
        END LOOP;
    END IF;
    IF collections = '{}'::text[] THEN
        RETURN NULL;
    END IF;
    RETURN collections;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;
CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    content JSONB NOT NULL,
    private jsonb
)
PARTITION BY LIST (collection)
;

CREATE INDEX "datetime_idx" ON items USING BTREE (datetime DESC, end_datetime ASC);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION partition_after_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p text;
    t timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Updating partition stats %', t;
    FOR p IN SELECT DISTINCT partition
        FROM newdata n JOIN partition_sys_meta p
        ON (n.collection=p.collection AND n.datetime <@ p.partition_dtrange)
    LOOP
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L, %L);', p, true));
    END LOOP;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        DELETE FROM format_item_cache c USING newdata n WHERE c.collection = n.collection AND c.id = n.id;
    END IF;
    RAISE NOTICE 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE TRIGGER items_after_insert_trigger
AFTER INSERT ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE TRIGGER items_after_update_trigger
AFTER DELETE ON items
REFERENCING OLD TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE TRIGGER items_after_delete_trigger
AFTER UPDATE ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();


CREATE OR REPLACE FUNCTION content_slim(_item jsonb) RETURNS jsonb AS $$
    SELECT strip_jsonb(_item - '{id,geometry,collection,type}'::text[], collection_base_item(_item->>'collection')) - '{id,geometry,collection,type}'::text[];
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
    SELECT
            content->>'id' as id,
            stac_geom(content) as geometry,
            content->>'collection' as collection,
            stac_datetime(content) as datetime,
            stac_end_datetime(content) as end_datetime,
            content_slim(content) as content,
            null::jsonb as private
    ;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION include_field(f text, fields jsonb DEFAULT '{}'::jsonb) RETURNS boolean AS $$
DECLARE
    includes jsonb := fields->'include';
    excludes jsonb := fields->'exclude';
BEGIN
    IF f IS NULL THEN
        RETURN NULL;
    END IF;


    IF
        jsonb_typeof(excludes) = 'array'
        AND jsonb_array_length(excludes)>0
        AND excludes ? f
    THEN
        RETURN FALSE;
    END IF;

    IF
        (
            jsonb_typeof(includes) = 'array'
            AND jsonb_array_length(includes) > 0
            AND includes ? f
        ) OR
        (
            includes IS NULL
            OR jsonb_typeof(includes) = 'null'
            OR jsonb_array_length(includes) = 0
        )
    THEN
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS content_hydrate(jsonb, jsonb, jsonb);
CREATE OR REPLACE FUNCTION content_hydrate(
    _item jsonb,
    _base_item jsonb,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
    SELECT merge_jsonb(
            jsonb_fields(_item, fields),
            jsonb_fields(_base_item, fields)
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION content_hydrate(_item items, _collection collections, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
    content jsonb;
    base_item jsonb := _collection.base_item;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := content_hydrate(
        jsonb_build_object(
            'id', _item.id,
            'geometry', geom,
            'collection', _item.collection,
            'type', 'Feature'
        ) || _item.content,
        _collection.base_item,
        fields
    );

    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := jsonb_build_object(
                'id', _item.id,
                'geometry', geom,
                'collection', _item.collection,
                'type', 'Feature'
            ) || _item.content;
    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_hydrate(_item items, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
    SELECT content_hydrate(
        _item,
        (SELECT c FROM collections c WHERE id=_item.collection LIMIT 1),
        fields
    );
$$ LANGUAGE SQL STABLE;


CREATE UNLOGGED TABLE items_staging (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_ignore (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_upsert (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p record;
    _partitions text[];
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
BEGIN
    RAISE NOTICE 'Creating Partitions. %', clock_timestamp() - ts;

    FOR part IN WITH t AS (
        SELECT
            n.content->>'collection' as collection,
            stac_daterange(n.content->'properties') as dtr,
            partition_trunc
        FROM newdata n JOIN collections ON (n.content->>'collection'=collections.id)
    ), p AS (
        SELECT
            collection,
            COALESCE(date_trunc(partition_trunc::text, lower(dtr)),'-infinity') as d,
            tstzrange(min(lower(dtr)),max(lower(dtr)),'[]') as dtrange,
            tstzrange(min(upper(dtr)),max(upper(dtr)),'[]') as edtrange
        FROM t
        GROUP BY 1,2
    ) SELECT check_partition(collection, dtrange, edtrange) FROM p LOOP
        RAISE NOTICE 'Partition %', part;
    END LOOP;

    RAISE NOTICE 'Creating temp table with data to be added. %', clock_timestamp() - ts;
    DROP TABLE IF EXISTS tmpdata;
    CREATE TEMP TABLE tmpdata ON COMMIT DROP AS
    SELECT
        (content_dehydrate(content)).*
    FROM newdata;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE 'Added % rows to tmpdata. %', nrows, clock_timestamp() - ts;

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT * FROM tmpdata;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        DELETE FROM items i USING tmpdata s
            WHERE
                i.id = s.id
                AND i.collection = s.collection
                AND i IS DISTINCT FROM s
        ;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;
        INSERT INTO items AS t
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    END IF;

    RAISE NOTICE 'Deleting data from staging table. %', clock_timestamp() - ts;
    DELETE FROM items_staging;
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER items_staging_insert_trigger AFTER INSERT ON items_staging REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_ignore_trigger AFTER INSERT ON items_staging_ignore REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_upsert_trigger AFTER INSERT ON items_staging_upsert REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();


CREATE OR REPLACE FUNCTION item_by_id(_id text, _collection text DEFAULT NULL) RETURNS items AS
$$
DECLARE
    i items%ROWTYPE;
BEGIN
    SELECT * INTO i FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection) LIMIT 1;
    RETURN i;
END;
$$ LANGUAGE PLPGSQL STABLE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION get_item(_id text, _collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT content_hydrate(items) FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection);
$$ LANGUAGE SQL STABLE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION delete_item(_id text, _collection text DEFAULT NULL) RETURNS VOID AS $$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id AND (_collection IS NULL OR collection=_collection) RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL;

--/*
CREATE OR REPLACE FUNCTION create_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION update_item(content jsonb) RETURNS VOID AS $$
DECLARE
    old items %ROWTYPE;
    out items%ROWTYPE;
BEGIN
    PERFORM delete_item(content->>'id', content->>'collection');
    PERFORM create_item(content);
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION collection_bbox(id text) RETURNS jsonb AS $$
    SELECT (replace(replace(replace(st_extent(geometry)::text,'BOX(','[['),')',']]'),' ',','))::jsonb
    FROM items WHERE collection=$1;
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_temporal_extent(id text) RETURNS jsonb AS $$
    SELECT to_jsonb(array[array[min(datetime)::text, max(datetime)::text]])
    FROM items WHERE collection=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections SET
    content = content ||
    jsonb_build_object(
        'extent', jsonb_build_object(
            'spatial', jsonb_build_object(
                'bbox', collection_bbox(collections.id)
            ),
            'temporal', jsonb_build_object(
                'interval', collection_temporal_extent(collections.id)
            )
        )
    )
;
$$ LANGUAGE SQL;
CREATE TABLE partition_stats (
    partition text PRIMARY KEY,
    dtrange tstzrange,
    edtrange tstzrange,
    spatial geometry,
    last_updated timestamptz,
    keys text[]
) WITH (FILLFACTOR=90);

CREATE INDEX partitions_range_idx ON partition_stats USING GIST(dtrange);


CREATE OR REPLACE FUNCTION constraint_tstzrange(expr text) RETURNS tstzrange AS $$
    WITH t AS (
        SELECT regexp_matches(
            expr,
            E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
        ) AS m
    ) SELECT tstzrange(m[1]::timestamptz, m[2]::timestamptz) FROM t
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION dt_constraint(coid oid, OUT dt tstzrange, OUT edt tstzrange) RETURNS RECORD AS $$
DECLARE
    expr text := pg_get_constraintdef(coid);
    matches timestamptz[];
BEGIN
    IF expr LIKE '%NULL%' THEN
        dt := tstzrange(null::timestamptz, null::timestamptz);
        edt := tstzrange(null::timestamptz, null::timestamptz);
        RETURN;
    END IF;
    WITH f AS (SELECT (regexp_matches(expr, E'([0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]\.?[0-9]*)', 'g'))[1] f)
    SELECT array_agg(f::timestamptz) INTO matches FROM f;
    IF cardinality(matches) = 4 THEN
        dt := tstzrange(matches[1], matches[2],'[]');
        edt := tstzrange(matches[3], matches[4], '[]');
        RETURN;
    ELSIF cardinality(matches) = 2 THEN
        edt := tstzrange(matches[1], matches[2],'[]');
        RETURN;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;

CREATE OR REPLACE VIEW partition_sys_meta AS
SELECT
    relid::text as partition,
    replace(replace(CASE WHEN level = 1 THEN pg_get_expr(c.relpartbound, c.oid)
        ELSE pg_get_expr(parent.relpartbound, parent.oid)
    END, 'FOR VALUES IN (''',''), ''')','') AS collection,
    level,
    c.reltuples,
    c.relhastriggers,
    COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity', 'infinity','[]')) as partition_dtrange,
    COALESCE((dt_constraint(edt.oid)).dt, constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity', 'infinity','[]')) as constraint_dtrange,
    COALESCE((dt_constraint(edt.oid)).edt, tstzrange('-infinity', 'infinity','[]')) as constraint_edtrange
FROM
    pg_partition_tree('items')
    JOIN pg_class c ON (relid::regclass = c.oid)
    JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    LEFT JOIN pg_constraint edt ON (conrelid=c.oid AND contype='c')
WHERE isleaf
;

CREATE VIEW partitions_view AS
SELECT
    relid::text as partition,
    replace(replace(CASE WHEN level = 1 THEN pg_get_expr(c.relpartbound, c.oid)
        ELSE pg_get_expr(parent.relpartbound, parent.oid)
    END, 'FOR VALUES IN (''',''), ''')','') AS collection,
    level,
    c.reltuples,
    c.relhastriggers,
    COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity', 'infinity','[]')) as partition_dtrange,
    COALESCE((dt_constraint(edt.oid)).dt, constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity', 'infinity','[]')) as constraint_dtrange,
    COALESCE((dt_constraint(edt.oid)).edt, tstzrange('-infinity', 'infinity','[]')) as constraint_edtrange,
    dtrange,
    edtrange,
    spatial,
    last_updated
FROM
    pg_partition_tree('items')
    JOIN pg_class c ON (relid::regclass = c.oid)
    JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    LEFT JOIN pg_constraint edt ON (conrelid=c.oid AND contype='c')
    LEFT JOIN partition_stats ON (relid::text=partition)
WHERE isleaf
;

CREATE MATERIALIZED VIEW partitions AS
SELECT * FROM partitions_view;
CREATE UNIQUE INDEX ON partitions (partition);

CREATE MATERIALIZED VIEW partition_steps AS
SELECT
    partition as name,
    date_trunc('month',lower(partition_dtrange)) as sdate,
    date_trunc('month', upper(partition_dtrange)) + '1 month'::interval as edate
    FROM partitions_view WHERE partition_dtrange IS NOT NULL AND partition_dtrange != 'empty'::tstzrange
    ORDER BY dtrange ASC
;


CREATE OR REPLACE FUNCTION update_partition_stats_q(_partition text, istrigger boolean default false) RETURNS VOID AS $$
DECLARE
BEGIN
    PERFORM run_or_queue(
        format('SELECT update_partition_stats(%L, %L);', _partition, istrigger)
    );
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION update_partition_stats(_partition text, istrigger boolean default false) RETURNS VOID AS $$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    cdtrange tstzrange;
    cedtrange tstzrange;
    extent geometry;
    collection text;
BEGIN
    RAISE NOTICE 'Updating stats for %.', _partition;
    EXECUTE format(
        $q$
            SELECT
                tstzrange(min(datetime), max(datetime),'[]'),
                tstzrange(min(end_datetime), max(end_datetime), '[]')
            FROM %I
        $q$,
        _partition
    ) INTO dtrange, edtrange;
    extent := st_estimatedextent('pgstac', _partition, 'geometry');
    INSERT INTO partition_stats (partition, dtrange, edtrange, spatial, last_updated)
        SELECT _partition, dtrange, edtrange, extent, now()
        ON CONFLICT (partition) DO
            UPDATE SET
                dtrange=EXCLUDED.dtrange,
                edtrange=EXCLUDED.edtrange,
                spatial=EXCLUDED.spatial,
                last_updated=EXCLUDED.last_updated
    ;

    SELECT
        constraint_dtrange, constraint_edtrange, pv.collection
        INTO cdtrange, cedtrange, collection
    FROM partitions_view pv WHERE partition = _partition;
    REFRESH MATERIALIZED VIEW partitions;
    REFRESH MATERIALIZED VIEW partition_steps;


    RAISE NOTICE 'Checking if we need to modify constraints...';
    RAISE NOTICE 'cdtrange: % dtrange: % cedtrange: % edtrange: %',cdtrange, dtrange, cedtrange, edtrange;
    IF
        (cdtrange IS DISTINCT FROM dtrange OR edtrange IS DISTINCT FROM cedtrange)
        AND NOT istrigger
    THEN
        RAISE NOTICE 'Modifying Constraints';
        RAISE NOTICE 'Existing % %', cdtrange, cedtrange;
        RAISE NOTICE 'New      % %', dtrange, edtrange;
        PERFORM drop_table_constraints(_partition);
        PERFORM create_table_constraints(_partition, dtrange, edtrange);
        REFRESH MATERIALIZED VIEW partitions;
        REFRESH MATERIALIZED VIEW partition_steps;
    END IF;
    RAISE NOTICE 'Checking if we need to update collection extents.';
    IF get_setting_bool('update_collection_extent') THEN
        RAISE NOTICE 'updating collection extent for %', collection;
        PERFORM run_or_queue(format($q$
            UPDATE collections
            SET content = jsonb_set_lax(
                content,
                '{extent}'::text[],
                collection_extent(%L, FALSE),
                true,
                'use_json_null'
            ) WHERE id=%L
            ;
        $q$, collection, collection));
    ELSE
        RAISE NOTICE 'Not updating collection extent for %', collection;
    END IF;

END;
$$ LANGUAGE PLPGSQL STRICT SECURITY DEFINER;


CREATE OR REPLACE FUNCTION partition_name( IN collection text, IN dt timestamptz, OUT partition_name text, OUT partition_range tstzrange) AS $$
DECLARE
    c RECORD;
    parent_name text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;
    parent_name := format('_items_%s', c.key);


    IF c.partition_trunc = 'year' THEN
        partition_name := format('%s_%s', parent_name, to_char(dt,'YYYY'));
    ELSIF c.partition_trunc = 'month' THEN
        partition_name := format('%s_%s', parent_name, to_char(dt,'YYYYMM'));
    ELSE
        partition_name := parent_name;
        partition_range := tstzrange('-infinity'::timestamptz, 'infinity'::timestamptz, '[]');
    END IF;
    IF partition_range IS NULL THEN
        partition_range := tstzrange(
            date_trunc(c.partition_trunc::text, dt),
            date_trunc(c.partition_trunc::text, dt) + concat('1 ', c.partition_trunc)::interval
        );
    END IF;
    RETURN;

END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION drop_table_constraints(t text) RETURNS text AS $$
DECLARE
    q text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM partitions_view WHERE partition=t) THEN
        RETURN NULL;
    END IF;
    FOR q IN SELECT FORMAT(
        $q$
            ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        $q$,
        t,
        conname
    ) FROM pg_constraint
        WHERE conrelid=t::regclass::oid AND contype='c'
    LOOP
        EXECUTE q;
    END LOOP;
    RETURN t;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION create_table_constraints(t text, _dtrange tstzrange, _edtrange tstzrange) RETURNS text AS $$
DECLARE
    q text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM partitions_view WHERE partition=t) THEN
        RETURN NULL;
    END IF;
    RAISE NOTICE 'Creating Table Constraints for % % %', t, _dtrange, _edtrange;
    IF _dtrange = 'empty' AND _edtrange = 'empty' THEN
        q :=format(
            $q$
                DO $block$
                BEGIN
                    ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
                    ALTER TABLE %I
                        ADD CONSTRAINT %I
                            CHECK (((datetime IS NULL) AND (end_datetime IS NULL))) NOT VALID
                    ;
                    ALTER TABLE %I
                        VALIDATE CONSTRAINT %I
                    ;



                EXCEPTION WHEN others THEN
                    RAISE WARNING '%%, Issue Altering Constraints. Please run update_partition_stats(%I)', SQLERRM USING ERRCODE = SQLSTATE;
                END;
                $block$;
            $q$,
            t,
            format('%s_dt', t),
            t,
            format('%s_dt', t),
            t,
            format('%s_dt', t),
            t
        );
    ELSE
        q :=format(
            $q$
                DO $block$
                BEGIN

                    ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
                    ALTER TABLE %I
                        ADD CONSTRAINT %I
                            CHECK (
                                (datetime >= %L)
                                AND (datetime <= %L)
                                AND (end_datetime >= %L)
                                AND (end_datetime <= %L)
                            ) NOT VALID
                    ;
                    ALTER TABLE %I
                        VALIDATE CONSTRAINT %I
                    ;



                EXCEPTION WHEN others THEN
                    RAISE WARNING '%%, Issue Altering Constraints. Please run update_partition_stats(%I)', SQLERRM USING ERRCODE = SQLSTATE;
                END;
                $block$;
            $q$,
            t,
            format('%s_dt', t),
            t,
            format('%s_dt', t),
            lower(_dtrange),
            upper(_dtrange),
            lower(_edtrange),
            upper(_edtrange),
            t,
            format('%s_dt', t),
            t
        );
    END IF;
    PERFORM run_or_queue(q);
    RETURN t;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION check_partition(
    _collection text,
    _dtrange tstzrange,
    _edtrange tstzrange
) RETURNS text AS $$
DECLARE
    c RECORD;
    pm RECORD;
    _partition_name text;
    _partition_dtrange tstzrange;
    _constraint_dtrange tstzrange;
    _constraint_edtrange tstzrange;
    q text;
    deferrable_q text;
    err_context text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;

    IF c.partition_trunc IS NOT NULL THEN
        _partition_dtrange := tstzrange(
            date_trunc(c.partition_trunc, lower(_dtrange)),
            date_trunc(c.partition_trunc, lower(_dtrange)) + (concat('1 ', c.partition_trunc))::interval,
            '[)'
        );
    ELSE
        _partition_dtrange :=  '[-infinity, infinity]'::tstzrange;
    END IF;

    IF NOT _partition_dtrange @> _dtrange THEN
        RAISE EXCEPTION 'dtrange % is greater than the partition size % for collection %', _dtrange, c.partition_trunc, _collection;
    END IF;


    IF c.partition_trunc = 'year' THEN
        _partition_name := format('_items_%s_%s', c.key, to_char(lower(_partition_dtrange),'YYYY'));
    ELSIF c.partition_trunc = 'month' THEN
        _partition_name := format('_items_%s_%s', c.key, to_char(lower(_partition_dtrange),'YYYYMM'));
    ELSE
        _partition_name := format('_items_%s', c.key);
    END IF;

    SELECT * INTO pm FROM partition_sys_meta WHERE collection=_collection AND partition_dtrange @> _dtrange;
    IF FOUND THEN
        RAISE NOTICE '% % %', _edtrange, _dtrange, pm;
        _constraint_edtrange :=
            tstzrange(
                least(
                    lower(_edtrange),
                    nullif(lower(pm.constraint_edtrange), '-infinity')
                ),
                greatest(
                    upper(_edtrange),
                    nullif(upper(pm.constraint_edtrange), 'infinity')
                ),
                '[]'
            );
        _constraint_dtrange :=
            tstzrange(
                least(
                    lower(_dtrange),
                    nullif(lower(pm.constraint_dtrange), '-infinity')
                ),
                greatest(
                    upper(_dtrange),
                    nullif(upper(pm.constraint_dtrange), 'infinity')
                ),
                '[]'
            );

        IF pm.constraint_edtrange @> _edtrange AND pm.constraint_dtrange @> _dtrange THEN
            RETURN pm.partition;
        ELSE
            PERFORM drop_table_constraints(_partition_name);
        END IF;
    ELSE
        _constraint_edtrange := _edtrange;
        _constraint_dtrange := _dtrange;
    END IF;
    RAISE NOTICE 'EXISTING CONSTRAINTS % %, NEW % %', pm.constraint_dtrange, pm.constraint_edtrange, _constraint_dtrange, _constraint_edtrange;
    RAISE NOTICE 'Creating partition % %', _partition_name, _partition_dtrange;
    IF c.partition_trunc IS NULL THEN
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                GRANT ALL ON %I to pgstac_ingest;
            $q$,
            _partition_name,
            _collection,
            concat(_partition_name,'_pk'),
            _partition_name,
            _partition_name
        );
    ELSE
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L) PARTITION BY RANGE (datetime);
                CREATE TABLE IF NOT EXISTS %I partition OF %I FOR VALUES FROM (%L) TO (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                GRANT ALL ON %I TO pgstac_ingest;
            $q$,
            format('_items_%s', c.key),
            _collection,
            _partition_name,
            format('_items_%s', c.key),
            lower(_partition_dtrange),
            upper(_partition_dtrange),
            format('%s_pk', _partition_name),
            _partition_name,
            _partition_name
        );
    END IF;

    BEGIN
        EXECUTE q;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'Partition % already exists.', _partition_name;
        WHEN others THEN
            GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
            RAISE INFO 'Error Name:%',SQLERRM;
            RAISE INFO 'Error State:%', SQLSTATE;
            RAISE INFO 'Error Context:%', err_context;
    END;
    PERFORM maintain_partitions(_partition_name);
    PERFORM update_partition_stats_q(_partition_name, true);
    REFRESH MATERIALIZED VIEW partitions;
    REFRESH MATERIALIZED VIEW partition_steps;
    RETURN _partition_name;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION repartition(_collection text, _partition_trunc text, triggered boolean DEFAULT FALSE) RETURNS text AS $$
DECLARE
    c RECORD;
    q text;
    from_trunc text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;
    IF triggered THEN
        RAISE NOTICE 'Converting % to % partitioning via Trigger', _collection, _partition_trunc;
    ELSE
        RAISE NOTICE 'Converting % from using % to % partitioning', _collection, c.partition_trunc, _partition_trunc;
        IF c.partition_trunc IS NOT DISTINCT FROM _partition_trunc THEN
            RAISE NOTICE 'Collection % already set to use partition by %', _collection, _partition_trunc;
            RETURN _collection;
        END IF;
    END IF;

    IF EXISTS (SELECT 1 FROM partitions_view WHERE collection=_collection LIMIT 1) THEN
        EXECUTE format(
            $q$
                CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
                DROP TABLE IF EXISTS %I CASCADE;
                WITH p AS (
                    SELECT
                        collection,
                        CASE
                            WHEN %L IS NULL THEN '-infinity'::timestamptz
                            ELSE date_trunc(%L, datetime)
                        END as d,
                        tstzrange(min(datetime),max(datetime),'[]') as dtrange,
                        tstzrange(min(end_datetime),max(end_datetime),'[]') as edtrange
                    FROM changepartitionstaging
                    GROUP BY 1,2
                ) SELECT check_partition(collection, dtrange, edtrange) FROM p;
                INSERT INTO items SELECT * FROM changepartitionstaging;
                DROP TABLE changepartitionstaging;
            $q$,
            concat('_items_', c.key),
            concat('_items_', c.key),
            c.partition_trunc,
            c.partition_trunc
        );
    END IF;
    RETURN _collection;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION collections_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    q text;
    partition_name text := format('_items_%s', NEW.key);
    partition_exists boolean := false;
    partition_empty boolean := true;
    err_context text;
    loadtemp boolean := FALSE;
BEGIN
    RAISE NOTICE 'Collection Trigger. % %', NEW.id, NEW.key;
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS DISTINCT FROM OLD.partition_trunc THEN
        PERFORM repartition(NEW.id, NEW.partition_trunc, TRUE);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER collections_trigger AFTER
INSERT
OR
UPDATE ON collections
FOR EACH ROW EXECUTE FUNCTION collections_trigger_func();

CREATE OR REPLACE FUNCTION chunker(
    IN _where text,
    OUT s timestamptz,
    OUT e timestamptz
) RETURNS SETOF RECORD AS $$
DECLARE
    explain jsonb;
BEGIN
    IF _where IS NULL THEN
        _where := ' TRUE ';
    END IF;
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s;', _where)
    INTO explain;

    RETURN QUERY
    WITH t AS (
        SELECT j->>0 as p FROM
            jsonb_path_query(
                explain,
                'strict $.**."Relation Name" ? (@ != null)'
            ) j
    ),
    parts AS (
        SELECT sdate, edate FROM t JOIN partition_steps ON (t.p = name)
    ),
    times AS (
        SELECT sdate FROM parts
        UNION
        SELECT edate FROM parts
    ),
    uniq AS (
        SELECT DISTINCT sdate FROM times ORDER BY sdate
    ),
    last AS (
    SELECT sdate, lead(sdate, 1) over () as edate FROM uniq
    )
    SELECT sdate, edate FROM last WHERE edate IS NOT NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN partitions text[] DEFAULT NULL
) RETURNS SETOF text AS $$
DECLARE
    query text;
    sdate timestamptz;
    edate timestamptz;
BEGIN
IF _where IS NULL OR trim(_where) = '' THEN
    _where = ' TRUE ';
END IF;
RAISE NOTICE 'Getting chunks for % %', _where, _orderby;
IF _orderby ILIKE 'datetime d%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 DESC LOOP
        RETURN NEXT format($q$
            SELECT * FROM items
            WHERE
            datetime >= %L AND datetime < %L
            AND (%s)
            ORDER BY %s
            $q$,
            sdate,
            edate,
            _where,
            _orderby
        );
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RETURN NEXT format($q$
            SELECT * FROM items
            WHERE
            datetime >= %L AND datetime < %L
            AND (%s)
            ORDER BY %s
            $q$,
            sdate,
            edate,
            _where,
            _orderby
        );
    END LOOP;
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
    $q$, _where, _orderby
    );

    RETURN NEXT query;
    RETURN;
END IF;

RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION partition_query_view(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 10
) RETURNS text AS $$
    WITH p AS (
        SELECT * FROM partition_queries(_where, _orderby) p
    )
    SELECT
        CASE WHEN EXISTS (SELECT 1 FROM p) THEN
            (SELECT format($q$
                SELECT * FROM (
                    %s
                ) total LIMIT %s
                $q$,
                string_agg(
                    format($q$ SELECT * FROM ( %s ) AS sub $q$, p),
                    '
                    UNION ALL
                    '
                ),
                _limit
            ))
        ELSE NULL
        END FROM p;
$$ LANGUAGE SQL IMMUTABLE;




CREATE OR REPLACE FUNCTION stac_search_to_where(j jsonb) RETURNS text AS $$
DECLARE
    where_segments text[];
    _where text;
    dtrange tstzrange;
    collections text[];
    geom geometry;
    sdate timestamptz;
    edate timestamptz;
    filterlang text;
    filter jsonb := j->'filter';
BEGIN
    IF j ? 'ids' THEN
        where_segments := where_segments || format('id = ANY (%L) ', to_text_array(j->'ids'));
    END IF;

    IF j ? 'collections' THEN
        collections := to_text_array(j->'collections');
        where_segments := where_segments || format('collection = ANY (%L) ', collections);
    END IF;

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        sdate := lower(dtrange);
        edate := upper(dtrange);

        where_segments := where_segments || format(' datetime <= %L::timestamptz AND end_datetime >= %L::timestamptz ',
            edate,
            sdate
        );
    END IF;

    geom := stac_geom(j);
    IF geom IS NOT NULL THEN
        where_segments := where_segments || format('st_intersects(geometry, %L)',geom);
    END IF;

    filterlang := COALESCE(
        j->>'filter-lang',
        get_setting('default_filter_lang', j->'conf')
    );
    IF NOT filter @? '$.**.op' THEN
        filterlang := 'cql-json';
    END IF;

    IF filterlang NOT IN ('cql-json','cql2-json') AND j ? 'filter' THEN
        RAISE EXCEPTION '% is not a supported filter-lang. Please use cql-json or cql2-json.', filterlang;
    END IF;

    IF j ? 'query' AND j ? 'filter' THEN
        RAISE EXCEPTION 'Can only use either query or filter at one time.';
    END IF;

    IF j ? 'query' THEN
        filter := query_to_cql2(j->'query');
    ELSIF filterlang = 'cql-json' THEN
        filter := cql1_to_cql2(filter);
    END IF;
    RAISE NOTICE 'FILTER: %', filter;
    where_segments := where_segments || cql2_query(filter);
    IF cardinality(where_segments) < 1 THEN
        RETURN ' TRUE ';
    END IF;

    _where := array_to_string(array_remove(where_segments, NULL), ' AND ');

    IF _where IS NULL OR BTRIM(_where) = '' THEN
        RETURN ' TRUE ';
    END IF;
    RETURN _where;

END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION parse_sort_dir(_dir text, reverse boolean default false) RETURNS text AS $$
    WITH t AS (
        SELECT COALESCE(upper(_dir), 'ASC') as d
    ) SELECT
        CASE
            WHEN NOT reverse THEN d
            WHEN d = 'ASC' THEN 'DESC'
            WHEN d = 'DESC' THEN 'ASC'
        END
    FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sort_dir_to_op(_dir text, prev boolean default false) RETURNS text AS $$
    WITH t AS (
        SELECT COALESCE(upper(_dir), 'ASC') as d
    ) SELECT
        CASE
            WHEN d = 'ASC' AND prev THEN '<='
            WHEN d = 'DESC' AND prev THEN '>='
            WHEN d = 'ASC' THEN '>='
            WHEN d = 'DESC' THEN '<='
        END
    FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE
) RETURNS text AS $$
    WITH sortby AS (
        SELECT coalesce(_search->'sortby','[{"field":"datetime", "direction":"desc"}]') as sort
    ), withid AS (
        SELECT CASE
            WHEN sort @? '$[*] ? (@.field == "id")' THEN sort
            ELSE sort || '[{"field":"id", "direction":"desc"}]'::jsonb
            END as sort
        FROM sortby
    ), withid_rows AS (
        SELECT jsonb_array_elements(sort) as value FROM withid
    ),sorts AS (
        SELECT
            coalesce(
                (queryable(value->>'field')).expression
            ) as key,
            parse_sort_dir(value->>'direction', reverse) as dir
        FROM withid_rows
    )
    SELECT array_to_string(
        array_agg(concat(key, ' ', dir)),
        ', '
    ) FROM sorts;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_sort_dir(sort_item jsonb) RETURNS text AS $$
    SELECT CASE WHEN sort_item->>'direction' ILIKE 'desc%' THEN 'DESC' ELSE 'ASC' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION  get_token_val_str(
    _field text,
    _item items
) RETURNS text AS $$
DECLARE
    q text;
    literal text;
BEGIN
    q := format($q$ SELECT quote_literal(%s) FROM (SELECT $1.*) as r;$q$, _field);
    EXECUTE q INTO literal USING _item;
    RETURN literal;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION get_token_record(IN _token text, OUT prev BOOLEAN, OUT item items) RETURNS RECORD AS $$
DECLARE
    _itemid text := _token;
    _collectionid text;
BEGIN
    IF _token IS NULL THEN
        RETURN;
    END IF;
    RAISE NOTICE 'Looking for token: %', _token;
    prev := FALSE;
    IF _token ILIKE 'prev:%' THEN
        _itemid := replace(_token, 'prev:','');
        prev := TRUE;
    ELSIF _token ILIKE 'next:%' THEN
        _itemid := replace(_token, 'next:', '');
    END IF;
    SELECT id INTO _collectionid FROM collections WHERE _itemid LIKE concat(id,':%');
    IF FOUND THEN
        _itemid := replace(_itemid, concat(_collectionid,':'), '');
        SELECT * INTO item FROM items WHERE id=_itemid AND collection=_collectionid;
    ELSE
        SELECT * INTO item FROM items WHERE id=_itemid;
    END IF;
    IF item IS NULL THEN
        RAISE EXCEPTION 'Could not find item using token: % item: % collection: %', _token, _itemid, _collectionid;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;


CREATE OR REPLACE FUNCTION get_token_filter(
    _sortby jsonb DEFAULT '[{"field":"datetime","direction":"desc"}]'::jsonb,
    token_item items DEFAULT NULL,
    prev boolean DEFAULT FALSE,
    inclusive boolean DEFAULT FALSE
) RETURNS text AS $$
DECLARE
    ltop text := '<';
    gtop text := '>';
    dir text;
    sort record;
    orfilter text := '';
    orfilters text[] := '{}'::text[];
    andfilters text[] := '{}'::text[];
    output text;
    token_where text;
BEGIN
    IF _sortby IS NULL OR _sortby = '[]'::jsonb THEN
        _sortby := '[{"field":"datetime","direction":"desc"}]'::jsonb;
    END IF;
    _sortby := _sortby || jsonb_build_object('field','id','direction',_sortby->0->>'direction');
    RAISE NOTICE 'Getting Token Filter. % %', _sortby, token_item;
    IF inclusive THEN
        orfilters := orfilters || format('( id=%L AND collection=%L )' , token_item.id, token_item.collection);
    END IF;

    FOR sort IN
        WITH s1 AS (
            SELECT
                _row,
                (queryable(value->>'field')).expression as _field,
                (value->>'field' = 'id') as _isid,
                get_sort_dir(value) as _dir
            FROM jsonb_array_elements(_sortby)
            WITH ORDINALITY AS t(value, _row)
        )
        SELECT
            _row,
            _field,
            _dir,
            get_token_val_str(_field, token_item) as _val
        FROM s1
        WHERE _row <= (SELECT min(_row) FROM s1 WHERE _isid)
    LOOP
        orfilter := NULL;
        RAISE NOTICE 'SORT: %', sort;
        IF sort._val IS NOT NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            orfilter := format($f$(
                (%s %s %s) OR (%s IS NULL)
            )$f$,
            sort._field,
            ltop,
            sort._val,
            sort._val
            );
        ELSIF sort._val IS NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            RAISE NOTICE '< but null';
            orfilter := format('%s IS NOT NULL', sort._field);
        ELSIF sort._val IS NULL THEN
            RAISE NOTICE '> but null';
        ELSE
            orfilter := format($f$(
                (%s %s %s) OR (%s IS NULL)
            )$f$,
            sort._field,
            gtop,
            sort._val,
            sort._field
            );
        END IF;
        RAISE NOTICE 'ORFILTER: %', orfilter;

        IF orfilter IS NOT NULL THEN
            IF sort._row = 1 THEN
                orfilters := orfilters || orfilter;
            ELSE
                orfilters := orfilters || format('(%s AND %s)', array_to_string(andfilters, ' AND '), orfilter);
            END IF;
        END IF;
        IF sort._val IS NOT NULL THEN
            andfilters := andfilters || format('%s = %s', sort._field, sort._val);
        ELSE
            andfilters := andfilters || format('%s IS NULL', sort._field);
        END IF;
    END LOOP;

    output := array_to_string(orfilters, ' OR ');

    token_where := concat('(',coalesce(output,'true'),')');
    IF trim(token_where) = '' THEN
        token_where := NULL;
    END IF;
    RAISE NOTICE 'TOKEN_WHERE: %',token_where;
    RETURN token_where;
    END;
$$ LANGUAGE PLPGSQL SET transform_null_equals TO TRUE
;

CREATE OR REPLACE FUNCTION search_tohash(jsonb) RETURNS jsonb AS $$
    SELECT $1 - '{token,limit,context,includes,excludes}'::text[];
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search_hash(jsonb, jsonb) RETURNS text AS $$
    SELECT md5(concat(search_tohash($1)::text,$2::text));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TABLE IF NOT EXISTS searches(
    hash text GENERATED ALWAYS AS (search_hash(search, metadata)) STORED PRIMARY KEY,
    search jsonb NOT NULL,
    _where text,
    orderby text,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS search_wheres(
    id bigint generated always as identity primary key,
    _where text NOT NULL,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    statslastupdated timestamptz,
    estimated_count bigint,
    estimated_cost float,
    time_to_estimate float,
    total_count bigint,
    time_to_count float,
    partitions text[]
);

CREATE INDEX IF NOT EXISTS search_wheres_partitions ON search_wheres USING GIN (partitions);
CREATE UNIQUE INDEX IF NOT EXISTS search_wheres_where ON search_wheres ((md5(_where)));

CREATE OR REPLACE FUNCTION where_stats(inwhere text, updatestats boolean default false, conf jsonb default null) RETURNS search_wheres AS $$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    partitions text[];
    sw search_wheres%ROWTYPE;
    inwhere_hash text := md5(inwhere);
    _context text := lower(context(conf));
    _stats_ttl interval := context_stats_ttl(conf);
    _estimated_cost float := context_estimated_cost(conf);
    _estimated_count int := context_estimated_count(conf);
    ro bool := pgstac.readonly(conf);
BEGIN
    IF ro THEN
        updatestats := FALSE;
    END IF;

    IF _context = 'off' THEN
        sw._where := inwhere;
        return sw;
    END IF;

    SELECT * INTO sw FROM search_wheres WHERE md5(_where)=inwhere_hash FOR UPDATE;

    -- Update statistics if explicitly set, if statistics do not exist, or statistics ttl has expired
    IF NOT updatestats THEN
        RAISE NOTICE 'Checking if update is needed for: % .', inwhere;
        RAISE NOTICE 'Stats Last Updated: %', sw.statslastupdated;
        RAISE NOTICE 'TTL: %, Age: %', _stats_ttl, now() - sw.statslastupdated;
        RAISE NOTICE 'Context: %, Existing Total: %', _context, sw.total_count;
        IF
            (
                sw.statslastupdated IS NULL
                OR (now() - sw.statslastupdated) > _stats_ttl
                OR (context(conf) != 'off' AND sw.total_count IS NULL)
            ) AND NOT ro
        THEN
            updatestats := TRUE;
        END IF;
    END IF;

    sw._where := inwhere;
    sw.lastused := now();
    sw.usecount := coalesce(sw.usecount,0) + 1;

    IF NOT updatestats THEN
        UPDATE search_wheres SET
            lastused = sw.lastused,
            usecount = sw.usecount
        WHERE md5(_where) = inwhere_hash
        RETURNING * INTO sw
        ;
        RETURN sw;
    END IF;

    -- Use explain to get estimated count/cost and a list of the partitions that would be hit by the query
    t := clock_timestamp();
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
    INTO explain_json;
    RAISE NOTICE 'Time for just the explain: %', clock_timestamp() - t;
    i := clock_timestamp() - t;

    sw.statslastupdated := now();
    sw.estimated_count := explain_json->0->'Plan'->'Plan Rows';
    sw.estimated_cost := explain_json->0->'Plan'->'Total Cost';
    sw.time_to_estimate := extract(epoch from i);

    RAISE NOTICE 'ESTIMATED_COUNT: % < %', sw.estimated_count, _estimated_count;
    RAISE NOTICE 'ESTIMATED_COST: % < %', sw.estimated_cost, _estimated_cost;

    -- Do a full count of rows if context is set to on or if auto is set and estimates are low enough
    IF
        _context = 'on'
        OR
        ( _context = 'auto' AND
            (
                sw.estimated_count < _estimated_count
                AND
                sw.estimated_cost < _estimated_cost
            )
        )
    THEN
        t := clock_timestamp();
        RAISE NOTICE 'Calculating actual count...';
        EXECUTE format(
            'SELECT count(*) FROM items WHERE %s',
            inwhere
        ) INTO sw.total_count;
        i := clock_timestamp() - t;
        RAISE NOTICE 'Actual Count: % -- %', sw.total_count, i;
        sw.time_to_count := extract(epoch FROM i);
    ELSE
        sw.total_count := NULL;
        sw.time_to_count := NULL;
    END IF;

    IF NOT ro THEN
        INSERT INTO search_wheres
            (_where, lastused, usecount, statslastupdated, estimated_count, estimated_cost, time_to_estimate, partitions, total_count, time_to_count)
        SELECT sw._where, sw.lastused, sw.usecount, sw.statslastupdated, sw.estimated_count, sw.estimated_cost, sw.time_to_estimate, sw.partitions, sw.total_count, sw.time_to_count
        ON CONFLICT ((md5(_where)))
        DO UPDATE
            SET
                lastused = sw.lastused,
                usecount = sw.usecount,
                statslastupdated = sw.statslastupdated,
                estimated_count = sw.estimated_count,
                estimated_cost = sw.estimated_cost,
                time_to_estimate = sw.time_to_estimate,
                total_count = sw.total_count,
                time_to_count = sw.time_to_count
        ;
    END IF;
    RETURN sw;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION search_query(
    _search jsonb = '{}'::jsonb,
    updatestats boolean = false,
    _metadata jsonb = '{}'::jsonb
) RETURNS searches AS $$
DECLARE
    search searches%ROWTYPE;
    pexplain jsonb;
    t timestamptz;
    i interval;
    _hash text := search_hash(_search, _metadata);
    doupdate boolean := FALSE;
    insertfound boolean := FALSE;
    ro boolean := pgstac.readonly();
BEGIN
    IF ro THEN
        updatestats := FALSE;
    END IF;

    SELECT * INTO search FROM searches
    WHERE hash=_hash;

    search.hash := _hash;

    -- Calculate the where clause if not already calculated
    IF search._where IS NULL THEN
        search._where := stac_search_to_where(_search);
    ELSE
        doupdate := TRUE;
    END IF;

    -- Calculate the order by clause if not already calculated
    IF search.orderby IS NULL THEN
        search.orderby := sort_sqlorderby(_search);
    ELSE
        doupdate := TRUE;
    END IF;

    PERFORM where_stats(search._where, updatestats, _search->'conf');

    IF NOT ro THEN
        IF NOT doupdate THEN
            INSERT INTO searches (search, _where, orderby, lastused, usecount, metadata)
            VALUES (_search, search._where, search.orderby, clock_timestamp(), 1, _metadata)
            ON CONFLICT (hash) DO NOTHING RETURNING * INTO search;
            IF FOUND THEN
                RETURN search;
            END IF;
        END IF;

        UPDATE searches
            SET
                lastused=clock_timestamp(),
                usecount=usecount+1
        WHERE hash=(
            SELECT hash FROM searches
            WHERE hash=_hash
            FOR UPDATE SKIP LOCKED
        );
        IF NOT FOUND THEN
            RAISE NOTICE 'Did not update stats for % due to lock. (This is generally OK)', _search;
        END IF;
    END IF;

    RETURN search;

END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION search_rows(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN partitions text[] DEFAULT NULL,
    IN _limit int DEFAULT 10
) RETURNS SETOF items AS $$
DECLARE
    base_query text;
    query text;
    sdate timestamptz;
    edate timestamptz;
    n int;
    records_left int := _limit;
    timer timestamptz := clock_timestamp();
    full_timer timestamptz := clock_timestamp();
BEGIN
IF _where IS NULL OR trim(_where) = '' THEN
    _where = ' TRUE ';
END IF;
RAISE NOTICE 'Getting chunks for % %', _where, _orderby;

base_query := $q$
    SELECT * FROM items
    WHERE
    datetime >= %L AND datetime < %L
    AND (%s)
    ORDER BY %s
    LIMIT %L
$q$;

IF _orderby ILIKE 'datetime d%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 DESC LOOP
        RAISE NOTICE 'Running Query for % to %. %', sdate, edate, age_ms(full_timer);
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE LOG 'QUERY: %', query;
        timer := clock_timestamp();
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE NOTICE 'Returned %/% Rows From % to %. % to go. Time: %ms', n, _limit, sdate, edate, records_left, age_ms(timer);
        timer := clock_timestamp();
        IF records_left <= 0 THEN
            RAISE NOTICE 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
            RETURN;
        END IF;
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RAISE NOTICE 'Running Query for % to %. %', sdate, edate, age_ms(full_timer);
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE LOG 'QUERY: %', query;
        timer := clock_timestamp();
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE NOTICE 'Returned %/% Rows From % to %. % to go. Time: %ms', n, _limit, sdate, edate, records_left, age_ms(timer);
        timer := clock_timestamp();
        IF records_left <= 0 THEN
            RAISE NOTICE 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
            RETURN;
        END IF;
    END LOOP;
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
        LIMIT %L
    $q$, _where, _orderby, _limit
    );
    RAISE LOG 'QUERY: %', query;
    timer := clock_timestamp();
    RETURN QUERY EXECUTE query;
    RAISE NOTICE 'FULL QUERY TOOK %ms', age_ms(timer);
END IF;
RAISE NOTICE 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE UNLOGGED TABLE format_item_cache(
    id text,
    collection text,
    fields text,
    hydrated bool,
    output jsonb,
    lastused timestamptz DEFAULT now(),
    usecount int DEFAULT 1,
    timetoformat float,
    PRIMARY KEY (collection, id, fields, hydrated)
);
CREATE INDEX ON format_item_cache (lastused);

CREATE OR REPLACE FUNCTION format_item(_item items, _fields jsonb DEFAULT '{}', _hydrated bool DEFAULT TRUE) RETURNS jsonb AS $$
DECLARE
    cache bool := get_setting_bool('format_cache');
    _output jsonb := null;
    t timestamptz := clock_timestamp();
BEGIN
    IF cache THEN
        SELECT output INTO _output FROM format_item_cache
        WHERE id=_item.id AND collection=_item.collection AND fields=_fields::text AND hydrated=_hydrated;
    END IF;
    IF _output IS NULL THEN
        IF _hydrated THEN
            _output := content_hydrate(_item, _fields);
        ELSE
            _output := content_nonhydrated(_item, _fields);
        END IF;
    END IF;
    IF cache THEN
        INSERT INTO format_item_cache (id, collection, fields, hydrated, output, timetoformat)
            VALUES (_item.id, _item.collection, _fields::text, _hydrated, _output, age_ms(t))
            ON CONFLICT(collection, id, fields, hydrated) DO
                UPDATE
                    SET lastused=now(), usecount = format_item_cache.usecount + 1
        ;
    END IF;
    RETURN _output;

END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    searches searches%ROWTYPE;
    _where text;
    orderby text;
    search_where search_wheres%ROWTYPE;
    total_count bigint;
    token record;
    token_prev boolean;
    token_item items%ROWTYPE;
    token_where text;
    full_where text;
    init_ts timestamptz := clock_timestamp();
    timer timestamptz := clock_timestamp();
    hydrate bool := NOT (_search->'conf'->>'nohydrate' IS NOT NULL AND (_search->'conf'->>'nohydrate')::boolean = true);
    prev text;
    next text;
    context jsonb;
    collection jsonb;
    out_records jsonb;
    out_len int;
    _limit int := coalesce((_search->>'limit')::int, 10);
    _querylimit int;
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    has_prev boolean := FALSE;
    has_next boolean := FALSE;
BEGIN
    searches := search_query(_search);
    _where := searches._where;
    orderby := searches.orderby;
    search_where := where_stats(_where);
    total_count := coalesce(search_where.total_count, search_where.estimated_count);
    RAISE NOTICE 'SEARCH:TOKEN: %', _search->>'token';
    token := get_token_record(_search->>'token');
    RAISE NOTICE '***TOKEN: %', token;
    _querylimit := _limit + 1;
    IF token IS NOT NULL THEN
        token_prev := token.prev;
        token_item := token.item;
        token_where := get_token_filter(_search->'sortby', token_item, token_prev, FALSE);
        RAISE LOG 'TOKEN_WHERE: % (%ms from search start)', token_where, age_ms(timer);
        IF token_prev THEN -- if we are using a prev token, we know has_next is true
            RAISE LOG 'There is a previous token, so automatically setting has_next to true';
            has_next := TRUE;
            orderby := sort_sqlorderby(_search, TRUE);
        ELSE
            RAISE LOG 'There is a next token, so automatically setting has_prev to true';
            has_prev := TRUE;

        END IF;
    ELSE -- if there was no token, we know there is no prev
        RAISE LOG 'There is no token, so we know there is no prev. setting has_prev to false';
        has_prev := FALSE;
    END IF;

    full_where := concat_ws(' AND ', _where, token_where);
    RAISE NOTICE 'FULL WHERE CLAUSE: %', full_where;
    RAISE NOTICE 'Time to get counts and build query %', age_ms(timer);
    timer := clock_timestamp();

    IF hydrate THEN
        RAISE NOTICE 'Getting hydrated data.';
    ELSE
        RAISE NOTICE 'Getting non-hydrated data.';
    END IF;
    RAISE NOTICE 'CACHE SET TO %', get_setting_bool('format_cache');
    RAISE NOTICE 'Time to set hydration/formatting %', age_ms(timer);
    timer := clock_timestamp();
    SELECT jsonb_agg(format_item(i, _fields, hydrate)) INTO out_records
    FROM search_rows(
        full_where,
        orderby,
        search_where.partitions,
        _querylimit
    ) as i;

    RAISE NOTICE 'Time to fetch rows %', age_ms(timer);
    timer := clock_timestamp();


    IF token_prev THEN
        out_records := flip_jsonb_array(out_records);
    END IF;

    RAISE NOTICE 'Query returned % records.', jsonb_array_length(out_records);
    RAISE LOG 'TOKEN:   % %', token_item.id, token_item.collection;
    RAISE LOG 'RECORD_1: % %', out_records->0->>'id', out_records->0->>'collection';
    RAISE LOG 'RECORD-1: % %', out_records->-1->>'id', out_records->-1->>'collection';

    -- REMOVE records that were from our token
    IF out_records->0->>'id' = token_item.id AND out_records->0->>'collection' = token_item.collection THEN
        out_records := out_records - 0;
    ELSIF out_records->-1->>'id' = token_item.id AND out_records->-1->>'collection' = token_item.collection THEN
        out_records := out_records - -1;
    END IF;

    out_len := jsonb_array_length(out_records);

    IF out_len = _limit + 1 THEN
        IF token_prev THEN
            has_prev := TRUE;
            out_records := out_records - 0;
        ELSE
            has_next := TRUE;
            out_records := out_records - -1;
        END IF;
    END IF;

    IF has_next THEN
        next := concat(out_records->-1->>'collection', ':', out_records->-1->>'id');
        RAISE NOTICE 'HAS NEXT | %', next;
    END IF;

    IF has_prev THEN
        prev := concat(out_records->0->>'collection', ':', out_records->0->>'id');
        RAISE NOTICE 'HAS PREV | %', prev;
    END IF;

    RAISE NOTICE 'Time to get prev/next %', age_ms(timer);
    timer := clock_timestamp();

    IF context(_search->'conf') != 'off' THEN
        context := jsonb_strip_nulls(jsonb_build_object(
            'limit', _limit,
            'matched', total_count,
            'returned', coalesce(jsonb_array_length(out_records), 0)
        ));
    ELSE
        context := jsonb_strip_nulls(jsonb_build_object(
            'limit', _limit,
            'returned', coalesce(jsonb_array_length(out_records), 0)
        ));
    END IF;

    collection := jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb),
        'next', next,
        'prev', prev,
        'context', context
    );

    IF get_setting_bool('timing', _search->'conf') THEN
        collection = collection || jsonb_build_object('timing', age_ms(init_ts));
    END IF;

    RAISE NOTICE 'Time to build final json %', age_ms(timer);
    timer := clock_timestamp();

    RAISE NOTICE 'Total Time: %', age_ms(current_timestamp);
    RAISE NOTICE 'RETURNING % records. NEXT: %. PREV: %', collection->'context'->>'returned', collection->>'next', collection->>'prev';
    RETURN collection;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION search_cursor(_search jsonb = '{}'::jsonb) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
    searches searches%ROWTYPE;
    _where text;
    _orderby text;
    q text;

BEGIN
    searches := search_query(_search);
    _where := searches._where;
    _orderby := searches.orderby;

    OPEN curs FOR
        WITH p AS (
            SELECT * FROM partition_queries(_where, _orderby) p
        )
        SELECT
            CASE WHEN EXISTS (SELECT 1 FROM p) THEN
                (SELECT format($q$
                    SELECT * FROM (
                        %s
                    ) total
                    $q$,
                    string_agg(
                        format($q$ SELECT * FROM ( %s ) AS sub $q$, p),
                        '
                        UNION ALL
                        '
                    )
                ))
            ELSE NULL
            END FROM p;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE VIEW collections_asitems AS
SELECT
    id,
    geometry,
    'collections' AS collection,
    datetime,
    end_datetime,
    jsonb_build_object(
        'properties', content - '{links,assets,stac_version,stac_extensions}',
        'links', content->'links',
        'assets', content->'assets',
        'stac_version', content->'stac_version',
        'stac_extensions', content->'stac_extensions'
    ) AS content,
    content as collectionjson
FROM collections;


CREATE OR REPLACE FUNCTION collection_search_matched(
    IN _search jsonb DEFAULT '{}'::jsonb,
    OUT matched bigint
) RETURNS bigint AS $$
DECLARE
    _where text := stac_search_to_where(_search);
BEGIN
    EXECUTE format(
        $query$
            SELECT
                count(*)
            FROM
                collections_asitems
            WHERE %s
            ;
        $query$,
        _where
    ) INTO matched;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION collection_search_rows(
    _search jsonb DEFAULT '{}'::jsonb
) RETURNS SETOF jsonb AS $$
DECLARE
    _where text := stac_search_to_where(_search);
    _limit int := coalesce((_search->>'limit')::int, 10);
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    _orderby text;
    _offset int := COALESCE((_search->>'offset')::int, 0);
BEGIN
    _orderby := sort_sqlorderby(
        jsonb_build_object(
            'sortby',
            coalesce(
                _search->'sortby',
                '[{"field": "id", "direction": "asc"}]'::jsonb
            )
        )
    );
    RETURN QUERY EXECUTE format(
        $query$
            SELECT
                jsonb_fields(collectionjson, %L) as c
            FROM
                collections_asitems
            WHERE %s
            ORDER BY %s
            LIMIT %L
            OFFSET %L
            ;
        $query$,
        _fields,
        _where,
        _orderby,
        _limit,
        _offset
    );
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION collection_search(
    _search jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    out_records jsonb;
    number_matched bigint := collection_search_matched(_search);
    number_returned bigint;
    _limit int := coalesce((_search->>'limit')::float::int, 10);
    _offset int := coalesce((_search->>'offset')::float::int, 0);
    links jsonb := '[]';
    ret jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'), '/collections');
    prevoffset int;
    nextoffset int;
BEGIN
    SELECT
        coalesce(jsonb_agg(c), '[]')
    INTO out_records
    FROM collection_search_rows(_search) c;

    number_returned := jsonb_array_length(out_records);

    IF _limit <= number_matched THEN --need to have paging links
        nextoffset := least(_offset + _limit, number_matched - 1);
        prevoffset := greatest(_offset - _limit, 0);
        IF _offset = 0 THEN -- no previous paging

            links := jsonb_build_array(
                jsonb_build_object(
                    'rel', 'next',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', nextoffset),
                    'merge', TRUE
                )
            );
        ELSE
            links := jsonb_build_array(
                jsonb_build_object(
                    'rel', 'prev',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', prevoffset),
                    'merge', TRUE
                ),
                jsonb_build_object(
                    'rel', 'next',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', nextoffset),
                    'merge', TRUE
                )
            );
        END IF;
    END IF;

    ret := jsonb_build_object(
        'collections', out_records,
        'context', jsonb_build_object(
            'limit', _limit,
            'matched', number_matched,
            'returned', number_returned
        ),
        'links', links
    );
    RETURN ret;

END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION tileenvelope(zoom int, x int, y int) RETURNS geometry AS $$
WITH t AS (
    SELECT
        20037508.3427892 as merc_max,
        -20037508.3427892 as merc_min,
        (2 * 20037508.3427892) / (2 ^ zoom) as tile_size
)
SELECT st_makeenvelope(
    merc_min + (tile_size * x),
    merc_max - (tile_size * (y + 1)),
    merc_min + (tile_size * (x + 1)),
    merc_max - (tile_size * y),
    3857
) FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;DROP FUNCTION IF EXISTS mercgrid;


CREATE OR REPLACE FUNCTION ftime() RETURNS interval as $$
SELECT age(clock_timestamp(), transaction_timestamp());
$$ LANGUAGE SQL;
SET SEARCH_PATH to pgstac, public;

DROP FUNCTION IF EXISTS geometrysearch;
CREATE OR REPLACE FUNCTION geometrysearch(
    IN geom geometry,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE, -- Return as soon as the passed in geometry is full covered
    IN skipcovered boolean DEFAULT TRUE -- Skip any items that would show up completely under the previous items
) RETURNS jsonb AS $$
DECLARE
    search searches%ROWTYPE;
    curs refcursor;
    _where text;
    query text;
    iter_record items%ROWTYPE;
    out_records jsonb := '{}'::jsonb[];
    exit_flag boolean := FALSE;
    counter int := 1;
    scancounter int := 1;
    remaining_limit int := _scanlimit;
    tilearea float;
    unionedgeom geometry;
    clippedgeom geometry;
    unionedgeom_area float := 0;
    prev_area float := 0;
    excludes text[];
    includes text[];

BEGIN
    DROP TABLE IF EXISTS pgstac_results;
    CREATE TEMP TABLE pgstac_results (content jsonb) ON COMMIT DROP;

    -- If the passed in geometry is not an area set exitwhenfull and skipcovered to false
    IF ST_GeometryType(geom) !~* 'polygon' THEN
        RAISE NOTICE 'GEOMETRY IS NOT AN AREA';
        skipcovered = FALSE;
        exitwhenfull = FALSE;
    END IF;

    -- If skipcovered is true then you will always want to exit when the passed in geometry is full
    IF skipcovered THEN
        exitwhenfull := TRUE;
    END IF;

    SELECT * INTO search FROM searches WHERE hash=queryhash;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Search with Query Hash % Not Found', queryhash;
    END IF;

    tilearea := st_area(geom);
    _where := format('%s AND st_intersects(geometry, %L::geometry)', search._where, geom);


    FOR query IN SELECT * FROM partition_queries(_where, search.orderby) LOOP
        query := format('%s LIMIT %L', query, remaining_limit);
        RAISE NOTICE '%', query;
        OPEN curs FOR EXECUTE query;
        LOOP
            FETCH curs INTO iter_record;
            EXIT WHEN NOT FOUND;
            IF exitwhenfull OR skipcovered THEN -- If we are not using exitwhenfull or skipcovered, we do not need to do expensive geometry operations
                clippedgeom := st_intersection(geom, iter_record.geometry);

                IF unionedgeom IS NULL THEN
                    unionedgeom := clippedgeom;
                ELSE
                    unionedgeom := st_union(unionedgeom, clippedgeom);
                END IF;

                unionedgeom_area := st_area(unionedgeom);

                IF skipcovered AND prev_area = unionedgeom_area THEN
                    scancounter := scancounter + 1;
                    CONTINUE;
                END IF;

                prev_area := unionedgeom_area;

                RAISE NOTICE '% % % %', unionedgeom_area/tilearea, counter, scancounter, ftime();
            END IF;
            RAISE NOTICE '% %', iter_record, content_hydrate(iter_record, fields);
            INSERT INTO pgstac_results (content) VALUES (content_hydrate(iter_record, fields));

            IF counter >= _limit
                OR scancounter > _scanlimit
                OR ftime() > _timelimit
                OR (exitwhenfull AND unionedgeom_area >= tilearea)
            THEN
                exit_flag := TRUE;
                EXIT;
            END IF;
            counter := counter + 1;
            scancounter := scancounter + 1;

        END LOOP;
        CLOSE curs;
        EXIT WHEN exit_flag;
        remaining_limit := _scanlimit - scancounter;
    END LOOP;

    SELECT jsonb_agg(content) INTO out_records FROM pgstac_results WHERE content IS NOT NULL;

    RETURN jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb)
    );
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS geojsonsearch;
CREATE OR REPLACE FUNCTION geojsonsearch(
    IN geojson jsonb,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
    SELECT * FROM geometrysearch(
        st_geomfromgeojson(geojson),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        exitwhenfull,
        skipcovered
    );
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS xyzsearch;
CREATE OR REPLACE FUNCTION xyzsearch(
    IN _x int,
    IN _y int,
    IN _z int,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
    SELECT * FROM geometrysearch(
        st_transform(tileenvelope(_z, _x, _y), 4326),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        exitwhenfull,
        skipcovered
    );
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE analyze_items() AS $$
DECLARE
    q text;
    timeout_ts timestamptz;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        SELECT format('ANALYZE (VERBOSE, SKIP_LOCKED) %I;', relname) INTO q
        FROM pg_stat_user_tables
        WHERE relname like '_item%' AND (n_mod_since_analyze>0 OR last_analyze IS NULL) LIMIT 1;
        IF NOT FOUND THEN
            EXIT;
        END IF;
        RAISE NOTICE '%', q;
        EXECUTE q;
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE PROCEDURE validate_constraints() AS $$
DECLARE
    q text;
BEGIN
    FOR q IN
    SELECT
        FORMAT(
            'ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;',
            nsp.nspname,
            cls.relname,
            con.conname
        )

    FROM pg_constraint AS con
        JOIN pg_class AS cls
        ON con.conrelid = cls.oid
        JOIN pg_namespace AS nsp
        ON cls.relnamespace = nsp.oid
    WHERE convalidated = FALSE AND contype in ('c','f')
    AND nsp.nspname = 'pgstac'
    LOOP
        RAISE NOTICE '%', q;
        PERFORM run_or_queue(q);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION collection_extent(_collection text, runupdate boolean default false) RETURNS jsonb AS $$
DECLARE
    geom_extent geometry;
    mind timestamptz;
    maxd timestamptz;
    extent jsonb;
BEGIN
    IF runupdate THEN
        PERFORM update_partition_stats_q(partition)
        FROM partitions_view WHERE collection=_collection;
    END IF;
    SELECT
        min(lower(dtrange)),
        max(upper(edtrange)),
        st_extent(spatial)
    INTO
        mind,
        maxd,
        geom_extent
    FROM partitions_view
    WHERE collection=_collection;

    IF geom_extent IS NOT NULL AND mind IS NOT NULL AND maxd IS NOT NULL THEN
        extent := jsonb_build_object(
            'extent', jsonb_build_object(
                'spatial', jsonb_build_object(
                    'bbox', to_jsonb(array[array[st_xmin(geom_extent), st_ymin(geom_extent), st_xmax(geom_extent), st_ymax(geom_extent)]])
                ),
                'temporal', jsonb_build_object(
                    'interval', to_jsonb(array[array[mind, maxd]])
                )
            )
        );
        RETURN extent;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;
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
SELECT set_version('0.8.6');
