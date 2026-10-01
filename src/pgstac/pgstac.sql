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

-- PgSTAC references PostGIS without schema qualification and runs with
-- search_path pinned to "pgstac, public", so PostGIS has to be reachable from
-- there. Fail here rather than part way through creating the schema.
DO $$
  BEGIN
    IF to_regtype('geometry') IS NULL THEN
      RAISE EXCEPTION 'PostGIS is not reachable from search_path "pgstac, public"'
        USING HINT = 'PgSTAC requires the postgis extension in the public schema.';
    END IF;
  END
$$;

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
-- The argument list differs from the installed signature, which CREATE OR REPLACE cannot
-- change, so both would otherwise exist at once. 998_idempotent_post addresses maintain_index by
-- bare name and would fail with "function name is not unique" if the other overload survived.
DROP FUNCTION IF EXISTS maintain_index(text, text, bigint, boolean, boolean, boolean);

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

-- The keys a content->'a'->'b' chain names, in order, NULL for anything else; a doubled quote
-- stands for one quote in a key. Read by the conversion below and dropped with it, so it never
-- joins the schema the diff that follows is calculated against. A path this cannot read becomes
-- NULL, and those rows are named in the NOTICE below before anything is converted.
CREATE OR REPLACE FUNCTION content_keys(_path text) RETURNS text[] AS $fn$
    SELECT array_agg(replace(m[1], '''''', '''') ORDER BY o)
    FROM regexp_matches(_path, $re$->\s*'((?:[^']|'')*)'$re$, 'g') WITH ORDINALITY AS u(m, o)
    WHERE _path ~ $re$^\s*content(\s*->\s*'(?:[^']|'')*')+\s*$$re$;
$fn$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- property_path held the SQL path expression a queryable was read through; it now holds the keys
-- that expression named. The plain cast the migration makes of the column fails on a legacy value,
-- so convert here, before it runs. A value content_keys cannot read names keys that cannot be
-- recovered and becomes NULL, falling the queryable back to those of its name.
DO $conv$
  DECLARE
    _lost text;
  BEGIN
    -- Guarded because a plain ALTER fails on the second run, and because content_keys takes the
    -- column's old type: once converted, the name no longer resolves.
    IF EXISTS (
      SELECT FROM pg_attribute
      WHERE attrelid = to_regclass('pgstac.queryables')
        AND attname = 'property_path'
        AND atttypid = 'text'::regtype
    ) THEN
      -- A value this cannot read becomes NULL, which re-points the queryable at the keys of its
      -- own name -- a different member of the item. Name those rows rather than losing them
      -- quietly.
      SELECT string_agg(format('%s (%s)', name, property_path), ', ' ORDER BY id) INTO _lost
      FROM queryables
      WHERE property_path IS NOT NULL AND content_keys(property_path) IS NULL;
      IF _lost IS NOT NULL THEN
        RAISE NOTICE 'property_path could not be read for: %. These queryables now read the keys of their own name.', _lost;
      END IF;
      ALTER TABLE queryables ALTER COLUMN property_path TYPE text[] USING content_keys(property_path);
    END IF;
  END
$conv$;
DROP FUNCTION content_keys(text);

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


-- How many times a queued statement is run before it is left for an operator. At least one,
-- or nothing would ever be eligible and the queue could not drain.
CREATE OR REPLACE FUNCTION queue_retries() RETURNS int AS $$
    SELECT greatest(coalesce(get_setting('queue_retries'), '3')::int, 1);
$$ LANGUAGE SQL;

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
    added timestamptz DEFAULT now(),
    attempts int NOT NULL DEFAULT 0
);

DROP TABLE IF EXISTS query_queue_history;
CREATE TABLE query_queue_history(
    query text,
    added timestamptz NOT NULL,
    finished timestamptz NOT NULL DEFAULT now(),
    error text,
    attempts int
);

-- Runs one statement that still has attempts left, or returns FALSE when none has. A failure
-- leaves the row in the queue, so a deadlock victim is retried rather than lost. A statement
-- that runs out of attempts is recorded in the history with its error and dropped.
CREATE OR REPLACE FUNCTION run_queued_query() RETURNS boolean AS $$
DECLARE
    qitem query_queue%ROWTYPE;
    _retries int := queue_retries();
    error text;
BEGIN
    -- Fewest attempts first, so a statement that keeps failing yields to the rest of the queue
    -- rather than spending its whole budget back to back. Counted at claim time: the failure
    -- below is caught in a subtransaction, but a backend killed outright records nothing.
    UPDATE query_queue SET attempts = attempts + 1
    WHERE query = (
        SELECT query FROM query_queue
        WHERE attempts < _retries
        ORDER BY attempts, added DESC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING * INTO qitem;
    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;
    BEGIN
        RAISE DEBUG 'RUNNING QUERY: %', qitem.query;
        EXECUTE qitem.query;
        EXCEPTION WHEN others THEN
            error := format('%s | %s', SQLERRM, SQLSTATE);
    END;
    IF error IS NULL THEN
        DELETE FROM query_queue WHERE query = qitem.query;
    ELSIF qitem.attempts < _retries THEN
        RAISE NOTICE 'Queued query failed on attempt % of %, will retry: % | %',
            qitem.attempts, _retries, qitem.query, error;
    ELSE
        -- Out of attempts: recorded below and dropped, so the queue can reach empty. The
        -- history row carries the error that stopped it.
        RAISE WARNING 'Queued query failed on all % attempts: % | %',
            _retries, qitem.query, error;
        DELETE FROM query_queue WHERE query = qitem.query;
    END IF;
    INSERT INTO query_queue_history (query, added, finished, error, attempts)
        VALUES (qitem.query, qitem.added, clock_timestamp(), error, qitem.attempts);
    RETURN TRUE;
END;
$$ LANGUAGE PLPGSQL;

-- Rows with no attempts left that run_queued_query did not retire itself: a backend killed
-- mid-statement, or a queue_retries lowered since they were queued. Without this they are
-- never eligible again and the queue never reaches empty.
CREATE OR REPLACE FUNCTION retire_queued_queries()
RETURNS TABLE (query text, attempts int) AS $$
    WITH retired AS (
        DELETE FROM pgstac.query_queue q
        WHERE q.attempts >= pgstac.queue_retries()
        RETURNING q.query, q.added, q.attempts
    ), recorded AS (
        INSERT INTO pgstac.query_queue_history (query, added, finished, error, attempts)
        SELECT
            r.query,
            r.added,
            clock_timestamp(),
            'Retired with no attempts left; the failure is on an earlier attempt of this query',
            r.attempts
        FROM retired r
        RETURNING 1
    )
    SELECT r.query, r.attempts FROM retired r;
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE run_queued_queries() AS $$
DECLARE
    timeout_ts timestamptz := statement_timestamp() + queue_timeout();
BEGIN
    WHILE clock_timestamp() < timeout_ts LOOP
        EXIT WHEN NOT run_queued_query();
        COMMIT;
    END LOOP;
    PERFORM retire_queued_queries();
    COMMIT;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION run_queued_queries_intransaction() RETURNS int AS $$
DECLARE
    timeout_ts timestamptz := statement_timestamp() + queue_timeout();
    cnt int := 0;
BEGIN
    WHILE clock_timestamp() < timeout_ts LOOP
        EXIT WHEN NOT run_queued_query();
        cnt := cnt + 1;
    END LOOP;
    PERFORM retire_queued_queries();
    RETURN cnt;
END;
$$ LANGUAGE PLPGSQL;


-- TRUE if the query ran, FALSE if it was queued, so callers can tell whether
-- its effects are visible in this transaction.
CREATE OR REPLACE FUNCTION run_or_queue(query text) RETURNS boolean AS $$
DECLARE
    use_queue boolean := get_setting_bool('use_queue');
BEGIN
    IF get_setting_bool('debug') THEN
        RAISE NOTICE '%', query;
    END IF;
    IF use_queue THEN
        -- A fresh request for a statement already queued gets a fresh budget: the earlier
        -- attempts were spent on a state this caller has just superseded.
        -- By constraint, not by column: the target column and this function's parameter
        -- are both named query, which ON CONFLICT (query) cannot tell apart.
        INSERT INTO query_queue (query) VALUES (query)
        ON CONFLICT ON CONSTRAINT query_queue_pkey DO UPDATE SET attempts = 0;
        RETURN FALSE;
    END IF;
    EXECUTE query;
    RETURN TRUE;
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

    -- Undrained queue means stale statistics and constraints, silently.
    SELECT count(*) INTO settingval FROM query_queue;
    IF settingval::bigint > 0 THEN
        RAISE WARNING '% queries are waiting in query_queue. Run "CALL run_queued_queries();" (pypgstac runqueue) to drain it -- until then partition statistics and constraints are stale.', settingval;
    END IF;

END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public SET CLIENT_MIN_MESSAGES TO NOTICE;


-- pg_get_expr renders timestamps under the READER's DateStyle, so this is the only place
-- pgstac deparses a partition bound. ISO text then parses the same under any DateStyle.
CREATE OR REPLACE FUNCTION partition_bound_expr(_oid oid) RETURNS text AS $$
    SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = _oid;
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE SET DateStyle TO 'ISO, YMD';

-- The collection of a LIST partition. The bound is a SQL literal, so quotes inside an id are
-- doubled; undoing that once keeps an id containing a quote matchable.
CREATE OR REPLACE FUNCTION partition_collection(_oid oid) RETURNS text AS $$
    SELECT replace(
        substring(pgstac.partition_bound_expr(_oid), '^FOR VALUES IN \(''(.*)''\)$'),
        '''''',
        ''''
    );
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;
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
        includes := includes || (
            CASE WHEN j ? 'collection' THEN
                '["id","collection"]'
            ELSE
                '["id"]'
            END)::jsonb;
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
        RAISE DEBUG 'DT: %, EDT: %', dt, edt;
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

-- Base items a collection has had since its first edit; none until then.
CREATE TABLE IF NOT EXISTS base_items (
    -- ON UPDATE CASCADE because collections.id is generated from the content: rewriting a
    -- collection's id would otherwise orphan these rows under the old key, and a collection
    -- later recreated with that id would inherit base items it was never dehydrated against.
    collection text NOT NULL REFERENCES collections(id) ON DELETE CASCADE ON UPDATE CASCADE,
    id int GENERATED ALWAYS AS IDENTITY,
    base_item jsonb NOT NULL,
    PRIMARY KEY (collection, id)
);

CREATE OR REPLACE FUNCTION collection_base_item(cid text, _base_item_id int DEFAULT NULL) RETURNS jsonb AS $$
    SELECT CASE
        WHEN _base_item_id IS NOT NULL THEN
            (SELECT base_item FROM pgstac.base_items WHERE collection = cid AND id = _base_item_id)
        ELSE coalesce(
            (SELECT base_item FROM pgstac.base_items WHERE collection = cid ORDER BY id ASC LIMIT 1),
            (SELECT base_item FROM pgstac.collections WHERE id = cid)
        )
    END;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

-- The collection's current base item and its base_items id (NULL if never edited).
CREATE OR REPLACE FUNCTION current_base_item(cid text, OUT base_item_id int, OUT base_item jsonb) AS $$
    -- The id only when that row really holds the current base item: these are two independent
    -- reads, and a tag naming anything else hydrates against content the item was not dehydrated
    -- from, silently. Before the first edit there are no rows, so the id is NULL and the item is
    -- stored untagged.
    SELECT b.id, c.base_item
    FROM pgstac.collections c
    LEFT JOIN LATERAL (
        SELECT id FROM pgstac.base_items
        WHERE collection = c.id AND base_item = c.base_item
        ORDER BY id DESC LIMIT 1
    ) b ON TRUE
    WHERE c.id = cid;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;


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


-- SECURITY DEFINER: the delete trigger drops partition tables, which are owned
-- by pgstac_admin.
CREATE OR REPLACE FUNCTION delete_collection(_id text) RETURNS VOID AS $$
BEGIN
    DELETE FROM collections WHERE id = _id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _id USING ERRCODE = 'no_data_found';
    END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
    SELECT content FROM collections
    WHERE id=$1
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION all_collections() RETURNS jsonb AS $$
    SELECT coalesce(jsonb_agg(content), '[]'::jsonb) FROM collections;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

-- SECURITY DEFINER: the partitions dropped here are owned by pgstac_admin.
CREATE OR REPLACE FUNCTION collection_delete_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    collection_base_partition text := concat('_items_', OLD.key);
BEGIN
    -- A recreated collection id must not inherit these rows.
    DELETE FROM base_items WHERE collection = OLD.id;
    -- A queryable that applies only to this collection goes with it; the others drop it from
    -- their collection_ids.
    DELETE FROM queryables WHERE collection_ids = ARRAY[OLD.id];
    UPDATE queryables SET collection_ids = array_remove(collection_ids, OLD.id) WHERE OLD.id = ANY(collection_ids);
    -- Tables before rows: check_partition takes these locks in the same order,
    -- and the reverse deadlocks against a concurrent partition create.
    EXECUTE format($q$
        DROP TABLE IF EXISTS %I CASCADE;
        DELETE FROM partition_stats WHERE collection=%L;
        $q$,
        collection_base_partition,
        OLD.id
    );
    RETURN OLD;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collection_delete_trigger BEFORE DELETE ON collections
FOR EACH ROW EXECUTE FUNCTION collection_delete_trigger_func();
CREATE TABLE queryables (
    id bigint GENERATED ALWAYS AS identity PRIMARY KEY,
    name text NOT NULL,
    collection_ids text[], -- NULL: every collection
    definition jsonb,
    property_path text[], -- the keys under content the queryable reads, when not those of its name
    property_wrapper text,
    property_index_type text
);
CREATE INDEX queryables_name_idx ON queryables (name);
CREATE INDEX queryables_collection_idx ON queryables USING GIN (collection_ids);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);

-- The property_wrapper names a queryable may use, each the name of a pgstac.<name>(jsonb)
-- function. A custom wrapper is registered by adding its row; seeded in 998_idempotent_post.
CREATE TABLE IF NOT EXISTS queryable_wrappers (name text PRIMARY KEY);

-- Strips every leading "properties." so a property name resolves the same however it was spelled.
CREATE OR REPLACE FUNCTION strip_properties_prefix(dotpath text) RETURNS text AS $$
    SELECT regexp_replace(dotpath, $r$^(properties\.)+$r$, '');
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The one spelling of collection_ids: sorted, deduplicated, and NULL (every collection) when empty.
CREATE OR REPLACE FUNCTION canonical_collection_ids(_collection_ids text[]) RETURNS text[] AS $$
    SELECT array_agg(DISTINCT c ORDER BY c) FROM unnest(_collection_ids) c;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The rows a queryable of this name and these collections cannot coexist with. NULL
-- collection_ids covers every collection, so it conflicts with any other row of the name.
CREATE OR REPLACE FUNCTION conflicting_queryables(_name text, _collection_ids text[]) RETURNS SETOF queryables AS $$
    SELECT * FROM queryables q
    WHERE
        q.name = strip_properties_prefix(_name)
        AND (
            q.collection_ids IS NULL
            OR canonical_collection_ids(_collection_ids) IS NULL
            OR q.collection_ids && _collection_ids
        );
$$ LANGUAGE SQL STABLE;

-- Only a registered name is resolved to a function, so a row cannot steer the lookup at an arbitrary one.
CREATE OR REPLACE FUNCTION check_queryable_wrapper(wrapper text) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (SELECT FROM queryable_wrappers WHERE name = wrapper) THEN
        RAISE check_violation USING MESSAGE = format('%s is not in queryable_wrappers.', wrapper);
    END IF;
    IF to_regprocedure(format('pgstac.%I(jsonb)', wrapper)) IS NULL THEN
        RAISE undefined_function USING MESSAGE = format(
            '%s is registered in queryable_wrappers but pgstac.%I(jsonb) does not exist.', wrapper, wrapper
        );
    END IF;
END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION queryables_constraint_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    conflicts json;
BEGIN
    -- Only the parameters of upsert_queryable and delete_queryable read empty collection_ids
    -- as every collection.
    IF NEW.collection_ids = '{}' THEN
        RAISE check_violation USING MESSAGE = 'collection_ids is empty; a queryable for every collection has NULL collection_ids.';
    END IF;
    -- Names and collection_ids are stored in one spelling, so equality is enough to find a row.
    NEW.name := strip_properties_prefix(NEW.name);
    NEW.collection_ids := canonical_collection_ids(NEW.collection_ids);
    -- Serializes writers of one property so the conflict check cannot race.
    PERFORM pg_advisory_xact_lock(hashtext(NEW.name));
    SELECT json_agg(row_to_json(q)) INTO conflicts
    FROM conflicting_queryables(NEW.name, NEW.collection_ids) q
    WHERE q.id IS DISTINCT FROM NEW.id;
    IF conflicts IS NOT NULL THEN
        RAISE unique_violation USING MESSAGE = format(
            'There is already a queryable for %s for a collection in %s: %s', NEW.name, NEW.collection_ids, conflicts
        );
    END IF;
    IF EXISTS (SELECT FROM unnest(NEW.collection_ids) c WHERE NOT EXISTS (SELECT FROM collections WHERE id = c)) THEN
        RAISE foreign_key_violation USING MESSAGE = format('One or more collections in %s do not exist.', NEW.collection_ids);
    END IF;
    PERFORM check_queryable_wrapper(queryable_wrapper(NEW.property_wrapper, NEW.definition));
    IF NEW.property_path IS NOT NULL AND (
        coalesce(array_ndims(NEW.property_path), 0) <> 1
        OR EXISTS (SELECT FROM unnest(NEW.property_path) k WHERE k IS NULL OR k = '')
    ) THEN
        RAISE check_violation USING MESSAGE = format('property_path %s is not a list of keys.', NEW.property_path);
    END IF;
    -- A column of items has the index items itself carries, so nothing to ask for.
    IF NEW.property_path IS NOT NULL AND queryable_column(NEW.name) IS NOT NULL THEN
        RAISE check_violation USING
            MESSAGE = format('%s names a column of items, which is read as itself, so property_path would be ignored.', NEW.name);
    END IF;
    IF NEW.property_index_type IS NOT NULL AND queryable_column(NEW.name) IS NOT NULL THEN
        RAISE check_violation USING MESSAGE = format('%s is read as a column of items, which items indexes itself.', NEW.name);
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
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE queryables_constraint_triggerfunc();

-- The row passed wins: every row of the name it would conflict with is replaced by it. Delete and
-- insert rather than update so the insert trigger checks the row the same way it does a new one.
-- An empty collection_ids means every collection.
CREATE OR REPLACE FUNCTION upsert_queryable(
    name text,
    definition jsonb DEFAULT NULL,
    property_wrapper text DEFAULT NULL,
    property_index_type text DEFAULT NULL,
    collection_ids text[] DEFAULT NULL,
    property_path text[] DEFAULT NULL
) RETURNS VOID AS $$
    SELECT pg_advisory_xact_lock(hashtext(strip_properties_prefix(upsert_queryable.name)));

    DELETE FROM queryables q
    USING conflicting_queryables(upsert_queryable.name, upsert_queryable.collection_ids) c
    WHERE q.id = c.id
    ;

    INSERT INTO queryables (name, definition, property_wrapper, property_index_type, collection_ids, property_path)
    VALUES (
        upsert_queryable.name,
        upsert_queryable.definition,
        upsert_queryable.property_wrapper,
        upsert_queryable.property_index_type,
        canonical_collection_ids(upsert_queryable.collection_ids),
        upsert_queryable.property_path
    )
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

-- Loads a whole queryables document: one row per property under "properties", the wrapper and
-- the index method both inferred from each property's own definition. The fields items carries
-- as columns are already indexed, so they are neither loaded nor treated as missing. With
-- _delete_missing, queryables of these collections that the document does not name are removed.
-- Returns the number of properties loaded.
CREATE OR REPLACE FUNCTION upsert_queryables(
    _definitions jsonb,
    _collection_ids text[] DEFAULT NULL,
    _index_fields text[] DEFAULT NULL,
    _delete_missing boolean DEFAULT FALSE
) RETURNS int AS $$
DECLARE
    _properties jsonb := _definitions->'properties';
    _name text;
    _definition jsonb;
    _loaded int := 0;
BEGIN
    IF jsonb_typeof(_properties) IS DISTINCT FROM 'object' OR _properties = '{}'::jsonb THEN
        RAISE EXCEPTION 'No properties found in the queryables definition'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    FOR _name, _definition IN SELECT key, value FROM jsonb_each(_properties) LOOP
        CONTINUE WHEN queryable_column(_name) IS NOT NULL;
        -- The wrapper is left NULL so queryable_wrapper infers it, and default_index_type
        -- picks the method from the same definition, keeping both rules in one place.
        PERFORM upsert_queryable(
            _name,
            _definition,
            NULL,
            CASE WHEN _name = ANY (coalesce(_index_fields, '{}'::text[]))
                 THEN default_index_type(_definition) END,
            _collection_ids
        );
        _loaded := _loaded + 1;
    END LOOP;

    IF _delete_missing AND _loaded > 0 THEN
        PERFORM delete_missing_queryables(
            ARRAY(SELECT jsonb_object_keys(_properties)),
            _collection_ids
        );
    END IF;

    RETURN _loaded;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

-- Removes the queryables of these collections that _names does not list, and returns how many
-- went. delete_queryable takes one name and raises when it matches nothing, which is the
-- ordinary outcome here. Empty _collection_ids means every collection, as it does elsewhere.
CREATE OR REPLACE FUNCTION delete_missing_queryables(
    _names text[],
    _collection_ids text[] DEFAULT NULL
) RETURNS int AS $$
DECLARE
    _deleted int;
BEGIN
    DELETE FROM queryables q
    WHERE q.collection_ids IS NOT DISTINCT FROM canonical_collection_ids(_collection_ids)
      -- A queryable named for a column of items is read as that column; a document that does
      -- not mention it is not asking for it to be removed.
      AND queryable_column(q.name) IS NULL
      AND q.name <> ALL (
          SELECT strip_properties_prefix(n) FROM unnest(coalesce(_names, '{}')) n
      );
    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION delete_queryable(
    name text,
    collection_ids text[] DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    DELETE FROM queryables q
    WHERE
        q.name = strip_properties_prefix(delete_queryable.name)
        AND q.collection_ids IS NOT DISTINCT FROM canonical_collection_ids(delete_queryable.collection_ids)
    ;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Queryable % for collections % does not exist',
            delete_queryable.name, delete_queryable.collection_ids
            USING ERRCODE = 'no_data_found';
    END IF;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- Updated in place so the update trigger validates the new member and builds its indexes.
-- A global queryable is the only row of its name, so several rows are all per-collection.
CREATE OR REPLACE FUNCTION add_collection_to_queryable(name text, collection_id text) RETURNS VOID AS $$
DECLARE
    q record;
BEGIN
    -- Same lock upsert_queryable takes, held for the read as well as the
    -- write: without it a concurrent upsert deletes and reinserts the row
    -- between the SELECT and the UPDATE, and this silently changes nothing.
    PERFORM pg_advisory_xact_lock(hashtext(strip_properties_prefix(add_collection_to_queryable.name)));

    SELECT id, collection_ids, count(*) OVER () AS n INTO q
    FROM queryables WHERE queryables.name = strip_properties_prefix(add_collection_to_queryable.name);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Queryable % does not exist, use upsert_queryable to create it', name
            USING ERRCODE = 'no_data_found';
    ELSIF q.collection_ids IS NULL THEN
        RAISE EXCEPTION 'Queryable % is global and already covers every collection', name
            USING ERRCODE = 'unique_violation';
    ELSIF q.n > 1 THEN
        RAISE EXCEPTION 'Queryable % has several per-collection rows, use upsert_queryable with the full list', name
            USING ERRCODE = 'too_many_rows';
    ELSIF collection_id = ANY(q.collection_ids) THEN
        RETURN;
    END IF;
    UPDATE queryables SET collection_ids = collection_ids || collection_id WHERE id = q.id;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION remove_collection_from_queryable(name text, collection_id text) RETURNS VOID AS $$
DECLARE
    q record;
BEGIN
    -- The same lock, for the same reason, as add_collection_to_queryable.
    PERFORM pg_advisory_xact_lock(hashtext(strip_properties_prefix(remove_collection_from_queryable.name)));

    SELECT id, collection_ids, array_remove(collection_ids, collection_id) AS remaining INTO q
    FROM queryables
    WHERE queryables.name = strip_properties_prefix(remove_collection_from_queryable.name)
        AND (collection_ids IS NULL OR collection_id = ANY(collection_ids));
    IF NOT FOUND THEN
        RAISE EXCEPTION 'No queryable % includes collection %', name, collection_id
            USING ERRCODE = 'no_data_found';
    ELSIF q.collection_ids IS NULL THEN
        RAISE EXCEPTION 'Queryable % is global; use delete_queryable', name
            USING ERRCODE = 'invalid_parameter_value';
    ELSIF q.remaining = '{}' THEN
        DELETE FROM queryables WHERE id = q.id;
    ELSE
        UPDATE queryables SET collection_ids = q.remaining WHERE id = q.id;
    END IF;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- What the trigger does to one row on write, done to every row: the repair for a database whose
-- queryables were edited by hand, and a no-op on one already in the stored spelling.
CREATE OR REPLACE FUNCTION canonicalize_queryables() RETURNS VOID AS $$
DECLARE
    r record;
BEGIN
    -- Nothing here may UPDATE a row before the table is canonical: the constraint trigger fires
    -- on every UPDATE and checks the row against rows the rest of this function has not fixed
    -- yet, so a legacy pair collides and the whole migration aborts. Everything below is
    -- therefore a DELETE until the single rewrite at the end.

    -- collection_ids naming a collection that no longer exists is the ordinary state of a
    -- long-lived catalog, not corruption. An empty array means every collection, not a row that
    -- applies to none, so it survives and becomes NULL in the rewrite. A row that names only
    -- collections which are gone does not.
    DELETE FROM queryables
      WHERE collection_ids IS NOT NULL
        AND collection_ids <> '{}'
        AND NOT EXISTS (
            SELECT FROM unnest(collection_ids) c WHERE EXISTS (SELECT FROM collections WHERE id = c));

    -- Taken before the dedupe so a removed row's configuration can still be merged into the row
    -- that survives it: a bare stub from missing_queryables is usually older than the row an
    -- operator configured by hand, and discarding the latter silently downgrades the wrapper and
    -- orphans its index. pg_temp so the name cannot resolve to a real table through search_path.
    DROP TABLE IF EXISTS pg_temp._canon_snapshot;
    CREATE TEMP TABLE _canon_snapshot ON COMMIT DROP AS SELECT * FROM queryables;

    -- Oldest first, dropping a row only when a row that has already been KEPT conflicts with it.
    -- Deleting the newest conflicting row per name instead would drop a row whose only conflict
    -- is with something removed in a later round: of {A}, {A,B}, {B} the middle row goes and
    -- both ends must stay, or collection B silently loses the queryable altogether.
    FOR r IN SELECT id FROM queryables ORDER BY id LOOP
        DELETE FROM queryables a
        WHERE a.id = r.id
          AND EXISTS (
              SELECT FROM queryables b
              WHERE b.id < a.id
                AND strip_properties_prefix(b.name) = strip_properties_prefix(a.name)
                AND (canonical_collection_ids(a.collection_ids) IS NULL
                  OR canonical_collection_ids(b.collection_ids) IS NULL
                  OR a.collection_ids && b.collection_ids));
    END LOOP;

    -- What each SURVIVING ROW inherits, field by field, from the removed rows that actually
    -- conflicted with IT. Keyed per row rather than per name: two rows of one name can survive
    -- naming different collections, and handing one of them the other's wrapper would build an
    -- index and read a property through a wrapper that collection never asked for.
    DROP TABLE IF EXISTS pg_temp._canon_merge;
    CREATE TEMP TABLE _canon_merge ON COMMIT DROP AS
    WITH gone AS (
        SELECT s.* FROM _canon_snapshot s
        WHERE NOT EXISTS (SELECT FROM queryables q WHERE q.id = s.id)
    )
    SELECT q.id AS winner,
           first_notnull(g.definition ORDER BY g.id) AS definition,
           first_notnull(g.property_wrapper ORDER BY g.id) AS property_wrapper,
           first_notnull(g.property_index_type ORDER BY g.id) AS property_index_type,
           first_notnull(g.property_path ORDER BY g.id) AS property_path
    FROM queryables q
    JOIN gone g
      ON strip_properties_prefix(g.name) = strip_properties_prefix(q.name)
     AND (canonical_collection_ids(g.collection_ids) IS NULL
       OR canonical_collection_ids(q.collection_ids) IS NULL
       OR g.collection_ids && q.collection_ids)
    GROUP BY q.id;

    -- Checked here, before the rewrite below can fire the trigger's barer message. A custom
    -- wrapper may legitimately be registered, so name the row and say how to register it rather
    -- than failing with a wrapper name alone.
    FOR r IN
        SELECT q.name,
               queryable_wrapper(
                   coalesce(q.property_wrapper, m.property_wrapper),
                   coalesce(q.definition, m.definition)) AS wrapper
        FROM queryables q
        LEFT JOIN _canon_merge m ON m.winner = q.id
    LOOP
        IF NOT EXISTS (SELECT FROM queryable_wrappers WHERE name = r.wrapper) THEN
            RAISE check_violation USING
                MESSAGE = format('Queryable %s uses the property_wrapper %s, which is not registered.',
                                 r.name, r.wrapper),
                HINT = format(
                    'Register it with INSERT INTO queryable_wrappers (name) VALUES (%L); pgstac.%I(jsonb) must exist.',
                    r.wrapper, r.wrapper);
        END IF;
    END LOOP;

    -- The one rewrite: name, collection_ids, the merged configuration and the index type items
    -- reserves for itself, all at once, so the trigger only ever sees a finished row.
    UPDATE queryables q SET
        name = strip_properties_prefix(q.name),
        collection_ids = m.cids,
        definition = coalesce(q.definition, m.definition),
        property_wrapper = coalesce(q.property_wrapper, m.property_wrapper),
        property_path = coalesce(q.property_path, m.property_path),
        property_index_type = CASE
            WHEN queryable_column(strip_properties_prefix(q.name)) IS NULL
            THEN coalesce(q.property_index_type, m.property_index_type) END
    FROM (SELECT q2.id, cm.definition, cm.property_wrapper, cm.property_index_type,
                 cm.property_path,
                 canonical_collection_ids(
                     CASE WHEN q2.collection_ids IS NULL THEN NULL ELSE ARRAY(
                         SELECT c FROM unnest(q2.collection_ids) c
                         WHERE EXISTS (SELECT FROM collections WHERE id = c)) END) AS cids
          FROM queryables q2 LEFT JOIN _canon_merge cm ON cm.winner = q2.id) m
    WHERE m.id = q.id
      -- Only rows that actually change, so an unchanged indexed row does not fire the reference
      -- index trigger and rebuild an index the upgrade was supposed to leave alone.
      AND (q.name IS DISTINCT FROM strip_properties_prefix(q.name)
        OR q.collection_ids IS DISTINCT FROM m.cids
        OR (q.property_index_type IS NOT NULL
            AND queryable_column(strip_properties_prefix(q.name)) IS NOT NULL)
        OR m.definition IS NOT NULL OR m.property_wrapper IS NOT NULL
        OR m.property_index_type IS NOT NULL OR m.property_path IS NOT NULL);

    -- Build every indexed row's reference index here, where a failure names the row and stops the
    -- upgrade. The whole-table walk in 998 only warns, and the row trigger above fires only for
    -- rows this function happened to rewrite, so without this whether an unbuildable index type
    -- halts the migration depends on whether the row's name needed stripping.
    PERFORM maintain_reference_index(id) FROM queryables WHERE property_index_type IS NOT NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


-- The one rule turning a queryable name into the jsonb keys under content, used by the
-- filter and the index: STAC top-level members sit at the root, anything else under properties.
CREATE OR REPLACE FUNCTION queryable_path_elements(dotpath text) RETURNS text[] AS $$
    SELECT CASE
        WHEN e[1] IN ('assets', 'links', 'bbox', 'stac_version', 'stac_extensions', 'properties') THEN e
        ELSE 'properties'::text || e
    END
    FROM string_to_array(strip_properties_prefix(dotpath), '.') e;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The keys a queryable reads: its property_path when it has one, else those of its name.
CREATE OR REPLACE FUNCTION queryable_keys(name text, property_path text[]) RETURNS text[] AS $$
    SELECT COALESCE(property_path, queryable_path_elements(name));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- The one rendering of a key chain under content, for filter SQL and the reference index alike.
-- 000_idempotent_pre reads this spelling back, so a change here has to reach its content_keys.
CREATE OR REPLACE FUNCTION content_path(keys text[]) RETURNS text AS $$
    SELECT 'content->' || string_agg(quote_literal(k), '->' ORDER BY o)
    FROM unnest(keys) WITH ORDINALITY AS u(k, o);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The items column a property name maps to, or NULL. Not STRICT, so the planner inlines its CASE body.
CREATE OR REPLACE FUNCTION queryable_column(dotpath text) RETURNS text AS $$
    SELECT CASE strip_properties_prefix(dotpath)
        WHEN 'start_datetime' THEN 'datetime'
        WHEN 'id' THEN 'id'
        WHEN 'geometry' THEN 'geometry'
        WHEN 'datetime' THEN 'datetime'
        WHEN 'end_datetime' THEN 'end_datetime'
        WHEN 'collection' THEN 'collection'
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- The wrapper a queryable is read through: its own, or one inferred from its definition.
-- jsonb ? matches a scalar type as well as a member of a list such as ["number", "null"].
CREATE OR REPLACE FUNCTION queryable_wrapper(property_wrapper text, definition jsonb) RETURNS text AS $$
    SELECT COALESCE(
        property_wrapper,
        CASE
            WHEN definition->'type' ? 'integer' THEN 'to_int'
            WHEN definition->'type' ? 'number' THEN 'to_float'
            WHEN definition->'type' ? 'array' THEN 'to_text_array'
            WHEN definition->>'format' IN ('date-time', 'date') THEN 'to_tstz'
            ELSE 'to_text'
        END
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION queryable(
    IN dotpath text,
    IN _collection_ids text[] DEFAULT NULL,
    OUT path text,
    OUT expression text,
    OUT wrapper text,
    OUT nulled_wrapper text,
    OUT definition jsonb,
    OUT registered boolean
) AS $$
    -- An items column is read as itself, anything else through its wrapper; nulled_wrapper is
    -- NULL for the to_text default. Only rows the search can reach: one for every collection
    -- always, a per-collection row when the search names one of its collections. The row for
    -- every collection wins, and per-collection rows that disagree widen to to_text -- one WHERE
    -- clause cannot read a property as a float for one collection and text for another.
    WITH n AS (
        -- An empty name is not a property: it resolves to content->'properties', so a filter
        -- on it would silently compare the entire properties object.
        SELECT strip_properties_prefix(dotpath) AS name, queryable_column(dotpath) AS col
        -- The stripped name, because that is what is resolved: 'properties.' strips to nothing.
        -- 'properties' alone is not stripped at all and named the whole properties object, which
        -- is the same silent whole-object comparison by another spelling.
        WHERE btrim(coalesce(strip_properties_prefix(dotpath), '')) <> ''
          AND btrim(dotpath) <> 'properties'
    ), applicable AS (
        SELECT q.id, q.definition, q.property_wrapper, q.property_path,
               q.collection_ids IS NULL AS global,
               queryable_wrapper(q.property_wrapper, q.definition) AS w,
               queryable_keys(q.name, q.property_path) AS keys
        FROM queryables q, n
        WHERE n.col IS NULL
          AND q.name = n.name
          AND (q.collection_ids IS NULL
            -- '{}' means every collection here as it does in get_queryables and the write
            -- functions; search() produces it for "collections": [].
            OR nullif(_collection_ids, '{}') IS NULL
            OR q.collection_ids && nullif(_collection_ids, '{}'))
    ), pick AS (
        -- One scan of applicable, not three: the same rows answer all three questions.
        SELECT count(DISTINCT a.w) AS wrappers,
               count(DISTINCT a.keys::text) AS paths,
               (array_agg(a.id ORDER BY a.global DESC, a.id))[1] AS id
        FROM applicable a
    ), best AS (
        SELECT a.*, p.wrappers, p.paths
        FROM pick p LEFT JOIN applicable a ON a.id = p.id
    )
    SELECT
        pth.path,
        CASE WHEN n.col IS NULL THEN format('%I(%s)', wr.wrapper, pth.path) ELSE pth.path END,
        CASE WHEN n.col IS NULL THEN wr.wrapper END,
        CASE WHEN n.col IS NULL AND (
                 b.property_wrapper IS NOT NULL
                 OR wr.wrapper <> 'to_text'
                 -- A widened result is a decision, not the to_text default: reported as NULL it
                 -- would let cql2_query infer a wrapper of its own from the literal.
                 OR (b.id IS NOT NULL AND NOT b.global AND b.wrappers > 1)
             ) THEN wr.wrapper END,
        b.definition,
        n.col IS NOT NULL OR b.id IS NOT NULL
    FROM n
    LEFT JOIN best b ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN b.id IS NULL OR b.global OR b.wrappers = 1
            THEN queryable_wrapper(b.property_wrapper, b.definition)
            ELSE 'to_text'
        END AS wrapper
    ) wr
    CROSS JOIN LATERAL (
        SELECT COALESCE(n.col, content_path(
            CASE
                WHEN b.id IS NULL OR b.global OR b.paths = 1
                THEN queryable_keys(n.name, b.property_path)
                ELSE queryable_path_elements(n.name)
            END)) AS path
    ) pth;
$$ LANGUAGE SQL STABLE;


-- Every indexed queryable has one reference index on queryable_index_template, an empty copy of
-- items: for a queryable named after a column of items the copy of the index items carries,
-- otherwise the index maintain_reference_index builds from the row. Partitions are indexed by
-- rewriting the reference index to them.
CREATE OR REPLACE FUNCTION reference_index_method(q queryables) RETURNS text AS $$
    SELECT format(
        'USING %I (%I(%s))',
        lower(q.property_index_type),
        queryable_wrapper(q.property_wrapper, q.definition),
        content_path(queryable_keys(q.name, q.property_path))
    ) WHERE q.property_index_type IS NOT NULL;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The index method for a queryable the caller asked to index without naming one. An array is
-- read through to_text_array and matched with @> and &&, which btree cannot serve.
CREATE OR REPLACE FUNCTION default_index_type(definition jsonb) RETURNS text AS $$
    SELECT CASE WHEN pgstac.queryable_wrapper(NULL, definition) = 'to_text_array'
                THEN 'GIN' ELSE 'BTREE' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- q<id>_<hash of the method>, so a reference index that no longer matches its row is told by
-- name; NULL for a row that asks for no index.
CREATE OR REPLACE FUNCTION reference_index_name(q queryables) RETURNS text AS $$
    SELECT 'q' || q.id || '_' || left(md5(reference_index_method(q)), 8);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION reference_index(q queryables) RETURNS regclass AS $$
    SELECT i.indexrelid
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
    WHERE i.indrelid = to_regclass('pgstac.queryable_index_template')
        AND (c.relname = reference_index_name(q) OR a.attname = q.name);
$$ LANGUAGE SQL STABLE;

-- Brings the reference indexes of one queryable, or of all, in line with the rows. Nothing runs
-- at steady state. The read paths do no DDL, so until maintain_partitions runs they show a stale
-- reference index as unpaired. An index PostgreSQL cannot build fails the row's own write.
-- SECURITY DEFINER: pgstac_ingest owns no table, and the statement is built from the stored row,
-- never from an argument.
CREATE OR REPLACE FUNCTION maintain_reference_index(_queryable_id bigint DEFAULT NULL) RETURNS VOID AS $$
DECLARE
    q queryables;
    stale text;
BEGIN
    FOR stale IN
        SELECT c.relname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        CROSS JOIN LATERAL (SELECT substring(c.relname FROM '^q(\d+)_')::bigint AS id) n
        LEFT JOIN queryables r ON r.id = n.id AND r.property_index_type IS NOT NULL
        WHERE i.indrelid = to_regclass('pgstac.queryable_index_template')
            AND c.relname ~ '^q\d+_[0-9a-f]{8}$'
            AND n.id = COALESCE(_queryable_id, n.id)
            AND c.relname IS DISTINCT FROM reference_index_name(r)
    LOOP
        EXECUTE format('DROP INDEX pgstac.%I', stale);
    END LOOP;
    FOR q IN
        SELECT r.* FROM queryables r
        WHERE r.property_index_type IS NOT NULL
            AND r.id = COALESCE(_queryable_id, r.id)
            AND to_regclass(format('pgstac.%I', reference_index_name(r))) IS NULL
    LOOP
        -- CREATE INDEX is where an unusable access method or wrapper type is caught.
        BEGIN
            EXECUTE format(
                'CREATE INDEX %I ON queryable_index_template %s',
                reference_index_name(q), reference_index_method(q)
            );
        EXCEPTION WHEN OTHERS THEN
            IF _queryable_id IS NOT NULL THEN
                RAISE EXCEPTION '% cannot be indexed: %', q.name, SQLERRM USING ERRCODE = SQLSTATE;
            END IF;
            RAISE WARNING '% cannot be indexed: %', q.name, SQLERRM;
        END;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION queryables_reference_index_triggerfunc() RETURNS TRIGGER AS $$
BEGIN
    -- Two sessions writing different queryable names can deadlock on the shared template.
    -- Retryable, and not serialised here: a lock wide enough to prevent it is held for every
    -- loader transaction.
    PERFORM maintain_reference_index(COALESCE(NEW.id, OLD.id));
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

-- Row level, so these run before the statement level triggers below build the partition indexes.
CREATE TRIGGER queryables_reference_index_insert_trigger AFTER INSERT ON queryables
FOR EACH ROW WHEN (NEW.property_index_type IS NOT NULL)
EXECUTE FUNCTION queryables_reference_index_triggerfunc();

CREATE TRIGGER queryables_reference_index_update_trigger AFTER UPDATE ON queryables
FOR EACH ROW WHEN (
    (OLD.name, OLD.definition, OLD.property_path, OLD.property_wrapper, OLD.property_index_type)
    IS DISTINCT FROM (NEW.name, NEW.definition, NEW.property_path, NEW.property_wrapper, NEW.property_index_type)
)
EXECUTE FUNCTION queryables_reference_index_triggerfunc();

CREATE TRIGGER queryables_reference_index_delete_trigger AFTER DELETE ON queryables
FOR EACH ROW WHEN (OLD.property_index_type IS NOT NULL)
EXECUTE FUNCTION queryables_reference_index_triggerfunc();

-- A deparsed index definition with its name and table replaced by the table given, so a
-- partition's indexes compare with the reference index rewritten to it, and the result runs.
-- Only the header is rewritten; the expression may hold anything, including the index name.
CREATE OR REPLACE FUNCTION indexdef_unnamed(_indexdef text, _tablename text) RETURNS text AS $$
    SELECT format(
        'CREATE %sINDEX ON %I%s',
        CASE WHEN _indexdef ^@ 'CREATE UNIQUE ' THEN 'UNIQUE ' END,
        _tablename,
        substr(_indexdef, strpos(_indexdef, ' USING '))
    );
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Pairs the indexes of the partitions under treeroot with the queryables that want them, by
-- partition and exact definition: both sides are pg_get_indexdef output of one session. An index
-- no queryable pairs with is an orphan (queryable_id NULL); a queryable no index pairs with is
-- missing one (indexname NULL).
CREATE OR REPLACE FUNCTION queryable_indexes(
    IN treeroot text DEFAULT 'items',
    IN changes boolean DEFAULT FALSE,
    OUT collection text,
    OUT partition text,
    OUT field text,
    OUT indexname text,
    OUT existing_idx text,
    OUT queryable_idx text,
    OUT queryable_id bigint
) RETURNS SETOF RECORD AS $$
WITH p AS (
        SELECT
            relid::text as partition,
            partition_collection(
                CASE WHEN parentrelid::regclass::text='items' THEN c.oid ELSE parent.oid END
            ) AS collection
        FROM pg_partition_tree(treeroot)
        JOIN pg_class c ON (relid::regclass = c.oid)
        JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    ), i AS (
        SELECT partition, indexname, indexdef_unnamed(indexdef, partition) AS iidx
        FROM pg_indexes JOIN p ON (tablename = partition)
        WHERE schemaname = 'pgstac'
    ), r AS (
        SELECT q.id, q.name, q.collection_ids, pg_get_indexdef(ri.oid) AS refdef
        FROM queryables q
        CROSS JOIN LATERAL (SELECT reference_index(q) AS oid) ri
        WHERE ri.oid IS NOT NULL
    ), structural AS (
        -- The indexes items itself carries, which every partition has and no queryables row
        -- derives, so they would read as orphans. Matched against items' own indexes rather than
        -- by excluding what a queryables row points at: reference_index matches a column-named
        -- queryable whatever collections it names, so narrowing the id queryable to one would
        -- leave every other partition's _items_N_pk open to dropindexes. Normalised to one fixed
        -- name, which makes the comparison independent of the partition.
        SELECT indexdef_unnamed(pg_get_indexdef(t.indexrelid), 'x') AS sdx
        FROM pg_index t
        WHERE t.indrelid = to_regclass('pgstac.queryable_index_template')
          AND EXISTS (
              SELECT FROM pg_index ii
              WHERE ii.indrelid = to_regclass('pgstac.items')
                AND indexdef_unnamed(pg_get_indexdef(ii.indexrelid), 'x')
                    = indexdef_unnamed(pg_get_indexdef(t.indexrelid), 'x'))
    ), wanted AS (
        SELECT p.collection, p.partition, r.name AS field, indexdef_unnamed(r.refdef, p.partition) AS qidx, r.id AS qid
        FROM r JOIN p ON (r.collection_ids IS NULL)
        UNION ALL
        SELECT p.collection, p.partition, r.name, indexdef_unnamed(r.refdef, p.partition), r.id
        FROM r CROSS JOIN LATERAL unnest(r.collection_ids) c JOIN p ON (p.collection = c)
    ), q AS (
        -- One index per distinct definition on a partition. Two queryables may legitimately
        -- resolve to the same index -- the same keys through the same wrapper -- and without this
        -- each asks for its own, so the partition ends up with byte-identical duplicates that
        -- pair with one another and are never reported as orphans.
        SELECT DISTINCT ON (partition, qidx) collection, partition, field, qidx, qid
        FROM wanted
        ORDER BY partition, qidx, qid
    )
    SELECT
        collection,
        COALESCE(i.partition, q.partition),
        field,
        indexname,
        iidx,
        qidx,
        qid
    FROM i FULL JOIN q ON (i.partition = q.partition AND i.iidx = q.qidx)
    WHERE NOT changes
        OR indexname IS NULL
        -- An unpaired index is only an orphan if it is not one of the structural ones.
        OR (qid IS NULL AND NOT EXISTS (
                SELECT FROM structural
                WHERE structural.sdx = indexdef_unnamed(i.iidx, 'x')));
$$ LANGUAGE SQL;

DROP VIEW IF EXISTS pgstac_indexes_stats;
DROP VIEW IF EXISTS pgstac_indexes;
CREATE VIEW pgstac_indexes AS
SELECT
    'pgstac'::name AS schemaname,
    partition AS tablename,
    indexname,
    pg_get_indexdef(indexname::regclass) AS indexdef,
    existing_idx AS idx,
    field,
    pg_table_size(indexname::text) AS index_size,
    pg_size_pretty(pg_table_size(indexname::text)) AS index_size_pretty
FROM queryable_indexes('items')
WHERE indexname IS NOT NULL;

CREATE VIEW pgstac_indexes_stats AS
SELECT
    i.*,
    n_distinct,
    most_common_vals::text::text[],
    most_common_freqs::text::text[],
    histogram_bounds::text::text[],
    correlation
FROM pgstac_indexes i
LEFT JOIN pg_stats s ON (s.schemaname = i.schemaname AND s.tablename = i.indexname);

-- SECURITY DEFINER, so the index statement is built here from the queryables
-- row; a caller supplied one would run with the privileges of the schema owner.
CREATE OR REPLACE FUNCTION maintain_index(
    _partition text,
    _indexname text,
    _queryable_id bigint,
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
DECLARE
    _queryable_idx text;
BEGIN
    -- Runs elevated, so it may only touch partitions of items.
    IF NOT EXISTS (SELECT 1 FROM partition_catalog_meta(_partition)) THEN
        RETURN;
    END IF;
    IF _indexname IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relnamespace = 'pgstac'::regnamespace
            AND c.relname = _indexname
            AND i.indrelid = partition_oid(_partition)
    ) THEN
        RAISE EXCEPTION '% is not an index on %', _indexname, _partition
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF _indexname IS NULL THEN
        SELECT indexdef_unnamed(pg_get_indexdef(reference_index(q)), _partition) INTO _queryable_idx
        FROM queryables q WHERE q.id = _queryable_id;
        IF _queryable_idx IS NOT NULL THEN
            EXECUTE _queryable_idx;
        END IF;
    ELSIF _queryable_id IS NULL AND dropindexes THEN
        -- The caller's word that this index is unpaired is not taken on trust. This function is
        -- SECURITY DEFINER and pgstac_ingest can execute it, so a caller that simply passed a
        -- NULL queryable id could drop any index on any partition -- including the unique id
        -- index, which is what stops duplicate item ids. Asked of the pairing itself instead.
        IF NOT EXISTS (
            SELECT FROM queryable_indexes(_partition, true) qi
            WHERE qi.indexname = _indexname AND qi.queryable_id IS NULL
        ) THEN
            RAISE EXCEPTION '% on % is not an unpaired index', _indexname, _partition
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        EXECUTE format('DROP INDEX IF EXISTS %I;', _indexname);
    ELSIF rebuildindexes THEN
        EXECUTE format('REINDEX INDEX %I;', _indexname);
    END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


-- A plain run builds missing indexes only; an orphan stays until dropindexes.
CREATE OR REPLACE FUNCTION maintain_partition_queries(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS SETOF text AS $$
    SELECT format(
        'SELECT maintain_index(%L,%L,%L,%L,%L);',
        partition, indexname, queryable_id, dropindexes, rebuildindexes
    )
    FROM queryable_indexes(part, NOT rebuildindexes)
    WHERE queryable_id IS NOT NULL OR dropindexes OR rebuildindexes;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION maintain_partitions(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
    SELECT maintain_reference_index();
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partition_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$$ LANGUAGE SQL;


-- Maintains the partitions of the collections the affected rows name; a global row
-- reaches every collection, so it walks the whole tree.
CREATE OR REPLACE FUNCTION queryables_trigger_func() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM new_rows WHERE collection_ids IS NULL) THEN
        PERFORM maintain_partitions();
    ELSE
        PERFORM maintain_partitions(format('_items_%s', key))
        FROM collections
        WHERE id IN (SELECT unnest(collection_ids) FROM new_rows)
            AND to_regclass(format('pgstac._items_%s', key)) IS NOT NULL;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

-- A transition table is allowed for one event only, hence two triggers.
CREATE TRIGGER queryables_insert_trigger AFTER INSERT ON queryables
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();

CREATE TRIGGER queryables_update_trigger AFTER UPDATE ON queryables
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();


-- The queryables of these collections, or of all when NULL, merged by name; NULL when none of the
-- collections exists.
CREATE OR REPLACE FUNCTION get_queryables(_collection_ids text[] DEFAULT NULL) RETURNS jsonb AS $$
    -- nullif because an empty array means every collection here as it does everywhere else in
    -- this file: upsert_queryable and delete_queryable both read '{}' that way.
    WITH g AS (
        SELECT
            name,
            -- Without the three merged keys: they are re-applied below at their widest, and
            -- leaving them here let the lowest-id row's own constraints through unchanged.
            first_notnull(definition - '{enum,minimum,maximum}'::text[] ORDER BY id) AS definition,
            -- A row that omits a constraint permits everything, so the merge has to omit it as
            -- well. These aggregates skip NULLs, which would answer with the one row that
            -- happened to carry the constraint and reject values another collection allows.
            CASE WHEN count(*) = count(definition->'enum')
                 THEN jsonb_array_unique_merge(definition->'enum' ORDER BY id) END AS enum,
            CASE WHEN count(*) = count(definition->'minimum')
                 THEN jsonb_min(definition->'minimum' ORDER BY id) END AS minimum,
            CASE WHEN count(*) = count(definition->'maximum')
                 THEN jsonb_max(definition->'maximum' ORDER BY id) END AS maximum
        FROM (
            SELECT id, name, coalesce(definition, '{"type":"string"}'::jsonb) AS definition
            FROM queryables
            WHERE collection_ids IS NULL
               OR nullif(_collection_ids, '{}') IS NULL
               OR collection_ids && nullif(_collection_ids, '{}')
        ) q
        GROUP BY name
    )
    SELECT CASE WHEN EXISTS (
        SELECT FROM collections
        WHERE nullif(_collection_ids, '{}') IS NULL
           OR id = ANY(nullif(_collection_ids, '{}'))) THEN
        jsonb_build_object(
            '$schema', 'http://json-schema.org/draft-07/schema#',
            '$id', '',
            'type', 'object',
            'title', 'STAC Queryables.',
            'properties', jsonb_object_agg(
                name,
                definition || jsonb_strip_nulls(jsonb_build_object('enum', enum, 'minimum', minimum, 'maximum', maximum))
            ),
            'additionalProperties', additional_properties()
        )
    END
    FROM g;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables(_collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT get_queryables(CASE WHEN _collection IS NULL THEN NULL ELSE ARRAY[_collection] END);
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables() RETURNS jsonb AS $$
    SELECT get_queryables(NULL::text[]);
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION schema_qualify_refs(url text, j jsonb) returns jsonb as $$
    SELECT replace(j::text, '"$ref": "#', '"$ref": "' || url || '#')::jsonb;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE VIEW stac_extension_queryables AS
SELECT DISTINCT key as name, schema_qualify_refs(e.url, j.value) as definition FROM stac_extensions e, jsonb_each(e.content->'definitions'->'fields'->'properties') j;


CREATE OR REPLACE FUNCTION missing_queryables(_collection text, _tablesample float DEFAULT 5, minrows float DEFAULT 10) RETURNS TABLE(collection text, name text, definition jsonb, property_wrapper text) AS $$
DECLARE
    q text;
    _partition text;
    explain_json json;
    psize float;
BEGIN
    SELECT format('_items_%s', key) INTO _partition FROM collections WHERE id=_collection;
    IF to_regclass(_partition) IS NULL THEN
        RETURN;
    END IF;

    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM %I;', _partition)
    INTO explain_json;
    psize := explain_json->0->'Plan'->'Plan Rows';
    -- Widens the sample until it is expected to hold at least minrows rows.
    _tablesample := least(100, greatest(_tablesample, minrows * 100 / greatest(psize, 1)));
    RAISE DEBUG 'Using tablesample % to find missing queryables from % % that has ~% rows', _tablesample, _collection, _partition, psize;

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
                    COALESCE(s.definition, jsonb_build_object('type', jsonb_typeof(value))) AS definition
                FROM t
                JOIN LATERAL jsonb_each(properties) ON TRUE
                LEFT JOIN q ON (q.name=key)
                LEFT JOIN stac_extension_queryables s ON (s.name=key)
                -- Registered at all, not merely defined: a row with a NULL definition is still a
                -- queryable, and reporting it again feeds a loader a wrapper contradicting its own.
                WHERE q.name IS NULL
            )
            SELECT
                %L,
                key,
                definition,
                queryable_wrapper(NULL, definition)
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
    -- STAC intervals are closed; a date ending one is bumped to the next midnight and left open
    bounds text := '[]';
    empty_reason text;
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
    RAISE DEBUG 'TIMESTRS %', timestrs;
    -- An explicit timestamp is the instant it names, matching temporal_operand: without this the
    -- same spelling is a day in the datetime parameter and an instant in a filter.
    IF jsonb_typeof(_indate) = 'object' AND _indate ? 'timestamp' THEN
        s := (_indate->>'timestamp')::timestamptz;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) = 1 THEN
        IF timestrs[1] ILIKE 'P%' THEN
            RETURN tstzrange(relative_base - upper(timestrs[1])::interval, relative_base, '[)');
        END IF;
        -- A bare date is the whole of that day, everywhere: alone, at either end of an
        -- interval, and as a temporal operand. A value carrying a time is the instant it names.
        s := timestrs[1]::timestamptz;
        IF timestrs[1] ~ '^\d{4}-\d{2}-\d{2}$' THEN
            RETURN tstzrange(s, (timestrs[1]::date + 1)::timestamptz, '[)');
        END IF;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) != 2 THEN
        RAISE EXCEPTION 'Timestamp cannot have more than 2 values';
    END IF;

    IF timestrs[2] ~ '^\d{4}-\d{2}-\d{2}$' THEN
        timestrs[2] := (timestrs[2]::date + 1)::text;
        bounds := '[)';
    END IF;

    IF timestrs[1] = '..' OR timestrs[1] = '' THEN
        s := '-infinity'::timestamptz;
        e := timestrs[2]::timestamptz;
        RETURN tstzrange(s,e,bounds);
    END IF;

    IF timestrs[2] = '..' OR timestrs[2] = '' THEN
        s := timestrs[1]::timestamptz;
        e := 'infinity'::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[1] ILIKE 'P%' AND timestrs[2] NOT ILIKE 'P%' THEN
        e := timestrs[2]::timestamptz;
        s := e - upper(timestrs[1])::interval;
        empty_reason := 'its duration is zero';
    ELSIF timestrs[2] ILIKE 'P%' AND timestrs[1] NOT ILIKE 'P%' THEN
        s := timestrs[1]::timestamptz;
        e := s + upper(timestrs[2])::interval;
        empty_reason := 'its duration is zero';
    ELSE
        s := timestrs[1]::timestamptz;
        e := timestrs[2]::timestamptz;
        empty_reason := 'it ends before it starts';
    END IF;

    -- A one day inversion bumps the high end onto the low one. tstzrange raises when the start
    -- is after the end, but a '[)' range whose ends are equal is EMPTY, and an empty range
    -- renders as a comparison against NULL: zero rows, silently.
    IF bounds = '[)' AND s >= e THEN
        RAISE EXCEPTION 'Datetime range % is empty: %.', _indate, empty_reason;
    END IF;

    RETURN tstzrange(s,e,bounds);
END;
$$ LANGUAGE PLPGSQL STABLE STRICT PARALLEL SAFE SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate text,
    relative_base timestamptz DEFAULT CURRENT_TIMESTAMP
) RETURNS tstzrange AS $$
    SELECT parse_dtrange(to_jsonb(_indate), relative_base);
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;


-- One operand of a temporal predicate as the closed interval [low, high], each end either SQL
-- text (a column or index expression) or a timestamptz literal. An instant is [t, t], a date
-- runs to the last microsecond of its day.
CREATE OR REPLACE FUNCTION temporal_operand(
    IN j jsonb,
    IN inside_interval boolean DEFAULT false,
    IN _collection_ids text[] DEFAULT NULL,
    OUT low text,
    OUT high text,
    OUT low_ts timestamptz,
    OUT high_ts timestamptz
) AS $$
DECLARE
    prop text;
    col text;
    ppath text;
    wrapper text;
    isdate boolean;
    qdef jsonb;
    rrange tstzrange;
    ends jsonb;
    s text;
    d date;
BEGIN
    -- Not STRICT: NULL collection_ids must not null the result.
    IF j IS NULL THEN
        RETURN;
    END IF;
    IF jsonb_typeof(j->'property') = 'string' THEN
        prop := j->>'property';
        -- Resolved here rather than through cql2_query, so it needs the same guard: an empty or
        -- properties-only name yields to_tstz() and fails in the executor.
        IF btrim(coalesce(strip_properties_prefix(prop), '')) = ''
           OR btrim(prop) = 'properties' THEN
            RAISE EXCEPTION 'A property name is required.'
                USING HINT = format('Got %s.', j);
        END IF;
        col := queryable_column(prop);
        IF col IN ('datetime', 'end_datetime') THEN
            low := col;
        ELSIF col IS NOT NULL THEN
            RAISE EXCEPTION 'Property % is not temporal.', prop;
        ELSE
            SELECT q.path, q.wrapper, q.definition->>'format' = 'date', q.definition
              INTO ppath, wrapper, isdate, qdef
              FROM queryable(prop, _collection_ids) q;
            -- A registered property must declare itself temporal; otherwise to_tstz fails per
            -- row, so the error depends on the plan. An unregistered one declares nothing and is
            -- read as a timestamp. {"type":"string"} with no format is what missing_queryables
            -- emits for every string property, so it stays acceptable.
            IF wrapper <> 'to_tstz' AND (
                   wrapper IN ('to_int', 'to_float', 'to_text_array')
                OR (qdef ? 'format' AND qdef->>'format' NOT IN ('date', 'date-time'))
                OR (qdef ? 'type' AND NOT (
                        qdef->>'type' = 'string'
                     OR (jsonb_typeof(qdef->'type') = 'array' AND qdef->'type' ? 'string')))
            ) THEN
                RAISE EXCEPTION 'Property % is not temporal.', prop
                    USING HINT = 'A queryable used as a temporal operand needs a string type, a date or date-time format, or an explicit to_tstz property_wrapper.';
            END IF;
            -- to_tstz is forced rather than taken from the queryable: an unregistered
            -- property's wrapper is to_text, which does not typecheck against a timestamptz
            low := format('to_tstz(%s)', ppath);
            IF isdate THEN
                -- Through UTC, because timestamptz + interval advances by a calendar day in the
                -- session's timezone: across a DST change that day is 23 or 25 hours long.
                high := format(
                    '((%s AT TIME ZONE ''UTC'' + interval ''1 day'' - interval ''1 microsecond'') AT TIME ZONE ''UTC'')',
                    low);
                RETURN;
            END IF;
        END IF;
        high := low;
        RETURN;
    END IF;

    -- CQL2 Example 19: each end of an interval is itself an operand, resolved here recursively.
    -- {"interval": [a, b]}, "a/b" and [a, b] all spell the same interval.
    ends := CASE
        WHEN j ? 'interval' THEN j->'interval'
        WHEN jsonb_typeof(j) = 'array' THEN j
        WHEN jsonb_typeof(j) = 'string' AND j #>> '{}' LIKE '%/%' THEN to_jsonb(string_to_array(j #>> '{}', '/'))
    END;
    IF ends IS NOT NULL THEN
        IF inside_interval OR (j ? 'interval' AND (ends->>0 ILIKE 'P%' OR ends->>1 ILIKE 'P%')) THEN
            RAISE EXCEPTION 'An interval end must be a timestamp, a date or a property.';
        END IF;
        IF jsonb_typeof(ends) != 'array' OR jsonb_array_length(ends) != 2 THEN
            RAISE EXCEPTION 'Temporal interval % must have exactly two ends.', j;
        END IF;
        -- a duration end is left open until the other end, which it is relative to, is known
        IF ends->>0 IN ('..', '') OR ends->>0 ILIKE 'P%' THEN
            low_ts := '-infinity';
        ELSE
            SELECT t.low, t.low_ts INTO low, low_ts FROM temporal_operand(ends->0, true, _collection_ids) t;
        END IF;
        IF ends->>1 IN ('..', '') OR ends->>1 ILIKE 'P%' THEN
            high_ts := 'infinity';
        ELSE
            SELECT t.high, t.high_ts INTO high, high_ts FROM temporal_operand(ends->1, true, _collection_ids) t;
        END IF;
        IF ends->>0 ILIKE 'P%' THEN
            low_ts := high_ts - upper(ends->>0)::interval;
        ELSIF ends->>1 ILIKE 'P%' THEN
            high_ts := low_ts + upper(ends->>1)::interval;
        END IF;
        IF (ends->>0 ILIKE 'P%' OR ends->>1 ILIKE 'P%') AND (isfinite(low_ts) AND isfinite(high_ts)) IS NOT TRUE THEN
            RAISE EXCEPTION 'A duration must be paired with a timestamp or a date.';
        END IF;
        IF low_ts > high_ts THEN
            RAISE EXCEPTION 'Temporal interval % ends before it starts.', j;
        END IF;
        RETURN;
    END IF;

    IF (jsonb_typeof(j) = 'string' AND j #>> '{}' ~ '^\d{4}-\d{2}-\d{2}$') OR jsonb_typeof(j->'date') = 'string' THEN
        d := COALESCE(j->>'date', j #>> '{}')::date;
        low_ts := d;
        high_ts := (d + 1)::timestamptz - interval '1 microsecond';
        RETURN;
    END IF;

    s := CASE WHEN jsonb_typeof(j) = 'string' THEN j #>> '{}' ELSE j->>'timestamp' END;
    IF s IS NULL THEN
        RAISE EXCEPTION 'Temporal operand % is not a timestamp, a date, an interval or a property.', j;
    END IF;
    -- An explicit timestamp is the instant it names, even spelled as a bare date. Every other
    -- spelling of a bare date is the whole day, which is what parse_dtrange returns, so this one
    -- must not go through it.
    IF j ? 'timestamp' THEN
        low_ts := s::timestamptz;
        high_ts := low_ts;
        RETURN;
    END IF;

    rrange := parse_dtrange(to_jsonb(s));
    low_ts := lower(rrange);
    -- high_ts is inclusive, and parse_dtrange returns a half-open range whenever it expanded a
    -- bare date. upper() as-is would make the operand a day plus one microsecond.
    high_ts := CASE WHEN upper_inc(rrange) THEN upper(rrange)
                    ELSE upper(rrange) - interval '1 microsecond' END;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE SET TIME ZONE 'UTC';

-- SQL text for one end of a temporal operand: a literal stays a literal so the planner sees a constant.
CREATE OR REPLACE FUNCTION temporal_end(expr text, ts timestamptz) RETURNS text AS $$
    SELECT CASE WHEN ts IS NOT NULL THEN format('%L::timestamptz', ts) ELSE expr END;
$$ LANGUAGE SQL STABLE SET TIME ZONE 'UTC';


CREATE OR REPLACE FUNCTION temporal_op_query(op text, args jsonb, _collection_ids text[] DEFAULT NULL) RETURNS text AS $$
DECLARE
    l RECORD;
    r RECORD;
    outq text;
BEGIN
    -- Not STRICT: NULL collection_ids must not null the result.
    IF op IS NULL OR args IS NULL THEN
        RETURN NULL;
    END IF;
    RAISE DEBUG 'Constructing temporal query OP: %, ARGS: %', op, args;
    op := lower(op);
    -- every comparison has the first operand's end on its left
    outq := CASE op
        WHEN 't_before'       THEN 'lh < rl'
        WHEN 't_after'        THEN 'll > rh'
        WHEN 't_meets'        THEN 'lh = rl'
        WHEN 't_metby'        THEN 'll = rh'
        WHEN 't_overlaps'     THEN 'll < rl AND lh > rl AND lh < rh'
        WHEN 't_overlappedby' THEN 'll > rl AND ll < rh AND lh > rh'
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
    IF outq IS NULL THEN
        RAISE EXCEPTION 'Temporal operator % is not supported.', op;
    END IF;
    IF args->0 IS NULL OR args->1 IS NULL THEN
        RAISE EXCEPTION 'Temporal operator % requires two operands.', op;
    END IF;
    SELECT * INTO l FROM temporal_operand(args->0, false, _collection_ids);
    SELECT * INTO r FROM temporal_operand(args->1, false, _collection_ids);
    -- Placeholders first, then one format(): an operand that itself contains the text 'rl' can
    -- never be rescanned as a placeholder. The templates above hold nothing but these four names
    -- and operators, so a plain replace cannot match part of anything else.
    outq := replace(replace(replace(replace(
        outq, 'll', '%1$s'), 'lh', '%2$s'), 'rl', '%3$s'), 'rh', '%4$s');
    RETURN format('(' || outq || ')',
        temporal_end(l.low, l.low_ts), temporal_end(l.high, l.high_ts),
        temporal_end(r.low, r.low_ts), temporal_end(r.high, r.high_ts)
    );
END;
$$ LANGUAGE PLPGSQL STABLE;



CREATE OR REPLACE FUNCTION spatial_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    geom text;
    j jsonb := args->1;
BEGIN
    op := lower(op);
    RAISE DEBUG 'Constructing spatial query OP: %, ARGS: %', op, args;
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
    IF geom IS NULL THEN
        RAISE EXCEPTION 'Spatial operand % is not a GeoJSON geometry or a bbox.', j;
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
    ret jsonb;
BEGIN
    RAISE DEBUG 'CQL1_TO_CQL2: %', j;
    IF j ? 'filter' THEN
        RETURN cql1_to_cql2(j->'filter');
    END IF;
    IF jsonb_typeof(j) = 'array' THEN
        SELECT jsonb_agg(cql1_to_cql2(el)) INTO ret FROM jsonb_array_elements(j) el;
        RETURN ret;
    END IF;
    -- scalars, property references, GeoJSON geometries and temporal blocks are literals, not operators
    IF jsonb_typeof(j) != 'object' OR j ?| '{property,type,timestamp,interval}'::text[] THEN
        RETURN j;
    END IF;
    -- every key is an operator whose value is its args; several keys are an implicit AND
    SELECT jsonb_agg(jsonb_build_object(
        'op', key,
        'args', CASE WHEN jsonb_typeof(value) = 'array' THEN cql1_to_cql2(value) ELSE jsonb_build_array(cql1_to_cql2(value)) END
    )) INTO ret FROM jsonb_each(j);
    IF coalesce(jsonb_array_length(ret), 0) <= 1 THEN
        RETURN ret->0;
    END IF;
    RETURN jsonb_build_object('op', 'and', 'args', ret);
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT;

CREATE TABLE cql2_ops (
    op text PRIMARY KEY,
    template text
);



CREATE OR REPLACE FUNCTION cql2_query(j jsonb, wrapper text DEFAULT NULL,
    _collection_ids text[] DEFAULT NULL, _terms_checked boolean DEFAULT false) RETURNS text AS $$
#variable_conflict use_variable
DECLARE
    args jsonb := j->'args';
    arg jsonb;
    op text := lower(j->>'op');
    cql2op RECORD;
    leftarg text;
    rightarg text;
    prop text;
    argdef jsonb;
    declared_wrapper text;
    extra_props bool := pgstac.additional_properties();
BEGIN
    IF j IS NULL THEN
        RETURN NULL;
    END IF;
    IF op IS NOT NULL AND jsonb_typeof(args) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'The % operator requires an array of args.', op;
    END IF;
    RAISE DEBUG 'CQL2_QUERY: %', j;

    -- Once, at the node the caller handed in: $.**.property walks the whole tree, so every
    -- deeper node is already covered and re-running it there only repeats the lookups.
    IF NOT extra_props AND NOT _terms_checked THEN
        FOR prop IN
            SELECT DISTINCT p->>0
            FROM jsonb_path_query(j, 'strict $.**.property') p
        LOOP
            IF NOT (queryable(prop, _collection_ids)).registered THEN
                RAISE EXCEPTION 'Term % is not found in queryables.', prop;
            END IF;
        END LOOP;
    END IF;

    IF j ? 'filter' THEN
        RETURN cql2_query(j->'filter', NULL, _collection_ids, true);
    END IF;

    IF j ? 'upper' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'upper', 'args', jsonb_build_array(j->'upper')), NULL, _collection_ids, true);
    END IF;

    IF j ? 'lower' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'lower', 'args', jsonb_build_array(j->'lower')), NULL, _collection_ids, true);
    END IF;

    -- Temporal Query
    IF op ilike 't_%' or op = 'anyinteracts' THEN
        RETURN temporal_op_query(op, args, _collection_ids);
    END IF;

    -- If property is a timestamp convert it to text to use with
    -- general operators
    IF j ? 'timestamp' THEN
        RETURN format('%L::timestamptz', to_tstz(j->'timestamp'));
    END IF;
    IF j ? 'interval' THEN
        RAISE EXCEPTION 'Please use temporal operators when using intervals.';
    END IF;

    -- Spatial Query
    IF op ilike 's_%' or op = 'intersects' THEN
        RETURN spatial_op_query(op, args);
    END IF;

    IF op IN ('a_equals','a_contains','a_contained_by','a_overlaps') THEN
        IF args->0 ? 'property' THEN
            leftarg := format('to_text_array(%s)', (queryable(args->0->>'property', _collection_ids)).path);
        END IF;
        IF args->1 ? 'property' THEN
            rightarg := format('to_text_array(%s)', (queryable(args->1->>'property', _collection_ids)).path);
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
        RAISE DEBUG 'IN : % % %', args, jsonb_build_array(args->0), args->1;
        IF jsonb_typeof(args->1) IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'The in operator takes a value and an array of values.';
        END IF;
        args := jsonb_build_array(args->0) || (args->1);
        RAISE DEBUG 'IN2 : %', args;
    END IF;



    -- Rebuilding args from its own first three elements cannot change it once the length is
    -- known, so only the check remains.
    IF op = 'between' AND jsonb_array_length(args) <> 3 THEN
        RAISE EXCEPTION 'The between operator takes a value, a lower bound and an upper bound.';
    END IF;

    RAISE DEBUG 'ARGS PRE: %', args;
    IF j ? 'args' THEN
        IF EXISTS (
            SELECT FROM jsonb_array_elements(args) a
            WHERE queryable_column(a->>'property') IN ('datetime', 'end_datetime')
        ) THEN
            -- to_tstz reads its argument as UTC when it carries no offset, and is immutable, so
            -- the literal folds to a fixed instant at plan time instead of being cast at
            -- execution in whatever timezone the session happens to have.
            wrapper := 'to_tstz';
        ELSIF EXISTS (
            SELECT FROM jsonb_array_elements(args) a WHERE queryable_column(a->>'property') IS NOT NULL
        ) THEN
            wrapper := NULL;
        ELSE
            -- if any of the arguments are a property, try to get the property_wrapper
            FOR arg IN SELECT jsonb_path_query(args, '$[*] ? (@.property != null)') LOOP
                RAISE DEBUG 'Arg: %', arg;
                SELECT q.nulled_wrapper, q.definition, q.wrapper
                  INTO wrapper, argdef, declared_wrapper
                  FROM queryable(arg->>'property', _collection_ids) q;
                IF wrapper IS NULL AND (argdef ? 'type' OR argdef ? 'format') THEN
                    -- Declared, so its own type decides rather than the literal's: the number
                    -- heuristic below would otherwise read a declared string as a float. A
                    -- definition that is only a $ref or a title declares nothing, so it is left
                    -- to the heuristic exactly as an unregistered property is.
                    wrapper := declared_wrapper;
                END IF;
                RAISE DEBUG 'Property: %, Wrapper: %', arg, wrapper;
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

        SELECT jsonb_agg(cql2_query(a, wrapper, _collection_ids, true))
            INTO args
        FROM jsonb_array_elements(args) a;
    END IF;
    RAISE DEBUG 'ARGS: %', args;

    IF op IN ('and', 'or') THEN
        RETURN
            format(
                '(%s)',
                array_to_string(to_text_array(args), format(' %s ', upper(op)))
            );
    END IF;

    IF op = 'in' THEN
        RAISE DEBUG 'IN --  % %', args->0, to_text(args->0);
        RETURN format(
            '%s IN (%s)',
            to_text(args->0),
            array_to_string((to_text_array(args))[2:], ',')
        );
    END IF;

    IF op IN ('like', 'ilike', 'not_like', 'not_ilike') THEN
        IF wrapper IS NOT NULL AND wrapper NOT IN ('to_text') THEN
            RAISE EXCEPTION 'The % operator compares text, but its operand is read with %.', op, wrapper
                USING HINT = 'Give the queryable a to_text property_wrapper to match it as text.';
        END IF;
        -- A column operand carries no wrapper, so the check above never sees it. Only id and
        -- collection are text columns; the rest fail in the executor.
        IF EXISTS (
            SELECT FROM jsonb_array_elements(j->'args') a
            WHERE queryable_column(a->>'property') IS NOT NULL
              AND queryable_column(a->>'property') NOT IN ('id', 'collection')
        ) THEN
            RAISE EXCEPTION 'The % operator compares text, but its operand is not a text column.', op;
        END IF;
    END IF;

    -- Look up template from cql2_ops
    IF j ? 'op' THEN
        SELECT * INTO cql2op FROM cql2_ops WHERE cql2_ops.op = op;
        IF FOUND THEN
            IF jsonb_array_length(args) >
               (length(cql2op.template) - length(replace(cql2op.template, '%s', ''))) / 2 THEN
                RAISE EXCEPTION 'The % operator was given % arguments, more than it takes.',
                    op, jsonb_array_length(args);
            END IF;
            RETURN format(
                cql2op.template,
                VARIADIC (to_text_array(args))
            );
        ELSE
            RAISE EXCEPTION 'Operator % Not Supported.', op;
        END IF;
    END IF;


    -- A property with no name cannot be resolved; emitting it anyway yields SQL like to_text(),
    -- which fails in the executor rather than here, where the cause is visible.
    IF j ? 'property' AND (
           btrim(coalesce(strip_properties_prefix(j->>'property'), '')) = ''
        OR btrim(coalesce(j->>'property', '')) = 'properties') THEN
        RAISE EXCEPTION 'A property name is required.'
            USING HINT = format('Got %s.', j);
    END IF;

    IF j ? 'property' THEN
        -- A column of items is already the right type; only the literals beside it need the
        -- wrapper that fixes how they are read.
        IF wrapper IS NULL OR queryable_column(j->>'property') IS NOT NULL THEN
            RETURN (queryable(j->>'property', _collection_ids)).path;
        END IF;
        RETURN format('%I(%s)', wrapper, (queryable(j->>'property', _collection_ids)).path);
    ELSIF wrapper IS NOT NULL THEN
        RETURN format('%I(%L)', wrapper, j);
    END IF;

    RETURN quote_literal(to_text(j));
END;
$$ LANGUAGE PLPGSQL STABLE;
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

-- Never written: it carries the reference index of every indexed queryable, the copies of the
-- indexes above and the unique id index among them (see 002a_queryables).
CREATE TABLE IF NOT EXISTS queryable_index_template (LIKE items INCLUDING INDEXES);
CREATE UNIQUE INDEX IF NOT EXISTS queryable_index_template_id_idx ON queryable_index_template (id);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION partition_after_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p text;
    t timestamptz := clock_timestamp();
BEGIN
    RAISE DEBUG 'Updating partition stats %', t;
    -- Ordered: each iteration holds a partition_stats row lock until commit.
    FOR p IN SELECT DISTINCT partition
        FROM newdata n JOIN partition_stats p
        ON (n.collection=p.collection AND n.datetime <@ p.partition_dtrange)
        ORDER BY 1
    LOOP
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L, %L);', p, true));
    END LOOP;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        DELETE FROM format_item_cache c USING newdata n WHERE c.collection = n.collection AND c.id = n.id;
    END IF;
    RAISE DEBUG 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

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
    SELECT (strip_jsonb(_item - '{id,geometry,collection,type,pgstac:base_item}'::text[], b.base_item)
                - '{id,geometry,collection,type}'::text[])
           || jsonb_strip_nulls(jsonb_build_object('pgstac:base_item', b.base_item_id))
    FROM current_base_item(_item->>'collection') b;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

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



CREATE OR REPLACE FUNCTION content_hydrate(_item items, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    content jsonb;
    base_item jsonb;
    tag text;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    -- The tag is validated rather than cast inside a BEGIN ... EXCEPTION block. A block with an
    -- exception handler opens a subtransaction on EVERY call, and this runs once per returned
    -- item; the guard costs a regex instead. Nine digits always fit in an int, so a tag that
    -- passes cannot overflow the cast. A tag that fails falls through to the warning below.
    tag := _item.content->>'pgstac:base_item';
    IF tag IS NULL OR tag ~ '^\s*\d{1,9}\s*$' THEN
        base_item := collection_base_item(_item.collection, tag::int);
    END IF;
    IF base_item IS NULL THEN
        RAISE WARNING 'Item % in collection % is tagged with base item %, which does not exist; hydrating against the current base item.',
            _item.id, _item.collection, tag;
        SELECT c.base_item INTO base_item FROM collections c WHERE c.id = _item.collection;
    END IF;
    content := jsonb_build_object(
        'id', _item.id,
        'geometry', geom,
        'collection', _item.collection,
        'type', 'Feature'
    ) || (_item.content - 'pgstac:base_item');
    RETURN content_hydrate(content, base_item, fields);
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    output jsonb;
    base_item jsonb;
    tag text;
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
    -- The base item itself, not the row id it is stored as, and emitted for every item: an
    -- untagged item hydrates against the collection's FIRST base item, so a client seeing no
    -- key would use the current one and get different content than search() returns.
    tag := output->>'pgstac:base_item';
    IF tag IS NULL OR tag ~ '^\s*\d{1,9}\s*$' THEN
        base_item := collection_base_item(_item.collection, tag::int);
    END IF;
    IF base_item IS NULL AND tag IS NOT NULL THEN
        RAISE WARNING 'Item % in collection % is tagged with base item %, which does not exist; returning the current base item.',
            _item.id, _item.collection, tag;
        SELECT c.base_item INTO base_item FROM collections c WHERE c.id = _item.collection;
    END IF;
    output := output || jsonb_build_object('pgstac:base_item', base_item);
    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

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
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
BEGIN
    RAISE DEBUG 'Creating Partitions. %', clock_timestamp() - ts;

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
    -- Ordered: check_partition holds DDL and row locks until commit.
    ) SELECT check_partition(collection, dtrange, edtrange) FROM (
        SELECT * FROM p ORDER BY collection, d
    ) ordered LOOP
        RAISE DEBUG 'Partition %', part;
    END LOOP;

    RAISE DEBUG 'Creating temp table with data to be added. %', clock_timestamp() - ts;
    DROP TABLE IF EXISTS tmpdata;
    -- LATERAL so content_dehydrate runs once per row.
    CREATE TEMP TABLE tmpdata ON COMMIT DROP AS
    SELECT d.* FROM newdata n, LATERAL content_dehydrate(n.content) d;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE DEBUG 'Added % rows to tmpdata. %', nrows, clock_timestamp() - ts;

    RAISE DEBUG 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT * FROM tmpdata;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        -- Locked in a fixed order first, so concurrent upserts over an
        -- overlapping id set cannot deadlock. A bare DELETE gives no ordering;
        -- ORDER BY ... FOR UPDATE does, because LockRows sits above the sort.
        WITH locked AS (
            SELECT o.collection, o.id
            FROM tmpdata s
                JOIN items o ON (o.id = s.id AND o.collection = s.collection)
            WHERE o IS DISTINCT FROM s
            ORDER BY o.collection, o.id
            FOR UPDATE OF o
        )
        DELETE FROM items i
        USING locked l
        WHERE i.collection = l.collection AND i.id = l.id
        ;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;
        INSERT INTO items AS t
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    END IF;

    RAISE DEBUG 'Deleting data from staging table. %', clock_timestamp() - ts;
    EXECUTE format('DELETE FROM %I', TG_TABLE_NAME);
    RAISE DEBUG 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
-- UTC, matching partition_name: the date_trunc here groups rows for check_partition.
$$ LANGUAGE PLPGSQL SET TIME ZONE 'UTC';


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
    SELECT to_jsonb(array[array[min(datetime), max(datetime)]])
    FROM items WHERE collection=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;
-- collection and partition_dtrange describe the partition itself and are
-- written synchronously by check_partition. dtrange, edtrange and spatial
-- describe the data in the partition and may be updated asynchronously
-- through the query queue.
CREATE TABLE partition_stats (
    partition text PRIMARY KEY,
    collection text,
    partition_dtrange tstzrange,
    dtrange tstzrange,
    edtrange tstzrange,
    spatial geometry,
    last_updated timestamptz,
    keys text[]
) WITH (FILLFACTOR=90);

CREATE INDEX partition_stats_collection_idx ON partition_stats (collection);
-- GiST so partition_name's containment lookup is an index condition on both columns; range
-- containment is not a btree operator. btree_gist is required by 000_idempotent_pre.
CREATE INDEX IF NOT EXISTS partition_stats_collection_dtrange_idx
    ON partition_stats USING GIST (collection, partition_dtrange);


-- Reads what partition_bound_expr renders, which is pinned to ISO. Any other spelling
-- returns NULL, which callers read as unbounded.
CREATE OR REPLACE FUNCTION constraint_tstzrange(expr text) RETURNS tstzrange AS $$
    WITH t AS (
        SELECT regexp_matches(
            expr,
            E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
        ) AS m
    ) SELECT tstzrange(m[1]::timestamptz, m[2]::timestamptz) FROM t
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION partition_bound(_oid oid) RETURNS tstzrange AS $$
    SELECT pgstac.constraint_tstzrange(pgstac.partition_bound_expr(_oid));
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_tstz_constraint(reloid oid, colname text) RETURNS tstzrange AS $$
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
    FOR m IN SELECT regexp_matches(expr, '[ (]' || colname || $expr$\s*([<>=]{1,2})\s*'([0-9 :.+\-]+)'$expr$, 'g') LOOP
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
$$ LANGUAGE plpgsql STRICT STABLE SET DateStyle TO 'ISO, YMD';

CREATE OR REPLACE FUNCTION get_partition_name(relid regclass) RETURNS text AS $$
    SELECT (parse_ident(relid::text))[cardinality(parse_ident(relid::text))];
$$ LANGUAGE SQL STABLE STRICT;

-- Resolve a partition name, bare or pgstac qualified, against pgstac rather
-- than the search_path. NULL if there is no such table, or if the name is
-- qualified with a different schema.
CREATE OR REPLACE FUNCTION partition_oid(_partition text) RETURNS oid AS $$
    SELECT CASE
        WHEN cardinality(parts) > 1 AND parts[cardinality(parts) - 1] <> 'pgstac' THEN NULL
        ELSE to_regclass(format('pgstac.%I', parts[cardinality(parts)]))
    END
    FROM parse_ident(_partition) AS parts;
$$ LANGUAGE SQL STABLE STRICT;

-- Catalog metadata for one partition, touching one relation. pg_partition_tree
-- would lock every partition in the tree. No rows unless _partition is a leaf
-- partition of items.
CREATE OR REPLACE FUNCTION partition_catalog_meta(_partition text)
RETURNS TABLE (
    collection text,
    partition_dtrange tstzrange,
    constraint_dtrange tstzrange,
    constraint_edtrange tstzrange
) AS $$
DECLARE
    _oid oid;
    _parent oid;
    _collection text;
    _inf tstzrange := tstzrange('-infinity', 'infinity', '[]');
    _dtrange tstzrange;
BEGIN
    _oid := partition_oid(_partition);
    -- Leaves only, matching partitions_view: an intermediate partition has no
    -- meaningful range of its own.
    IF _oid IS NULL OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = _oid) THEN
        RETURN;
    END IF;

    SELECT inhparent INTO _parent FROM pg_inherits WHERE inhrelid = _oid;

    -- Partitions of items only. The SECURITY DEFINER functions below use this
    -- to decide what they may alter, so any other relation must return nothing.
    IF _parent IS NULL
        OR (
            _parent <> 'pgstac.items'::regclass
            AND NOT EXISTS (
                SELECT 1 FROM pg_inherits
                WHERE inhrelid = _parent AND inhparent = 'pgstac.items'::regclass
            )
        )
    THEN
        RETURN;
    END IF;

    -- A partition of a sub-partitioned collection carries a datetime range
    -- bound; its collection is on the parent. A direct partition of items
    -- carries the collection itself.
    _collection := partition_collection(
        CASE WHEN _parent = 'pgstac.items'::regclass THEN _oid ELSE _parent END
    );
    _dtrange := COALESCE(partition_bound(_oid), _inf);

    RETURN QUERY SELECT
        _collection,
        _dtrange,
        COALESCE(get_tstz_constraint(_oid, 'datetime'), _dtrange, _inf),
        COALESCE(get_tstz_constraint(_oid, 'end_datetime'), _inf);
END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE VIEW partition_sys_meta AS
SELECT
    partition,
    -- pg_class.relpartbound is declared COLLATE "C" and the old inline pg_get_expr inherited
    -- it. Reading it through a function taking oid does not, and CREATE OR REPLACE VIEW
    -- cannot change a column's collation.
    partition_collection(CASE WHEN level = 1 THEN c.oid ELSE parent.oid END)
        COLLATE "C" AS collection,
    level,
    c.reltuples,
    c.relhastriggers,
    partition_dtrange,
    COALESCE(
        get_tstz_constraint(c.oid, 'datetime'),
        partition_dtrange,
        inf_range
    ) as constraint_dtrange,
    COALESCE(
        get_tstz_constraint(c.oid, 'end_datetime'),
        inf_range
    ) as constraint_edtrange
FROM
    pg_partition_tree('items')
    JOIN pg_class c ON (relid::regclass = c.oid)
    JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    JOIN LATERAL get_partition_name(relid) AS partition ON TRUE
    JOIN LATERAL tstzrange('-infinity', 'infinity','[]') as inf_range ON TRUE
    JOIN LATERAL COALESCE(partition_bound(c.oid), inf_range) as partition_dtrange ON TRUE
WHERE isleaf
;

-- partition_sys_meta plus the statistics tracked alongside each partition.
CREATE OR REPLACE VIEW partitions_view AS
SELECT
    sm.*,
    ps.dtrange,
    ps.edtrange,
    ps.spatial,
    ps.last_updated
FROM partition_sys_meta sm
    LEFT JOIN pgstac.partition_stats ps USING (partition)
;

CREATE VIEW partitions AS
SELECT * FROM partitions_view;


-- Returns TRUE if the stats update ran, FALSE if it was queued.
CREATE OR REPLACE FUNCTION update_partition_stats_q(_partition text, istrigger boolean default false) RETURNS boolean AS $$
DECLARE
BEGIN
    RETURN run_or_queue(
        format('SELECT update_partition_stats(%L, %L);', _partition, istrigger)
    );
END;
$$ LANGUAGE PLPGSQL;

-- _extent NULL follows pgstac.update_collection_extent; TRUE computes the
-- spatial extent regardless, for callers that are about to read it.
CREATE OR REPLACE FUNCTION update_partition_stats(
    _partition text,
    istrigger boolean default false,
    _extent boolean default NULL
) RETURNS VOID AS $$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    cdtrange tstzrange;
    cedtrange tstzrange;
    extent geometry;
    collection text;
    pdtrange tstzrange;
    auto_extent boolean := get_setting_bool('update_collection_extent');
    do_extent boolean := COALESCE(_extent, auto_extent);
BEGIN
    -- Cannot be STRICT: _extent is three valued, and STRICT would skip the
    -- body whenever it is NULL.
    IF _partition IS NULL OR istrigger IS NULL THEN
        RETURN;
    END IF;
    RAISE DEBUG 'Updating stats for %.', _partition;

    SELECT m.collection, m.partition_dtrange, m.constraint_dtrange, m.constraint_edtrange
        INTO collection, pdtrange, cdtrange, cedtrange
    FROM partition_catalog_meta(_partition) m;

    -- A queued update can outlive the partition it names.
    IF NOT FOUND THEN
        RAISE NOTICE 'Partition % no longer exists, skipping stats update.', _partition;
        RETURN;
    END IF;

    -- Taken before the partition is read. The constraint rebuild below needs
    -- ACCESS EXCLUSIVE, and escalating to that mid-transaction deadlocks
    -- against another session doing the same. SHARE UPDATE EXCLUSIVE conflicts
    -- with itself but not with readers.
    IF NOT istrigger THEN
        EXECUTE format('LOCK TABLE %I IN SHARE UPDATE EXCLUSIVE MODE', _partition);
    END IF;

    -- The observed ranges feed the constraint tightening below and collection
    -- extents. When neither will read them, only the partition's identity is
    -- written, which is what keeps it visible to search.
    IF NOT istrigger OR do_extent THEN
        -- st_extent visits every geometry, so it is only run when wanted.
        IF do_extent THEN
            EXECUTE format(
                $q$
                    SELECT
                        tstzrange(min(datetime), max(datetime),'[]'),
                        tstzrange(min(end_datetime), max(end_datetime), '[]'),
                        st_extent(geometry)::geometry
                    FROM %I
                $q$,
                _partition
            ) INTO dtrange, edtrange, extent;
            RAISE DEBUG 'Extent: %', extent;
        ELSE
            EXECUTE format(
                $q$
                    SELECT
                        tstzrange(min(datetime), max(datetime),'[]'),
                        tstzrange(min(end_datetime), max(end_datetime), '[]')
                    FROM %I
                $q$,
                _partition
            ) INTO dtrange, edtrange;
        END IF;

        INSERT INTO partition_stats
            (partition, collection, partition_dtrange, dtrange, edtrange, spatial, last_updated)
            VALUES (_partition, collection, pdtrange, dtrange, edtrange, extent, now())
            ON CONFLICT (partition) DO
                UPDATE SET
                    collection=EXCLUDED.collection,
                    partition_dtrange=EXCLUDED.partition_dtrange,
                    dtrange=EXCLUDED.dtrange,
                    edtrange=EXCLUDED.edtrange,
                    spatial=COALESCE(EXCLUDED.spatial, partition_stats.spatial),
                    last_updated=EXCLUDED.last_updated
        ;
    ELSE
        INSERT INTO partition_stats (partition, collection, partition_dtrange)
            VALUES (_partition, collection, pdtrange)
            ON CONFLICT (partition) DO UPDATE
                SET collection = EXCLUDED.collection,
                    partition_dtrange = EXCLUDED.partition_dtrange
                WHERE
                    partition_stats.collection IS DISTINCT FROM EXCLUDED.collection
                    OR partition_stats.partition_dtrange IS DISTINCT FROM EXCLUDED.partition_dtrange
        ;
    END IF;

    RAISE DEBUG 'Checking if we need to modify constraints...';
    RAISE DEBUG 'cdtrange: % dtrange: % cedtrange: % edtrange: %',cdtrange, dtrange, cedtrange, edtrange;
    IF
        (cdtrange IS DISTINCT FROM dtrange OR edtrange IS DISTINCT FROM cedtrange)
        AND NOT istrigger
    THEN
        RAISE DEBUG 'Modifying Constraints';
        RAISE DEBUG 'Existing % %', cdtrange, cedtrange;
        RAISE DEBUG 'New      % %', dtrange, edtrange;
        PERFORM drop_table_constraints(_partition);
        PERFORM create_table_constraints(_partition, dtrange, edtrange);
    END IF;
    -- auto_extent, not do_extent: a caller that passed _extent aggregates the
    -- extent itself, and update_collection_extents would then be updating
    -- collections from inside its own UPDATE of collections.
    RAISE DEBUG 'Checking if we need to update collection extents.';
    IF auto_extent THEN
        RAISE DEBUG 'updating collection extent for %', collection;
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
        RAISE DEBUG 'Not updating collection extent for %', collection;
    END IF;

END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;



CREATE OR REPLACE FUNCTION partition_name( IN collection text, IN dt timestamptz, OUT partition_name text, OUT partition_range tstzrange) AS $$
DECLARE
    c RECORD;
    parent_name text;
BEGIN
    -- check_partition records collection and partition_dtrange synchronously, so one indexed
    -- lookup answers every ingest into an existing partition. It also settles alignment: a
    -- catalog partitioned by a non-UTC session keeps the bounds it already has.
    SELECT ps.partition, ps.partition_dtrange
    INTO partition_name, partition_range
    FROM pgstac.partition_stats ps
    WHERE ps.collection = partition_name.collection
        AND ps.partition_dtrange @> dt
    LIMIT 1;
    IF partition_name IS NOT NULL THEN
        RETURN;
    END IF;

    -- Only when no partition covers it, which is the path that goes on to create one.
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
-- UTC so a new partition's boundary does not depend on who loaded the item.
$$ LANGUAGE PLPGSQL STABLE SET TIME ZONE 'UTC';


CREATE OR REPLACE FUNCTION drop_table_constraints(t text) RETURNS text AS $$
DECLARE
    q text;
    _oid oid := partition_oid(t);
BEGIN
    IF _oid IS NULL THEN
        RETURN NULL;
    END IF;
    -- Only partitions of items. This runs elevated, so without the check it
    -- would alter any table pgstac_admin owns that the caller names.
    IF NOT EXISTS (SELECT 1 FROM partition_catalog_meta(t)) THEN
        RETURN NULL;
    END IF;
    -- Reduce to the bare name so the ALTER statements below quote it correctly
    -- even when the caller passed a schema qualified name.
    t := get_partition_name(_oid);
    FOR q IN SELECT FORMAT(
        $q$
            ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        $q$,
        t,
        conname
    ) FROM pg_constraint
        WHERE conrelid=_oid AND contype='c'
    LOOP
        EXECUTE q;
    END LOOP;
    RETURN t;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION create_table_constraints(t text, _dtrange tstzrange, _edtrange tstzrange) RETURNS text AS $$
DECLARE
    q text;
    _oid oid := partition_oid(t);
BEGIN
    IF _oid IS NULL THEN
        RETURN NULL;
    END IF;
    -- Only partitions of items. This runs elevated, so without the check it
    -- would alter any table pgstac_admin owns that the caller names.
    IF NOT EXISTS (SELECT 1 FROM partition_catalog_meta(t)) THEN
        RETURN NULL;
    END IF;
    -- Reduce to the bare name so the ALTER statements below quote it correctly
    -- even when the caller passed a schema qualified name.
    t := get_partition_name(_oid);
    RAISE DEBUG 'Creating Table Constraints for % % %', t, _dtrange, _edtrange;
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
    -- Run, not queued: the queue runner is not a definer, so queued DDL
    -- executes as whoever drains it. Defer by queueing a call to this function.
    EXECUTE q;
    RETURN t;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


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
    err_context text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;

    -- Through partition_name rather than repeating its arithmetic: it buckets in UTC and returns
    -- an existing partition with the bounds it actually has.
    SELECT p.partition_name, p.partition_range
    INTO _partition_name, _partition_dtrange
    FROM pgstac.partition_name(_collection, lower(_dtrange)) p;

    IF NOT _partition_dtrange @> _dtrange THEN
        RAISE EXCEPTION 'dtrange % is greater than the partition size % for collection %', _dtrange, c.partition_trunc, _collection;
    END IF;

    -- Constraint ranges are maintained asynchronously, so they come from the
    -- catalog rather than partition_stats.
    SELECT ps.partition, m.constraint_dtrange, m.constraint_edtrange
        INTO pm
    FROM partition_stats ps
        JOIN LATERAL partition_catalog_meta(ps.partition) m ON TRUE
    WHERE ps.collection = _collection AND ps.partition_dtrange @> _dtrange
    LIMIT 1;
    IF FOUND THEN
        RAISE DEBUG '% % %', _edtrange, _dtrange, pm;
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
    RAISE DEBUG 'EXISTING CONSTRAINTS % %, NEW % %', pm.constraint_dtrange, pm.constraint_edtrange, _constraint_dtrange, _constraint_edtrange;
    RAISE DEBUG 'Creating partition % %', _partition_name, _partition_dtrange;
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
            RAISE DEBUG 'Partition % already exists.', _partition_name;
        WHEN others THEN
            GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
            RAISE INFO 'Error Name:%',SQLERRM;
            RAISE INFO 'Error State:%', SQLSTATE;
            RAISE INFO 'Error Context:%', err_context;
    END;
    -- The _constraint_ ranges are the union of the existing constraint and the
    -- incoming batch, so they hold for rows already present as well as the ones
    -- about to be added. Queueable: rebuilding validates the partition under an
    -- ACCESS EXCLUSIVE lock.
    PERFORM run_or_queue(format(
        'SELECT create_table_constraints(%L, %L, %L);',
        _partition_name,
        _constraint_dtrange,
        _constraint_edtrange
    ));
    PERFORM maintain_partitions(_partition_name);
    -- Search finds partitions through partition_stats, so the row has to exist
    -- before this transaction commits. A queued stats update has not written it.
    IF NOT update_partition_stats_q(_partition_name, true) THEN
        INSERT INTO partition_stats (partition, collection, partition_dtrange)
            VALUES (_partition_name, _collection, _partition_dtrange)
            ON CONFLICT (partition) DO UPDATE
                SET collection = EXCLUDED.collection,
                    partition_dtrange = EXCLUDED.partition_dtrange
                WHERE
                    partition_stats.collection IS DISTINCT FROM EXCLUDED.collection
                    OR partition_stats.partition_dtrange IS DISTINCT FROM EXCLUDED.partition_dtrange
        ;
    END IF;
    RETURN _partition_name;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


CREATE OR REPLACE FUNCTION repartition(_collection text, _partition_trunc text, triggered boolean DEFAULT FALSE) RETURNS text AS $$
DECLARE
    c RECORD;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;
    IF triggered THEN
        RAISE DEBUG 'Converting % to % partitioning via Trigger', _collection, _partition_trunc;
    ELSE
        RAISE DEBUG 'Converting % from using % to % partitioning', _collection, c.partition_trunc, _partition_trunc;
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
                DELETE FROM partition_stats WHERE collection = %L;
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
            _collection,
            c.partition_trunc,
            c.partition_trunc
        );
    END IF;
    RETURN _collection;
END;
-- UTC: a repartition rebuilds every partition of the collection, so it is the one point where an
-- existing catalog's alignment can be corrected without rewriting anything unasked for.
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION collections_trigger_func() RETURNS TRIGGER AS $$
BEGIN
    RAISE DEBUG 'Collection Trigger. % %', NEW.id, NEW.key;
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS DISTINCT FROM OLD.partition_trunc THEN
        PERFORM repartition(NEW.id, NEW.partition_trunc, TRUE);
    END IF;
    -- The first edit also records the old base item, which untagged items use.
    IF TG_OP = 'UPDATE' AND NEW.base_item IS DISTINCT FROM OLD.base_item THEN
        IF NOT EXISTS (SELECT 1 FROM base_items WHERE collection = NEW.id) THEN
            INSERT INTO base_items (collection, base_item) VALUES (NEW.id, OLD.base_item);
        END IF;
        INSERT INTO base_items (collection, base_item) VALUES (NEW.id, NEW.base_item);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;


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
    RAISE DEBUG 'EXPLAIN: %', explain;

    RETURN QUERY
    WITH t AS (
        SELECT j->>0 as p FROM
            jsonb_path_query(
                explain,
                'strict $.**."Relation Name" ? (@ != null)'
            ) j
    ),
    parts AS (
        -- = ANY(array) uses the partition_stats primary key; the planner
        -- cannot estimate jsonb_path_query, so a join plans as a full scan.
        SELECT
            date_trunc('month', lower(partition_dtrange)) as sdate,
            date_trunc('month', upper(partition_dtrange)) + '1 month'::interval as edate
        FROM partition_stats
        WHERE
            partition = ANY (ARRAY(SELECT p FROM t))
            AND partition_dtrange IS NOT NULL
            AND partition_dtrange != 'empty'::tstzrange
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
    IN _orderby text DEFAULT 'datetime DESC, collection DESC, id DESC',
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
RAISE DEBUG 'Getting chunks for % %', _where, _orderby;
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

-- Volatile like partition_queries, which reads the partitions as they stand.
CREATE OR REPLACE FUNCTION partition_query_view(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, collection DESC, id DESC',
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
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION q_to_tsquery (jinput jsonb)
    RETURNS tsquery
    AS $$
DECLARE
    input text;
    processed_text text;
    temp_text text;
    quote_array text[];
    placeholder text := '@QUOTE@';
BEGIN
    IF jsonb_typeof(jinput) = 'string' THEN
        input := jinput->>0;
    ELSIF jsonb_typeof(jinput) = 'array' THEN
        input := array_to_string(
            array(select jsonb_array_elements_text(jinput)),
            ' OR '
        );
    ELSE
        RAISE EXCEPTION 'Input must be a string or an array of strings.';
    END IF;
    -- The placeholder has to be made of term characters, so it cannot be made unspellable.
    -- An input that contains it would be substituted for a phrase it never wrote.
    IF position(placeholder in input) > 0 THEN
        RAISE EXCEPTION 'Free text query may not contain %', placeholder;
    END IF;

    -- Extract all quoted phrases and store in array. ARRAY(...) because regexp_matches with
    -- the g flag returns a set, and assigning a set to a scalar fails on the second match.
    quote_array := ARRAY(SELECT m[1] FROM regexp_matches(input, '"[^"]*"', 'g') m);

    -- Replace each quoted part with a unique placeholder if there are any quoted phrases
    IF array_length(quote_array, 1) IS NOT NULL THEN
        processed_text := input;
        FOR i IN array_lower(quote_array, 1) .. array_upper(quote_array, 1) LOOP
            processed_text := replace(processed_text, quote_array[i], placeholder || i || placeholder);
        END LOOP;
    ELSE
        processed_text := input;
    END IF;

    -- Replace non-quoted text using regular expressions

    -- , -> |
    processed_text := regexp_replace(processed_text, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', ' | ', 'g');

    -- and -> &
    processed_text := regexp_replace(processed_text, '\s+AND\s+', ' & ', 'gi');

    -- or -> |
    processed_text := regexp_replace(processed_text, '\s+OR\s+', ' | ', 'gi');

    -- + ->
    processed_text := regexp_replace(processed_text, '^\s*\+([a-zA-Z0-9_@]+)', '\1', 'g'); -- +term at start
    processed_text := regexp_replace(processed_text, '\s+\+([a-zA-Z0-9_@]+)', ' & \1', 'g'); -- +term elsewhere, whitespace required so that foo+bar stays one word

    -- - ->  !
    processed_text := regexp_replace(processed_text, '^\s*\-([a-zA-Z0-9_@]+)', '! \1', 'g'); -- -term at start
    processed_text := regexp_replace(processed_text, '\s+\-([a-zA-Z0-9_@]+)', ' & ! \1', 'g'); -- -term elsewhere, whitespace required so that foo-bar stays one word

    -- a +/- term following an operator would otherwise double the operator
    processed_text := regexp_replace(processed_text, '([&|])\s*&\s*(!?)', '\1 \2', 'g');

    -- terms separated with spaces are assumed to represent adjacent terms. loop through these
    -- occurrences and replace them with the adjacency operator (<->)
    LOOP
        -- The placeholder standing in for a quoted phrase counts as a term here, or no adjacency
        -- operator is inserted beside it and the result is not a valid tsquery.
        temp_text := regexp_replace(processed_text, '([a-zA-Z0-9_@]+)\s+([a-zA-Z0-9_@]+)(?!\s*[&|<>])', '\1 <-> \2', 'g');
        IF temp_text = processed_text THEN
            EXIT; -- No more replacements were made
        END IF;
        processed_text := temp_text;
    END LOOP;


    -- Replace placeholders back with quoted phrases if there were any
    IF array_length(quote_array, 1) IS NOT NULL THEN
        FOR i IN array_lower(quote_array, 1) .. array_upper(quote_array, 1) LOOP
            processed_text := replace(processed_text, placeholder || i || placeholder, '''' || substring(quote_array[i] from 2 for length(quote_array[i]) - 2) || '''');
        END LOOP;
    END IF;

    RETURN to_tsquery('english', processed_text);
END;
$$
LANGUAGE plpgsql;


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
    ft_query tsquery;
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

        where_segments := where_segments || format(' datetime %s %L::timestamptz AND end_datetime >= %L::timestamptz ',
            CASE WHEN upper_inc(dtrange) THEN '<=' ELSE '<' END,
            edate,
            sdate
        );
    END IF;

    IF j ? 'q' THEN
        ft_query := q_to_tsquery(j->'q');
        where_segments := where_segments || format(
            $quote$
            (
                to_tsvector('english', content->'properties'->>'description') ||
                to_tsvector('english', coalesce(content->'properties'->>'title', '')) ||
                to_tsvector('english', coalesce(content->'properties'->>'keywords', ''))
            ) @@ %L
            $quote$,
            ft_query
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
    RAISE DEBUG 'FILTER: %', filter;
    where_segments := where_segments || cql2_query(filter, NULL, collections);
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
DECLARE
    d text := btrim(coalesce(_dir, ''));
BEGIN
    -- The whole word, not a prefix: 'desc%' accepts anything merely beginning with desc. An
    -- unrecognised direction raises rather than reading as ASC, where a typo silently reverses
    -- half a result set.
    IF d <> '' AND d !~* '^(asc|desc)(ending)?$' THEN
        RAISE EXCEPTION 'Invalid sortby direction %: must be asc or desc', _dir;
    END IF;
    -- boolean <> is xor: reverse flips whichever direction was asked for
    RETURN CASE WHEN (d ILIKE 'desc%') <> reverse THEN 'DESC' ELSE 'ASC' END;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION sortby_with_tiebreakers(
    _sortby jsonb,
    _keys text[] DEFAULT '{collection,id}'
) RETURNS jsonb AS $$
DECLARE
    -- A missing or empty sortby is datetime DESC and a lone object is a one
    -- element array. The key columns of the searched relation (items unless
    -- given) are appended in the first direction for a total order.
    sort jsonb := CASE
        WHEN _sortby IS NULL OR jsonb_typeof(_sortby) = 'null' OR _sortby = '[]'::jsonb THEN '[{"field":"datetime","direction":"desc"}]'::jsonb
        WHEN jsonb_typeof(_sortby) = 'object' THEN jsonb_build_array(_sortby)
        ELSE _sortby
    END;
BEGIN
    IF jsonb_typeof(sort) != 'array' OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(sort) e
        WHERE jsonb_typeof(e) != 'object'
           OR jsonb_typeof(e->'field') IS DISTINCT FROM 'string'
           -- An empty field resolves to nothing, leaving a bare direction in the ORDER BY:
           -- a syntax error raised from deep inside search_rows.
           OR btrim(coalesce(strip_properties_prefix(e->>'field'), '')) = ''
    ) THEN
        RAISE EXCEPTION 'Invalid sortby %: must be an array of {"field": text, "direction": text} objects', _sortby;
    END IF;
    RETURN sort || coalesce(
        (
            SELECT jsonb_agg(jsonb_build_object('field', f, 'direction', sort->0->>'direction') ORDER BY n)
            FROM unnest(_keys) WITH ORDINALITY AS t(f, n)
            WHERE NOT jsonb_path_exists(sort, '$[*] ? (@.field == $f)', jsonb_build_object('f', f))
        ),
        '[]'::jsonb
    );
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE,
    _keys text[] DEFAULT '{collection,id}',
    _collection_ids text[] DEFAULT NULL
) RETURNS text AS $$
    WITH sorts AS (
        SELECT
            (queryable(value->>'field', _collection_ids)).expression as key,
            parse_sort_dir(value->>'direction', reverse) as dir
        FROM jsonb_array_elements(sortby_with_tiebreakers(_search->'sortby', _keys)) AS t(value)
    )
    SELECT array_to_string(
        array_agg(concat(key, ' ', dir)),
        ', '
    ) FROM sorts;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION  get_token_val_str(
    _field text,
    _item items
) RETURNS text AS $$
DECLARE
    q text;
    literal text;
BEGIN
    q := format($q$ SELECT quote_literal((%s)::text) FROM (SELECT $1.*) as r;$q$, _field);
    EXECUTE q INTO literal USING _item;
    RETURN literal;
END;
$$ LANGUAGE PLPGSQL;



-- The <collection>:<id> half of a paging token, each side the hex of its UTF-8 bytes so
-- neither can contain the separator. Hex digits are RFC 3986 unreserved, so the token crosses
-- a query string unchanged. The caller prefixes the direction to make a whole token.
CREATE OR REPLACE FUNCTION page_token(_collection text, _id text) RETURNS text AS $$
    SELECT encode(convert_to(_collection, 'UTF8'), 'hex')
        || ':' || encode(convert_to(_id, 'UTF8'), 'hex');
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_token_record(IN _token text, OUT prev BOOLEAN, OUT item items) RETURNS RECORD AS $$
DECLARE
    _parts text[];
BEGIN
    RAISE DEBUG 'Looking for token: %', _token;

    -- <direction>:<collection>:<id>, the collection and id hex encoded so neither can hold a
    -- separator. The direction is required: a token is only ever handed out as part of a
    -- next or prev link. Digits in pairs, or decode meets an odd-length string and rejects it.
    -- Lowered whole: hex is case insensitive to decode, so this costs nothing and makes the
    -- direction match without case folding each part.
    _parts := string_to_array(lower(_token), ':');
    IF cardinality(_parts) <> 3
        OR _parts[1] NOT IN ('next', 'prev')
        OR _parts[2] !~ '^([0-9a-f]{2})+$'
        OR _parts[3] !~ '^([0-9a-f]{2})+$'
    THEN
        RAISE EXCEPTION 'Invalid paging token: %', _token;
    END IF;
    prev := _parts[1] = 'prev';

    SELECT * INTO item FROM items
        WHERE collection = convert_from(decode(_parts[2], 'hex'), 'UTF8')
          AND id = convert_from(decode(_parts[3], 'hex'), 'UTF8');

    IF item IS NULL THEN
        RAISE EXCEPTION 'Could not find item using token: %', _token;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;


CREATE OR REPLACE FUNCTION get_token_filter(
    _sortby jsonb DEFAULT NULL,
    token_item items DEFAULT NULL,
    prev boolean DEFAULT FALSE,
    inclusive boolean DEFAULT FALSE,
    _collection_ids text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    ltop text := '<';
    gtop text := '>';
    sort record;
    orfilter text := '';
    orfilters text[] := '{}'::text[];
    andfilters text[] := '{}'::text[];
    output text;
    token_where text;
BEGIN
    _sortby := sortby_with_tiebreakers(_sortby);
    IF inclusive THEN
        orfilters := orfilters || format('( id=%L AND collection=%L )' , token_item.id, token_item.collection);
    END IF;

    FOR sort IN
        WITH s1 AS (
            SELECT
                _row,
                (queryable(value->>'field', _collection_ids)).expression as _field,
                (value->>'field' = 'id') as _isid,
                (value->>'field' = 'collection') as _iscollection,
                parse_sort_dir(value->>'direction') as _dir
            FROM jsonb_array_elements(_sortby)
            WITH ORDINALITY AS t(value, _row)
        )
        SELECT
            _row,
            _field,
            _dir,
            get_token_val_str(_field, token_item) as _val
        FROM s1
        WHERE _row <= (SELECT greatest(min(_row) FILTER (WHERE _isid), min(_row) FILTER (WHERE _iscollection)) FROM s1)
        ORDER BY _row ASC
    LOOP
        orfilter := NULL;
        RAISE DEBUG 'SORT: %', sort;
        IF sort._val IS NOT NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            orfilter := format('(%s %s %s)', sort._field, ltop, sort._val);
        ELSIF sort._val IS NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            RAISE DEBUG '< but null';
            orfilter := format('%s IS NOT NULL', sort._field);
        ELSIF sort._val IS NULL THEN
            RAISE DEBUG '> but null';
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
        RAISE DEBUG 'ORFILTER: %', orfilter;

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
    RAISE DEBUG 'TOKEN_WHERE: %',token_where;
    RETURN token_where;
    END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search_hash(jsonb, jsonb) RETURNS text AS $$
    SELECT md5(concat(($1 - '{token,limit,context,includes,excludes}'::text[])::text,$2::text));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
DROP FUNCTION IF EXISTS search_tohash(jsonb);

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

CREATE OR REPLACE FUNCTION where_stats(
    inwhere text,
    updatestats boolean default false,
    conf jsonb default null
) RETURNS search_wheres AS $$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    sw search_wheres%ROWTYPE;
    inwhere_hash text := md5(inwhere);
    _context text := lower(context(conf));
    _stats_ttl interval := context_stats_ttl(conf);
    _estimated_cost_threshold float := context_estimated_cost(conf);
    _estimated_count_threshold int := context_estimated_count(conf);
    ro bool := pgstac.readonly(conf);
BEGIN
    -- If updatestats is true then set ttl to 0
    IF updatestats THEN
        RAISE DEBUG 'Updatestats set to TRUE, setting TTL to 0';
        _stats_ttl := '0'::interval;
    END IF;

    -- If we don't need to calculate context, just return
    IF _context = 'off' THEN
        sw._where = inwhere;
        RETURN sw;
    END IF;

    -- Unlocked read. A fresh hit only bumps bookkeeping counters, and that is
    -- the common case when identical searches run concurrently.
    SELECT * INTO sw FROM search_wheres WHERE md5(_where)=inwhere_hash;

    -- Within ttl: bump usage counters and return. The bump skips locked rows so
    -- identical searches do not serialize; a missed increment is harmless.
    -- sw.id, not "sw IS NOT NULL": a composite is only IS NOT NULL when every
    -- field is, and search_wheres.partitions is never populated.
    IF
        sw.id IS NOT NULL
        AND sw.statslastupdated IS NOT NULL
        AND sw.total_count IS NOT NULL
        AND now() - sw.statslastupdated <= _stats_ttl
    THEN
        RAISE DEBUG 'Stats present in table and lastupdated within ttl: %', sw;
        IF NOT ro THEN
            UPDATE search_wheres SET
                lastused = now(),
                usecount = search_wheres.usecount + 1
            WHERE id = (
                SELECT id FROM search_wheres
                WHERE md5(_where) = inwhere_hash
                FOR UPDATE SKIP LOCKED
            );
        END IF;
        RAISE DEBUG 'Returning cached counts. %', sw;
        RETURN sw;
    END IF;

    -- Missing or stale, so lock the row to compute once, then re-check
    -- freshness in case another session finished while we waited.
    IF NOT ro THEN
        SELECT * INTO sw FROM search_wheres WHERE md5(_where)=inwhere_hash FOR UPDATE;
        IF
            sw.statslastupdated IS NOT NULL
            AND sw.total_count IS NOT NULL
            AND now() - sw.statslastupdated <= _stats_ttl
        THEN
            RAISE DEBUG 'Another process refreshed stats while we waited: %', sw;
            UPDATE search_wheres SET
                lastused = now(),
                usecount = search_wheres.usecount + 1
            WHERE md5(_where) = inwhere_hash
            RETURNING * INTO sw;
            RETURN sw;
        END IF;
    END IF;

    -- Calculate estimated cost and rows
    -- Use explain to get estimated count/cost
    IF sw.estimated_count IS NULL OR sw.estimated_cost IS NULL THEN
        RAISE DEBUG 'Calculating estimated stats';
        t := clock_timestamp();
        EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
            INTO explain_json;
        RAISE DEBUG 'Time for just the explain: %', clock_timestamp() - t;
        i := clock_timestamp() - t;

        sw.estimated_count := (explain_json->0->'Plan'->>'Plan Rows')::bigint;
        sw.estimated_cost := (explain_json->0->'Plan'->>'Total Cost')::float;
        sw.time_to_estimate := extract(epoch from i);
    END IF;

    RAISE DEBUG 'ESTIMATED_COUNT: %, THRESHOLD %', sw.estimated_count, _estimated_count_threshold;
    RAISE DEBUG 'ESTIMATED_COST: %, THRESHOLD %', sw.estimated_cost, _estimated_cost_threshold;

    -- If context is set to auto and the costs are within the threshold return the estimated costs
    IF
        _context = 'auto'
        AND sw.estimated_count >= _estimated_count_threshold
        AND sw.estimated_cost >= _estimated_cost_threshold
    THEN
        IF NOT ro THEN
            INSERT INTO search_wheres (
                _where,
                lastused,
                usecount,
                statslastupdated,
                estimated_count,
                estimated_cost,
                time_to_estimate,
                total_count,
                time_to_count
            ) VALUES (
                inwhere,
                now(),
                1,
                now(),
                sw.estimated_count,
                sw.estimated_cost,
                sw.time_to_estimate,
                null,
                null
            ) ON CONFLICT ((md5(_where)))
            DO UPDATE SET
                lastused = EXCLUDED.lastused,
                usecount = search_wheres.usecount + 1,
                statslastupdated = EXCLUDED.statslastupdated,
                estimated_count = EXCLUDED.estimated_count,
                estimated_cost = EXCLUDED.estimated_cost,
                time_to_estimate = EXCLUDED.time_to_estimate,
                total_count = EXCLUDED.total_count,
                time_to_count = EXCLUDED.time_to_count
            RETURNING * INTO sw;
        END IF;
        RAISE DEBUG 'Estimates are within thresholds, returning estimates. %', sw;
        RETURN sw;
    END IF;

    -- Calculate Actual Count
    t := clock_timestamp();
    RAISE DEBUG 'Calculating actual count...';
    EXECUTE format(
        'SELECT count(*) FROM items WHERE %s',
        inwhere
    ) INTO sw.total_count;
    i := clock_timestamp() - t;
    RAISE DEBUG 'Actual Count: % -- %', sw.total_count, i;
    sw.time_to_count := extract(epoch FROM i);

    IF NOT ro THEN
        INSERT INTO search_wheres (
            _where,
            lastused,
            usecount,
            statslastupdated,
            estimated_count,
            estimated_cost,
            time_to_estimate,
            total_count,
            time_to_count
        ) VALUES (
            inwhere,
            now(),
            1,
            now(),
            sw.estimated_count,
            sw.estimated_cost,
            sw.time_to_estimate,
            sw.total_count,
            sw.time_to_count
        ) ON CONFLICT ((md5(_where)))
        DO UPDATE SET
            lastused = EXCLUDED.lastused,
            usecount = search_wheres.usecount + 1,
            statslastupdated = EXCLUDED.statslastupdated,
            estimated_count = EXCLUDED.estimated_count,
            estimated_cost = EXCLUDED.estimated_cost,
            time_to_estimate = EXCLUDED.time_to_estimate,
            total_count = EXCLUDED.total_count,
            time_to_count = EXCLUDED.time_to_count
        RETURNING * INTO sw;
    END IF;
    RAISE DEBUG 'Returning with actual count. %', sw;
    RETURN sw;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION search_query(
    _search jsonb = '{}'::jsonb,
    updatestats boolean = false,
    _metadata jsonb = '{}'::jsonb
) RETURNS searches AS $$
DECLARE
    search searches%ROWTYPE;
    cached_search searches%ROWTYPE;
    pexplain jsonb;
    t timestamptz;
    i interval;
    doupdate boolean := FALSE;
    insertfound boolean := FALSE;
    ro boolean := pgstac.readonly();
    found_search text;
BEGIN
    -- Calculate hash, where clause, and order by statement
    search.search := _search;
    search.metadata := _metadata;
    search.hash := search_hash(_search, _metadata);
    search._where := stac_search_to_where(_search);
    search.orderby := sort_sqlorderby(_search, FALSE, '{collection,id}', to_text_array(_search->'collections'));
    search.lastused := now();
    search.usecount := 1;

    -- If we are in read only mode, directly return search
    IF ro THEN
        RETURN search;
    END IF;

    -- Update statistics for times used and and when last used
    -- If the entry is locked, rather than waiting, skip updating the stats
    INSERT INTO searches (search, lastused, usecount, metadata)
        VALUES (search.search, now(), 1, search.metadata)
        ON CONFLICT DO NOTHING
        RETURNING * INTO cached_search
    ;

    IF NOT FOUND OR cached_search IS NULL THEN
        UPDATE searches SET
            lastused = now(),
            usecount = searches.usecount + 1
        WHERE hash = (
            SELECT hash FROM searches WHERE hash=search.hash FOR UPDATE SKIP LOCKED
        )
        RETURNING * INTO cached_search
        ;
    END IF;

    IF cached_search IS NOT NULL THEN
        cached_search._where = search._where;
        cached_search.orderby = search.orderby;
        RETURN cached_search;
    END IF;
    RETURN search;

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search_fromhash(
    _hash text
) RETURNS searches AS $$
DECLARE
    _search jsonb;
BEGIN
    SELECT search INTO _search FROM searches WHERE hash=_hash LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Search with Query Hash % Not Found', _hash;
    END IF;
    RETURN search_query(_search);
END;
$$ LANGUAGE PLPGSQL STRICT;

CREATE OR REPLACE FUNCTION search_rows(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, collection DESC, id DESC',
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
RAISE DEBUG 'Getting chunks for % %', _where, _orderby;

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
        RAISE DEBUG 'Running Query for % to %. %', sdate, edate, age_ms(full_timer);
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE DEBUG 'QUERY: %', query;
        timer := clock_timestamp();
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE DEBUG 'Returned %/% Rows From % to %. % to go. Time: %ms', n, _limit, sdate, edate, records_left, age_ms(timer);
        timer := clock_timestamp();
        IF records_left <= 0 THEN
            RAISE DEBUG 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
            RETURN;
        END IF;
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RAISE DEBUG 'Running Query for % to %. %', sdate, edate, age_ms(full_timer);
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE DEBUG 'QUERY: %', query;
        timer := clock_timestamp();
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE DEBUG 'Returned %/% Rows From % to %. % to go. Time: %ms', n, _limit, sdate, edate, records_left, age_ms(timer);
        timer := clock_timestamp();
        IF records_left <= 0 THEN
            RAISE DEBUG 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
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
    RAISE DEBUG 'QUERY: %', query;
    timer := clock_timestamp();
    RETURN QUERY EXECUTE query;
    RAISE DEBUG 'FULL QUERY TOOK %ms', age_ms(timer);
END IF;
RAISE DEBUG 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
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
$$ LANGUAGE PLPGSQL;


-- A non-negative integer from a jsonb member, named for the message. Read as numeric, so a
-- value of the right shape but too large is refused here rather than surfacing as a bare
-- integer overflow from the cast.
CREATE OR REPLACE FUNCTION check_int(_value jsonb, _name text) RETURNS int AS $$
DECLARE
    v text := btrim(_value#>>'{}');
BEGIN
    IF v IS NULL THEN
        RETURN NULL;
    END IF;
    IF v !~ '^\d+$' OR v::numeric > 2147483647 THEN
        RAISE EXCEPTION 'Invalid % %: must be a non-negative integer', _name, _value;
    END IF;
    RETURN v::int;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

-- One reading of offset for both collection_search and collection_search_rows: two would let
-- the rows returned and the links offered to reach them disagree.
CREATE OR REPLACE FUNCTION search_offset(_search jsonb, _default int DEFAULT 0) RETURNS int AS $$
    SELECT COALESCE(pgstac.check_int(_search->'offset', 'offset'), _default);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search_limit(_search jsonb, _default int DEFAULT 10) RETURNS int AS $$
    SELECT COALESCE(pgstac.check_int(_search->'limit', 'limit'), _default);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

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
    collection jsonb;
    out_records jsonb;
    -- The (collection, id) of each row in out_records, in the same order. The tokens are
    -- built from these rather than from the formatted features, which fields.exclude can
    -- strip of exactly the two members a token needs.
    out_keys jsonb;
    out_len int;
    _limit int := search_limit(_search);
    _querylimit int;
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    has_prev boolean := FALSE;
    has_next boolean := FALSE;
    links jsonb := '[]'::jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'));
BEGIN
    searches := search_query(_search);
    _where := searches._where;
    orderby := searches.orderby;
    search_where := where_stats(_where, false, _search->'conf');
    total_count := coalesce(search_where.total_count, search_where.estimated_count);
    RAISE DEBUG 'SEARCH:TOKEN: %', _search->>'token';
    token := get_token_record(_search->>'token');
    _querylimit := _limit + 1;
    IF token IS NOT NULL THEN
        token_prev := token.prev;
        token_item := token.item;
        token_where := get_token_filter(_search->'sortby', token_item, token_prev, FALSE, to_text_array(_search->'collections'));
        RAISE DEBUG 'TOKEN_WHERE: % (%ms from search start)', token_where, age_ms(timer);
        IF token_prev THEN -- if we are using a prev token, we know has_next is true
            RAISE DEBUG 'There is a previous token, so automatically setting has_next to true';
            has_next := TRUE;
            orderby := sort_sqlorderby(_search, TRUE, '{collection,id}', to_text_array(_search->'collections'));
        ELSE
            RAISE DEBUG 'There is a next token, so automatically setting has_prev to true';
            has_prev := TRUE;

        END IF;
    ELSE -- if there was no token, we know there is no prev
        RAISE DEBUG 'There is no token, so we know there is no prev. setting has_prev to false';
        has_prev := FALSE;
    END IF;

    full_where := concat_ws(' AND ', _where, token_where);
    RAISE DEBUG 'FULL WHERE CLAUSE: %', full_where;
    RAISE DEBUG 'Time to get counts and build query %', age_ms(timer);
    timer := clock_timestamp();

    IF hydrate THEN
        RAISE DEBUG 'Getting hydrated data.';
    ELSE
        RAISE DEBUG 'Getting non-hydrated data.';
    END IF;
    RAISE DEBUG 'CACHE SET TO %', get_setting_bool('format_cache');
    RAISE DEBUG 'Time to set hydration/formatting %', age_ms(timer);
    timer := clock_timestamp();
    SELECT
        jsonb_agg(format_item(i, _fields, hydrate)),
        jsonb_agg(jsonb_build_array(i.collection, i.id))
    INTO out_records, out_keys
    FROM search_rows(
        full_where,
        orderby,
        search_where.partitions,
        _querylimit
    ) as i;

    RAISE DEBUG 'Time to fetch rows %', age_ms(timer);
    timer := clock_timestamp();


    IF token_prev THEN
        out_records := flip_jsonb_array(out_records);
        out_keys := flip_jsonb_array(out_keys);
    END IF;

    RAISE DEBUG 'Query returned % records.', jsonb_array_length(out_records);
    RAISE DEBUG 'TOKEN:   % %', token_item.id, token_item.collection;
    RAISE DEBUG 'RECORD_1: % %', out_keys->0->>1, out_keys->0->>0;
    RAISE DEBUG 'RECORD-1: % %', out_keys->-1->>1, out_keys->-1->>0;

    -- REMOVE records that were from our token
    IF out_keys->0->>0 = token_item.collection AND out_keys->0->>1 = token_item.id THEN
        out_records := out_records - 0;
        out_keys := out_keys - 0;
    ELSIF out_keys->-1->>0 = token_item.collection AND out_keys->-1->>1 = token_item.id THEN
        out_records := out_records - -1;
        out_keys := out_keys - -1;
    END IF;

    IF jsonb_array_length(out_records) = _limit + 1 THEN
        IF token_prev THEN
            has_prev := TRUE;
            out_records := out_records - 0;
            out_keys := out_keys - 0;
        ELSE
            has_next := TRUE;
            out_records := out_records - -1;
            out_keys := out_keys - -1;
        END IF;
    END IF;
    out_len := coalesce(jsonb_array_length(out_records), 0);

    links := links || jsonb_build_object(
        'rel', 'root',
        'type', 'application/json',
        'href', base_url
    ) || jsonb_build_object(
        'rel', 'self',
        'type', 'application/json',
        'href', concat(base_url, '/search')
    );

    -- An empty page still anchors on the token that produced it, so the caller has a way back
    -- instead of a dead end. A link identical to the incoming token is dropped: following it
    -- would return this same page forever.
    IF has_next AND out_len > 0 THEN
        next := page_token(out_keys->-1->>0, out_keys->-1->>1);
        RAISE DEBUG 'HAS NEXT | %', next;
        links := links || jsonb_build_object(
            'rel', 'next',
            'type', 'application/geo+json',
            'method', 'GET',
            'href', concat(base_url, '/search?token=next:', next)
        );
    END IF;

    IF has_prev AND (out_len > 0 OR _limit = 0) THEN
        prev := CASE WHEN out_len > 0
            THEN page_token(out_keys->0->>0, out_keys->0->>1)
            ELSE page_token(token_item.collection, token_item.id)
        END;
        -- Never hand back the token that produced this page: limit 0 reached by a prev token
        -- anchored the prev link on that same token, so a client following it never advanced.
        -- Compared lowered, as get_token_record reads the token, so PREV: cannot reopen the
        -- loop this guard closes.
        IF lower(concat('prev:', prev)) IS DISTINCT FROM lower(_search->>'token') THEN
            RAISE DEBUG 'HAS PREV | %', prev;
            links := links || jsonb_build_object(
                'rel', 'prev',
                'type', 'application/geo+json',
                'method', 'GET',
                'href', concat(base_url, '/search?token=prev:', prev)
            );
        ELSE
            prev := NULL;
        END IF;
    END IF;

    RAISE DEBUG 'Time to get prev/next %', age_ms(timer);
    timer := clock_timestamp();


    collection := jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb),
        'links', links
    );



    collection := collection || jsonb_build_object('numberReturned', out_len);
    IF context(_search->'conf') != 'off' THEN
        collection := collection || jsonb_strip_nulls(jsonb_build_object('numberMatched', total_count));
    END IF;

    IF get_setting_bool('timing', _search->'conf') THEN
        collection = collection || jsonb_build_object('timing', age_ms(init_ts));
    END IF;

    RAISE DEBUG 'Time to build final json %', age_ms(timer);
    timer := clock_timestamp();

    RAISE DEBUG 'Total Time: %', age_ms(current_timestamp);
    RAISE DEBUG 'RETURNING % records. NEXT: %. PREV: %', collection->>'numberReturned', collection->>'next', collection->>'prev';
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
    _limit int := search_limit(_search);
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    _orderby text := sort_sqlorderby(_search, FALSE, '{id}');
    _offset int := search_offset(_search);
BEGIN
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
    _limit int := search_limit(_search);
    _offset int := search_offset(_search);
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
    RAISE DEBUG 'nm: %, nr: %, l:%, o:%', number_matched, number_returned, _limit, _offset;



    -- a prev link depends only on being past the first page, a next link only on rows remaining
    IF _offset > 0 THEN
        -- Stepped back from the last offset that can hold rows, so an offset past the end does
        -- not walk back one empty page at a time, and by at least one so a limit of 0 does not
        -- produce a prev link pointing at the page that produced it.
        prevoffset := greatest(least(_offset, number_matched) - greatest(_limit, 1), 0);
        links := links || jsonb_build_object(
                'rel', 'prev',
                'type', 'application/json',
                'method', 'GET' ,
                'href', base_url,
                'body', jsonb_build_object('offset', prevoffset),
                'merge', TRUE
            );
    END IF;

    IF _limit > 0 AND _offset + _limit < number_matched THEN
        nextoffset := _offset + _limit;
        links := links || jsonb_build_object(
                'rel', 'next',
                'type', 'application/json',
                'method', 'GET' ,
                'href', base_url,
                'body', jsonb_build_object('offset', nextoffset),
                'merge', TRUE
            );
    END IF;

    ret := jsonb_build_object(
        'collections', out_records,
        'numberMatched', number_matched,
        'numberReturned', number_returned,
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
        RAISE DEBUG 'GEOMETRY IS NOT AN AREA';
        skipcovered = FALSE;
        exitwhenfull = FALSE;
    END IF;

    -- If skipcovered is true then you will always want to exit when the passed in geometry is full
    IF skipcovered THEN
        exitwhenfull := TRUE;
    END IF;

    search := search_fromhash(queryhash);

    tilearea := st_area(geom);
    _where := format('%s AND st_intersects(geometry, %L::geometry)', search._where, geom);


    FOR query IN SELECT * FROM partition_queries(_where, search.orderby) LOOP
        query := format('%s LIMIT %L', query, remaining_limit);
        RAISE DEBUG '%', query;
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

                RAISE DEBUG '% % % %', unionedgeom_area/tilearea, counter, scancounter, ftime();
            END IF;
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
        RAISE DEBUG '%', q;
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
        RAISE DEBUG '%', q;
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
    _partition text;
BEGIN
    IF runupdate THEN
        -- Not queued: the aggregate below reads what this writes. Ordered by
        -- partition, as every other multi-partition writer is.
        FOR _partition IN
            SELECT partition FROM partition_stats
            WHERE collection=_collection
            ORDER BY partition
        LOOP
            PERFORM update_partition_stats(_partition, false, true);
        END LOOP;
    END IF;
    SELECT
        min(lower(dtrange)),
        max(upper(edtrange)),
        st_extent(spatial)
    INTO
        mind,
        maxd,
        geom_extent
    FROM partition_stats
    WHERE collection=_collection;

    IF geom_extent IS NOT NULL AND mind IS NOT NULL AND maxd IS NOT NULL THEN
        extent := jsonb_build_object(
                'spatial', jsonb_build_object(
                    'bbox', to_jsonb(array[array[st_xmin(geom_extent), st_ymin(geom_extent), st_xmax(geom_extent), st_ymax(geom_extent)]])
                ),
                'temporal', jsonb_build_object(
                    'interval', to_jsonb(array[array[mind, maxd]])
                )
        );
        RETURN extent;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

-- Recalculates partition statistics before aggregating them; the observed
-- ranges are only maintained automatically when update_collection_extent is on.
-- return_target, not use_json_null: collection_extent returns NULL when it
-- cannot compute a full extent, and JSON null is not a valid STAC extent.
CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections
    SET content = jsonb_set_lax(
        content,
        '{extent}'::text[],
        collection_extent(id, TRUE),
        true,
        'return_target'
    )
;
$$ LANGUAGE SQL;


-- Reconcile partition_stats against the partition tree: an identity row for
-- every partition, and no rows for partitions that are gone. dtrange, edtrange
-- and spatial are left alone; update_partition_stats maintains those. Used by
-- the idempotent install and as a repair path.
CREATE OR REPLACE FUNCTION sync_partition_stats() RETURNS VOID AS $$
BEGIN
    -- Ordered by partition, as every other writer of these rows is.
    INSERT INTO partition_stats (partition, collection, partition_dtrange)
        SELECT partition, collection, partition_dtrange FROM partitions_view
        ORDER BY partition
        ON CONFLICT (partition) DO UPDATE
            SET collection = EXCLUDED.collection,
                partition_dtrange = EXCLUDED.partition_dtrange
            WHERE
                partition_stats.collection IS DISTINCT FROM EXCLUDED.collection
                OR partition_stats.partition_dtrange IS DISTINCT FROM EXCLUDED.partition_dtrange
    ;

    DELETE FROM partition_stats ps
    WHERE ps.partition IN (
        SELECT partition FROM partition_stats stale
        WHERE NOT EXISTS (
            SELECT 1 FROM partitions_view pv WHERE pv.partition = stale.partition
        )
        ORDER BY partition
        FOR UPDATE
    );
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;
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
SELECT set_version('0.10.0');
