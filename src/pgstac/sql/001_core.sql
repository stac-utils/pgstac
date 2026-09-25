
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
    use_queue boolean := COALESCE(get_setting('use_queue'), 'FALSE')::boolean;
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
