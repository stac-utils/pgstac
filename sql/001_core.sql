CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS pgstac;
SET SEARCH_PATH TO pgstac, public;

CREATE TABLE migrations (
  version text PRIMARY KEY,
  datetime timestamptz DEFAULT clock_timestamp() NOT NULL
);

CREATE OR REPLACE FUNCTION get_version() RETURNS text AS $$
  SELECT version FROM migrations ORDER BY datetime DESC, version DESC LIMIT 1;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION set_version(text) RETURNS text AS $$
  INSERT INTO migrations (version) VALUES ($1)
  ON CONFLICT DO NOTHING
  RETURNING version;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;


CREATE TABLE IF NOT EXISTS pgstac_settings (
  name text PRIMARY KEY,
  value text NOT NULL
);

INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '1000000'),
  ('context_stats_ttl', '1 day'),
  ('default-filter-lang', 'cql2-json')
ON CONFLICT DO NOTHING
;


CREATE OR REPLACE FUNCTION get_setting(IN _setting text, IN conf jsonb DEFAULT NULL) RETURNS text AS $$
SELECT COALESCE(
  conf->>_setting,
  current_setting(concat('pgstac.',_setting), TRUE),
  (SELECT value FROM pgstac_settings WHERE name=_setting)
);
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context();
CREATE OR REPLACE FUNCTION context(conf jsonb DEFAULT NULL) RETURNS text AS $$
  SELECT get_setting('context', conf);
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_estimated_count();
CREATE OR REPLACE FUNCTION context_estimated_count(conf jsonb DEFAULT NULL) RETURNS int AS $$
  SELECT get_setting('context_estimated_count', conf)::int;
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_estimated_cost();
CREATE OR REPLACE FUNCTION context_estimated_cost(conf jsonb DEFAULT NULL) RETURNS float AS $$
  SELECT get_setting('context_estimated_cost', conf)::float;
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_stats_ttl();
CREATE OR REPLACE FUNCTION context_stats_ttl(conf jsonb DEFAULT NULL) RETURNS interval AS $$
  SELECT get_setting('context_stats_ttl', conf)::interval;
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

CREATE OR REPLACE FUNCTION array_map_ident(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_ident(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_map_literal(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_literal(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION estimated_count(_where text) RETURNS bigint AS $$
DECLARE
rec record;
rows bigint;
BEGIN
    FOR rec in EXECUTE format(
        $q$
            EXPLAIN SELECT 1 FROM items WHERE %s
        $q$,
        _where)
    LOOP
        rows := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
        EXIT WHEN rows IS NOT NULL;
    END LOOP;

    RETURN rows;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE 'sql' STRICT IMMUTABLE;
