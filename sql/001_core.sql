CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE SCHEMA IF NOT EXISTS pgstac;


SET SEARCH_PATH TO pgstac, public;

CREATE TABLE migrations (
  version text,
  datetime timestamptz DEFAULT now() NOT NULL
);

CREATE OR REPLACE FUNCTION notice(text) RETURNS boolean AS $$
DECLARE
BEGIN
    --IF current_setting('pgstac.debug')::boolean THEN
        RAISE NOTICE 'NOTICE FROM FUNC: % %', $1, clock_timestamp();
        RETURN TRUE;
    --END IF;
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
