CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE SCHEMA IF NOT EXISTS pgstac;


SET SEARCH_PATH TO pgstac, public;

/* converts a jsonb text array to a pg text[] array */
CREATE OR REPLACE FUNCTION textarr(_js jsonb)
  RETURNS text[] AS $$
  SELECT ARRAY(SELECT jsonb_array_elements_text(_js));
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

/*
converts a jsonb text array to comma delimited list of identifer quoted
useful for constructing column lists for selects
*/
CREATE OR REPLACE FUNCTION array_idents(_js jsonb)
  RETURNS text AS $$
  SELECT string_agg(quote_ident(v),',') FROM jsonb_array_elements_text(_js) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION properties_idx(_in jsonb) RETURNS jsonb AS $$
WITH kv AS (
    SELECT key, value FROM jsonb_each(_in) WHERE key = 'datetime' OR jsonb_typeof(value) IN ('object','array')
) SELECT lower((_in - array_agg(key))::text)::jsonb FROM kv;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;