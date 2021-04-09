CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS pgstac;
ALTER ROLE postgres SET SEARCH_PATH TO pgstac, public;
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


/* configuration table */
CREATE TABLE configuration (
    key VARCHAR,
    val JSONB
);

INSERT INTO configuration VALUES
('sort_columns', '{"datetime":"datetime","eo:cloud_cover":"properties->>''eo:cloud_cover''"}'::jsonb)
;

/* retrieve value from configuration table */
CREATE OR REPLACE FUNCTION get_config(_config text) RETURNS JSONB AS $$
SELECT val FROM configuration WHERE key=_config;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;


CREATE OR REPLACE FUNCTION link(rel text, href text, type text = 'application/json', baseurl text = '', title text = null) RETURNS jsonb AS $$
SELECT jsonb_strip_nulls(jsonb_build_object(
  'href', concat(baseurl,href),
  'rel', rel,
  'type', type,
  'title', title
));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION jsonb_value_as_tstz(val jsonb)
RETURNS timestamptz AS $$
SELECT * FROM jsonb_to_record(jsonb_build_object('a', val)) AS x(a timestamptz);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE CAST (jsonb AS timestamptz) WITH FUNCTION jsonb_value_as_tstz(jsonb) AS assignment;
