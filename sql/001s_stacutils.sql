/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
    CASE
            WHEN value->>'geometry' IS NOT NULL THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value->>'bbox' IS NOT NULL THEN
                ST_MakeEnvelope(
                    (value->'bbox'->>0)::float,
                    (value->'bbox'->>1)::float,
                    (value->'bbox'->>2)::float,
                    (value->'bbox'->>3)::float,
                    4326
                )
            ELSE NULL
        END as geometry
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION stac_datetime(value jsonb) RETURNS timestamptz AS $$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'start_datetime')::timestamptz
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_end_datetime(value jsonb) RETURNS timestamptz AS $$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'end_datetime')::timestamptz
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';


CREATE OR REPLACE FUNCTION stac_daterange(value jsonb) RETURNS tstzrange AS $$
SELECT tstzrange(stac_datetime(value),stac_end_datetime(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

-- CREATE OR REPLACE FUNCTION properties_idx(_in jsonb) RETURNS jsonb AS $$
-- WITH t AS (
--   select array_to_string(path,'.') as path, lower(value::text)::jsonb as lowerval
--   FROM  jsonb_val_paths(_in)
--   WHERE array_to_string(path,'.') not in ('datetime')
-- )
-- SELECT jsonb_object_agg(path, lowerval) FROM t;
-- $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

--DROP FUNCTION IF EXISTS jsonb_search_paths;
CREATE OR REPLACE FUNCTION properties_idx (IN jdata jsonb) RETURNS
jsonb AS $$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        ARRAY[key]::text[] as fullpath,
        value
    FROM jsonb_each(jdata)
union all
    select
        CASE WHEN obj_key IS NOT NULL THEN path || obj_key ELSE path END,
        path || coalesce(obj_key, (arr_key- 1)::text),
        coalesce(obj_value, arr_value)
    from extract_all
    left join lateral
        jsonb_each(case jsonb_typeof(value) when 'object' then value end)
        as o(obj_key, obj_value)
        on jsonb_typeof(value) = 'object'
    left join lateral
        jsonb_array_elements(case jsonb_typeof(value) when 'array' then value end)
        with ordinality as a(arr_value, arr_key)
        on jsonb_typeof(value) = 'array'
    where obj_key is not null or arr_key is not null
)
, paths AS (
select array_to_string(path, '.') as path, value FROM extract_all WHERE jsonb_typeof(value) NOT IN ('array','object')
), grouped AS (
SELECT path, jsonb_agg(distinct value) vals FROM paths group by path
) SELECT jsonb_object_agg(path, CASE WHEN jsonb_array_length(vals)=1 THEN vals->0 ELSE vals END) FROM grouped
; --*/
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
-- select jsonb_search_paths(content->'properties') from items limit 10;


-- with recursive extract_all as
-- (
--     select
--         ARRAY[key]::text[] as path,
--         ARRAY[key]::text[] as fullpath,
--         value
--     FROM jsonb_each((select content->'properties' from items limit 1))
-- union all
--     select
--         CASE WHEN obj_key IS NOT NULL THEN path || obj_key ELSE path END,
--         path || coalesce(obj_key, (arr_key- 1)::text),
--         coalesce(obj_value, arr_value)
--     from extract_all
--     left join lateral
--         jsonb_each(case jsonb_typeof(value) when 'object' then value end)
--         as o(obj_key, obj_value)
--         on jsonb_typeof(value) = 'object'
--     left join lateral
--         jsonb_array_elements(case jsonb_typeof(value) when 'array' then value end)
--         with ordinality as a(arr_value, arr_key)
--         on jsonb_typeof(value) = 'array'
--     where obj_key is not null or arr_key is not null
-- )
-- , paths AS (
-- select array_to_string(path, '.') as path, value FROM extract_all WHERE jsonb_typeof(value) NOT IN ('array','object')
-- )
-- SELECT jsonb_build_object(path, jsonb_agg(value))  FROM paths GROUP BY path;
