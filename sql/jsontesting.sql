DROP TYPE IF EXISTS flat_jsonb CASCADE;
CREATE TYPE flat_jsonb AS (p jsonb, v jsonb);

DROP FUNCTION IF EXISTS flatten;
CREATE OR REPLACE FUNCTION flatten(
    j jsonb
) RETURNS
setof jsonb AS $$
with recursive extract_all as
(
    select
        '[]'::jsonb || to_jsonb(key::text) as path,
        value
    FROM jsonb_each(j)
union all
    select
        path ||
        CASE WHEN obj_key is not null then to_jsonb(obj_key) ELSE to_jsonb(arr_key::int -1) end,
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
select path || value
from extract_all where jsonb_typeof(value) not in ('array','object');
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION unflatten_step(j jsonb[]) returns jsonb[] as $$
WITH t AS (
    SELECT * FROM jsonb_array_elements(j)
),
   SELECT value - -1 - -2, value->-2, value->-1
    FROM t
    WHERE value
/*
with recursive t as (
    select p as path, v as value, 1 as loop from flatten('{"bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif"}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg"}}, "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 28, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_extensions": ["eo", "projection"]}'::jsonb)
    UNION ALL
    SELECT
        path - -1,
        jsonb_agg(value),
        loop + 1
    FROM t
    WHERE jsonb_array_length(path) > 1 AND jsonb_typeof(path - -1) = 'number'
    GROUP BY 1, 3
)
SELECT * FROM t limit 100;
*/

DROP FUNCTION IF EXISTS unflat;
CREATE OR REPLACE FUNCTION unflat(f flat_jsonb[], out p jsonb, out v jsonb) RETURNS SETOF record AS $$
WITH t AS (
    SELECT ((f).* FROM unnest(f)
)
SELECT * FROM t WHERE jsonb_array_length(p) = 1 OR jsonb_typeof(v) = 'string'
UNION ALL
SELECT p - -1, jsonb_agg(v) FROM t WHERE jsonb_array_length > 1 AND jsonb_typeof(v) = 'number'
;
$$ LANGUAGE SQL IMMUTABLE;

with t as (select array_agg(flatten('{"bbox": [-85.379245, 30.933949, -85.308201, 31.003555], "links": [], "assets": {"image": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.tif"}, "metadata": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_fgdc_2011/30085/m_3008506_nw_16_1_20110825.txt"}, "thumbnail": {"href": "https://naipeuwest.blob.core.windows.net/naip/v002/al/2011/al_100cm_2011/30085/m_3008506_nw_16_1_20110825.200.jpg"}}, "properties": {"gsd": 1, "datetime": "2011-08-25T00:00:00Z", "naip:year": "2011", "proj:bbox": [654842, 3423507, 661516, 3431125], "proj:epsg": 26916, "providers": [{"url": "https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/", "name": "USDA Farm Service Agency", "roles": ["producer", "licensor"]}], "naip:state": "al", "proj:shape": [7618, 6674], "eo:cloud_cover": 28, "proj:transform": [1, 0, 654842, 0, -1, 3431125, 0, 0, 1]}, "stac_extensions": ["eo", "projection"]}'::jsonb)) as agg)
select unflat(agg) from t;
