/* converts a jsonb text array to a pg text[] array */
CREATE OR REPLACE FUNCTION textarr(_js jsonb)
  RETURNS text[] AS $$
  SELECT
    CASE jsonb_typeof(_js)
        WHEN 'array' THEN ARRAY(SELECT jsonb_array_elements_text(_js))
        ELSE ARRAY[_js->>0]
    END
;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_paths (IN jdata jsonb, OUT path text[], OUT value jsonb) RETURNS
SETOF RECORD AS $$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        value
    FROM jsonb_each(jdata)
union all
    select
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
select *
from extract_all;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION jsonb_obj_paths (IN jdata jsonb, OUT path text[], OUT value jsonb) RETURNS
SETOF RECORD AS $$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        value
    FROM jsonb_each(jdata)
union all
    select
        path || obj_key,
        obj_value
    from extract_all
    left join lateral
        jsonb_each(case jsonb_typeof(value) when 'object' then value end)
        as o(obj_key, obj_value)
        on jsonb_typeof(value) = 'object'
    where obj_key is not null
)
select *
from extract_all;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION jsonb_val_paths (IN jdata jsonb, OUT path text[], OUT value jsonb) RETURNS
SETOF RECORD AS $$
SELECT * FROM jsonb_obj_paths(jdata) WHERE jsonb_typeof(value) not in  ('object','array');
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION path_includes(IN path text[], IN includes text[]) RETURNS BOOLEAN AS $$
WITH t AS (SELECT unnest(includes) i)
SELECT EXISTS (
    SELECT 1 FROM t WHERE path @> string_to_array(trim(i), '.')
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION path_excludes(IN path text[], IN excludes text[]) RETURNS BOOLEAN AS $$
WITH t AS (SELECT unnest(excludes) e)
SELECT NOT EXISTS (
    SELECT 1 FROM t WHERE path @> string_to_array(trim(e), '.')
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_obj_paths_filtered (
    IN jdata jsonb,
    IN includes text[] DEFAULT ARRAY[]::text[],
    IN excludes text[] DEFAULT ARRAY[]::text[],
    OUT path text[],
    OUT value jsonb
) RETURNS
SETOF RECORD AS $$
SELECT path, value
FROM jsonb_obj_paths(jdata)
WHERE
    CASE WHEN cardinality(includes) > 0 THEN path_includes(path, includes) ELSE TRUE END
    AND
    path_excludes(path, excludes)

;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION filter_jsonb(
    IN jdata jsonb,
    IN includes text[] DEFAULT ARRAY[]::text[],
    IN excludes text[] DEFAULT ARRAY[]::text[]
) RETURNS jsonb AS $$
DECLARE
rec RECORD;
outj jsonb := '{}'::jsonb;
created_paths text[] := '{}'::text[];
BEGIN

IF empty_arr(includes) AND empty_arr(excludes) THEN
RAISE NOTICE 'no filter';
  RETURN jdata;
END IF;
FOR rec in
SELECT * FROM jsonb_obj_paths_filtered(jdata, includes, excludes)
WHERE jsonb_typeof(value) != 'object'
LOOP
    IF array_length(rec.path,1)>1 THEN
        FOR i IN 1..(array_length(rec.path,1)-1) LOOP
          IF NOT array_to_string(rec.path[1:i],'.') = ANY (created_paths) THEN
            outj := jsonb_set(outj, rec.path[1:i],'{}', true);
            created_paths := created_paths || array_to_string(rec.path[1:i],'.');
          END IF;
        END LOOP;
    END IF;
    outj := jsonb_set(outj, rec.path, rec.value, true);
    created_paths := created_paths || array_to_string(rec.path,'.');
END LOOP;
RETURN outj;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION flip_jsonb_array(j jsonb) RETURNS jsonb AS $$
SELECT jsonb_agg(value) FROM jsonb_array_elements(j) WITH ORDINALITY ORDER BY ordinality DESC;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
