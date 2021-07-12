CREATE OR REPLACE FUNCTION jsonb_str(text) RETURNS jsonb AS $$
SELECT jsonb_build_object('a', $1) -> 'a';
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION is_wkt(text) RETURNS boolean AS $body$
SELECT $1 ~* $q$^(SRID=\d+;)?(POINT|LINESTRING|LINEARRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)[ACEGIMLONPSRUTYZ\d,\.\\(\\) -]+$$q$;
$body$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION is_timestamp(text) RETURNS boolean AS $body$
SELECT $1 ~* $q$^(\d+)-(0[1-9]|1[012])-(0[1-9]|[12]\d|3[01])[ T]([01]\d|2[0-3]):([0-5]\d):([0-5]\d|60)(\.\d+)?(([Zz])|([\+|\-]([01]\d|2[0-3])))$$q$;
$body$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION cql_wrap(IN jdata jsonb) RETURNS jsonb AS $$
SELECT jsonb_build_object(
    'and',
    jsonb_agg(jsonb_build_object(key, value))
)
FROM jsonb_each(jdata);
$$ LANGUAGE SQL;


--DROP FUNCTION cql_paths;
/*
CREATE OR REPLACE FUNCTION cql_paths (IN jdata jsonb, OUT path text[], OUT value jsonb, OUT ctype text) RETURNS
SETOF RECORD AS $$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        value,
        jsonb_typeof(value) as ctype
    FROM jsonb_each(jdata)
union all
    select
        path || coalesce(obj_key, (arr_key- 1)::text),
        coalesce(obj_value, arr_value),
        jsonb_typeof(coalesce(obj_value, arr_value)) as ctype
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
*/



DROP FUNCTION array_map_ident;
CREATE OR REPLACE FUNCTION array_map_ident(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_ident(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

DROP FUNCTION array_map_literal;
CREATE OR REPLACE FUNCTION array_map_literal(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_literal(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION format_arr(text, text[]) returns text AS $$
DECLARE
ret text;
BEGIN
EXECUTE format('SELECT format(%L,%s);', $1, array_to_string(array_map_literal($2),',')) INTO ret;
RETURN ret;
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION cql_query_op;
CREATE OR REPLACE FUNCTION cql_query_op(j jsonb, _op text DEFAULT NULL) RETURNS text AS $$
DECLARE
jtype text := jsonb_typeof(j);
op text := lower(_op);
ops jsonb :=
    '{
        "eq": "%s = %s",
        "lt": "%s < %s",
        "lte": "%s <= %s",
        "gt": "%s > %s",
        "gte": "%s >= %s",
        "like": "%s LIKE %s",
        "+": "%s + %s",
        "-": "%s - %s",
        "*": "%s * %s",
        "/": "%s / %s",
        "s_equals": "st_equals(%s, %s)",
        "s_disjoint": "st_disjoint(%s, %s)",
        "s_touches": "st_touches(%s, %s)",
        "s_within": "st_within(%s, %s)",
        "s_overlaps": "st_overlaps(%s, %s)",
        "s_crosses": "st_crosses(%s, %s)",
        "s_intersects": "st_intersects(%s, %s)",
        "s_contains": "st_contains(%s, %s)",
        "intersects": "st_intersects(%s, %s)",
        "t_after":"%s >> %s",
        "t_before":"%s << %s",
        "t_contains":"%s @> %s",
        "t_disjoint":"NOT (%s && %s)",
        "t_during":"%s <@ %s",
        "t_equals":"%s = %s",
        "t_finishedby":"lower(%1$s) > lower(%2$s) AND upper(%1$s) = upper(%2$s)",
        "t_finishes":"lower(%1$s) < lower(%2$s) AND upper(%1$s) = upper(%2$s)",
        "t_intersects":"%s && %s",
        "anyinteracts":"%s && %s",
        "t_meets":"upper(%1$s) = lower(%2$s)",
        "t_metby":"lower(%1$s) = upper(%2$s)",
        "t_overlappedby":"lower(%1$s) < lower(%2$s) AND upper(%1$s) = upper(%2$s)",
        "t_overlaps":"lower(%1$s) < lower(%2$s) AND upper(%1$s) = upper(%2$s)",
        "t_startedby":"lower(%1$s) = lower(%2$s) AND upper(%1$s) < upper(%2$s)",
        "t_starts":"lower(%1$s) = lower(%2$s) AND upper(%1$s) > upper(%2$s)",
        "in": "%s = ANY (%s)",
        "not": "NOT (%s)",
        "between": "%s BETWEEN %s AND %s",
        "aequals":"%s = %s",
        "acontains":"%s @> %s",
        "contained by":"%s <@ %s",
        "aoverlaps":"%s && %s",
        "lower":"lower(%s)"
    }'::jsonb;
path_arr text[];
sub_path text;
ret text;
args text[] := NULL;

BEGIN
RAISE NOTICE 'j: %, op: %, jtype: %', j, op, jtype;

-- Set Lower Case on Both Arguments When Case Insensitive Flag Set
IF op in ('eq','lt','lte','gt','gte','like') AND jsonb_typeof(j->2) = 'boolean' THEN
    IF (j->>2)::boolean THEN
        RETURN format(concat('(',ops->>op,')'), cql_query_op(jsonb_build_array(j->0), 'lower'), cql_query_op(jsonb_build_array(j->1), 'lower'));
    END IF;
END IF;

-- Special Case when comparing a property in a jsonb field to a string or number using eq
-- Allows to leverage GIN index on jsonb fields
IF op = 'eq' THEN
    IF j->0 ? 'property' AND jsonb_typeof(j->1) IN ('number','string') THEN
        path_arr := string_to_array(j->0->>'property', '.');
        IF cardinality(path_arr) > 1 THEN
            sub_path := array_to_string('{$}'::text[] || array_map_ident(path_arr[2:]), '.');
            RETURN format($F$ ( %s @> '%s ? (@ == %s)' ) $F$, path_arr[1], sub_path, j->1);
        END IF;
    END IF;
END IF;

-- If using an array op, make sure both both arguments are passed as array
IF op in ('aequals','acontains','contained by','aoverlaps') THEN
    IF j->0 ? 'property' THEN
        -- wrap property in array
    ELSE
        -- return array
    END IF;
    IF j->1 ? 'property' THEN
        -- wrap property in array
    ELSE
        -- return array
    END IF;
END IF;



IF jtype = 'object' THEN
    RAISE NOTICE 'parsing object';
    IF j ? 'property' THEN
        -- Convert the property to be used as an identifier
        return array_to_string(array_map_ident(string_to_array(j->>'property', '.')),'->');

    ELSIF j ? 'type' AND j ? 'coordinates' THEN
        -- Convert to geometry wkb string
        RAISE NOTICE 'parsing geometry: % %', j::text, st_geomfromgeojson(j)::text;
        return st_geomfromgeojson(j)::text;

    ELSIF j ? 'bbox' THEN
        -- Convert to geometry wkb string
        return st_geomfromgeojson(j->>0)::text;

    ELSIF _op IS NULL THEN
        -- Iterate to convert elements in an object where the operator has not been set
        -- Combining with AND
        SELECT
            array_to_string(array_agg(cql_query_op(e.value, e.key)), ' AND ')
        INTO ret
        FROM jsonb_each(j) e;
        RETURN ret;
    END IF;
END IF;

IF jtype = 'string' THEN
    RETURN quote_literal(j->>0);
END IF;

IF jtype ='number' THEN
    RETURN (j->>0)::numeric;
END IF;

-- If the type of the passed json is an array
-- Calculate the arguments that will be passed to functions/operators
IF jtype = 'array' THEN
    SELECT INTO args
        array_agg(cql_query_op(e))
    FROM jsonb_array_elements(j) e;
END IF;
RAISE NOTICE 'ARGS: %', args;

IF op IN ('and','or') THEN
    SELECT
        CONCAT(
            '(',
            array_to_string(args, UPPER(CONCAT(' ',op,' '))),
            ')'
        ) INTO ret
        FROM jsonb_array_elements(j) e;
        RETURN ret;
END IF;

IF ops ? op THEN
    RAISE NOTICE 'ARGS: % MAPPED: %',args, array_map_literal(args);
    RETURN format(concat('(',ops->>op,')'), VARIADIC args);
END IF;

RETURN j->>0;

END;
$$ LANGUAGE PLPGSQL;




select * from cql_query_op('
{"eq": [{"property": "properties.landsat:wrs_row"},28],
"or": [
        { "lte": [{"property": "properties.ro:cloud_cover"},{"+":[{"property": "properties.ro:cloud_cover"},0.1]}] },
        { "eq": [{"property": "properties.landsat:wrs_row"},"texttest", true] },
        { "eq": [{"property": "datetime"},"2020-01-01T00:00:00Z"] },
        { "aoverlaps": [{"property": "myarray"},["a","b","c"]] }
        ]
}'::jsonb);

/*
select * from cql_query_op('{
"and": [
    {"and": [
        { "lte": [{"property": "properties.ro:cloud_cover"},0.1] },
        {"not": [{ "eq": [{"property": "properties.landsat:wrs_row"},28] }]},
        { "t_after": [{"property": "datetime"},203] },
        {
            "like": [{"property": "eo:instrument"},"OLI%"]
        },
        {
            "s_intersects": [
                {"property": "footprint"},
                {
                "type": "Polygon",
                "coordinates": [
                    [
                        [43.5845,-79.5442],
                        [43.6079,-79.4893],
                        [43.5677,-79.4632],
                        [43.6129,-79.3925],
                        [43.6223,-79.3238],
                        [43.6576,-79.3163],
                        [43.7945,-79.1178],
                        [43.8144,-79.1542],
                        [43.8555,-79.1714],
                        [43.7509,-79.6390],
                        [43.5845,-79.5442]
                    ]
                ]
                }
            ]
        }
    ]},
    {"or":[
        {"eq":[{"property":"assets.href"},"http://foo.com", true]},
        {"gte":[{"property":"ro:cloud_cover"},8]},
        {"eq":[{"property":"properties.foo"},"bar"]}
    ]}
]
}'::jsonb);
--*/
/*

CREATE OR REPLACE FUNCTION cql_sql(jdata jsonb) RETURNS text AS $$
DECLARE
_path text[];
j jsonb := cql_wrap(jdata);
c int;
counter int := 0;
BEGIN
LOOP
    counter := counter + 1;
    IF counter >200 THEN EXIT; END IF; -- Failsafe for infinite recursion

    SELECT path INTO _path FROM cql_paths(j)
    WHERE path[cardinality(path)] !~* E'^[0-9]+$'
    ORDER BY cardinality(path) desc;

    IF NOT FOUND OR cardinality(_path) < 3 THEN
        EXIT;
    END IF;

    RAISE NOTICE 'path: % lastpath: %', _path, _path[c];

    c := cardinality(_path);
    IF _path[c] = 'property' THEN
        j := jsonb_set(
            j,
            _path[:c-1],
            j #> _path[:c]
        );
    ELSIF _path[c] IN ('type', 'coordinates') THEN
        j := jsonb_set(
            j,
            _path[:c-1],
            jsonb_str(st_geomfromgeojson(j #>> _path[:c-1]))
        );
    ELSE
    j := jsonb_set(
        j,
        _path[:c-1],
        jsonb_str(cql_query_op(_path[c], j #> _path[:c]))
    );
    END IF;
    RAISE NOTICE '-- %', j;

END LOOP;
--RETURN cql_query_op(_path[c], j #> _path[:c]);
RETURN cql_query_op('and', j->'and');
END;
$$ LANGUAGE PLPGSQL;
*/

-- "landsat:processing_level": "l2sp"
/*
CREATE OR REPLACE FUNCTION cql_search(_cql jsonb) RETURNS jsonb AS $$
DECLARE
_where text := ' TRUE ';
_limit int := 100;
_orderby text := 'datetime desc, id asc';
rowcount int := 0;
curs refcursor;
r record;
features jsonb := '[]'::jsonb;
BEGIN
_where := cql_sql(_cql->'filter');
RAISE NOTICE '_where: %', _where;
FOR curs IN SELECT * FROM partition_cursor(_where, tstzrange('-infinity','infinity'), _orderby) LOOP
    LOOP
        FETCH curs INTO r;
        EXIT WHEN NOT FOUND OR rowcount > _limit;
        RAISE NOTICE 'rowcount: %       cloud_cover: %        processing_level: %', rowcount, r.properties->>'eo:cloud_cover', r.properties->>'landsat:processing_level';
        features := features || to_jsonb(r);
        rowcount := rowcount + 1;
    END LOOP;
    EXIT WHEN NOT NOT FOUND OR rowcount > _limit;
END LOOP;
RETURN features;
END;
$$ LANGUAGE PLPGSQL;


SELECT jsonb_array_length(s) FROM cql_search('{
    "filter": {
        "le": [{"property":"eo:cloud_cover"}, 5],
        "eq": [{"property":"landsat:processing_level"}, "l2sp"]
    }
}') s;
--*/
