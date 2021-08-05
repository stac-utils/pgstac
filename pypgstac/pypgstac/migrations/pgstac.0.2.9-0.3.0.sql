SET SEARCH_PATH to pgstac, public;
alter table "pgstac"."items" drop constraint "items_collections_fk";

drop function if exists "pgstac"."search"(_js jsonb);

drop function if exists "pgstac"."array_idents"(_js jsonb);

drop function if exists "pgstac"."count_by_delim"(text, text);

drop function if exists "pgstac"."filter_by_order"(_item item, _sort jsonb, _type text);

drop function if exists "pgstac"."filter_by_order"(item_id text, _sort jsonb, _type text);

drop function if exists "pgstac"."in_array_q"(col text, arr jsonb);

drop function if exists "pgstac"."items_by_partition"(_where text, _dtrange tstzrange, _orderby text, _limit integer);

drop function if exists "pgstac"."properties_idx"(_in jsonb);

drop function if exists "pgstac"."rsort"(_sort jsonb);

drop function if exists "pgstac"."search_dtrange"(_indate jsonb, OUT _tstzrange tstzrange);

drop function if exists "pgstac"."sort"(_sort jsonb);

drop function if exists "pgstac"."sort_base"(_sort jsonb, OUT key text, OUT col text, OUT dir text, OUT rdir text, OUT sort text, OUT rsort text);

drop function if exists "pgstac"."split_stac_path"(path text, OUT col text, OUT dotpath text, OUT jspath text, OUT jspathtext text);

drop function if exists "pgstac"."stac_query"(_query jsonb);

drop function if exists "pgstac"."stac_query_op"(att text, _op text, val jsonb);

drop index if exists "pgstac"."properties_idx";

alter table "pgstac"."items" drop column IF EXISTS "properties";

alter table "pgstac"."items" alter column "id" set data type text using "id"::text;

alter table "pgstac"."items_template" drop column IF EXISTS "properties";

alter table "pgstac"."items_template" alter column "id" set data type text using "id"::text;



alter table "pgstac"."items" add constraint "items_collections_fk" FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.add_filters_to_cql(j jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
newprop jsonb;
newprops jsonb := '[]'::jsonb;
BEGIN
IF j ? 'id' THEN
    newprop := jsonb_build_object(
        'in',
        jsonb_build_array(
            '{"property":"id"}'::jsonb,
            j->'id'
        )
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;
IF j ? 'collection' THEN
    newprop := jsonb_build_object(
        'in',
        jsonb_build_array(
            '{"property":"collection"}'::jsonb,
            j->'collection'
        )
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

IF j ? 'datetime' THEN
    newprop := format(
        '{"anyinteracts":[{"property":"datetime"}, %s]}',
        j->'datetime'
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

IF j ? 'bbox' THEN
    newprop := format(
        '{"intersects":[{"property":"geometry"}, %s]}',
        j->'bbox'
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

IF j ? 'intersects' THEN
    newprop := format(
        '{"intersects":[{"property":"geometry"}, %s]}',
        j->'intersects'
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

RAISE NOTICE 'newprops: %', newprops;

IF newprops IS NOT NULL AND jsonb_array_length(newprops) > 0 THEN
    return jsonb_set(
        j,
        '{filter}',
        cql_and_append(j, jsonb_build_object('and', newprops))
    ) - '{id,collection,datetime,bbox,intersects}'::text[];
END IF;

return j;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.array_map_ident(_a text[])
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT array_agg(quote_ident(v)) FROM unnest(_a) v;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.array_map_literal(_a text[])
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT array_agg(quote_literal(v)) FROM unnest(_a) v;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.cql_and_append(existing jsonb, newfilters jsonb)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
SELECT CASE WHEN existing ? 'filter' AND newfilters IS NOT NULL THEN
    jsonb_build_object(
        'and',
        jsonb_build_array(
            existing->'filter',
            newfilters
        )
    )
ELSE
    newfilters
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.cql_query_op(j jsonb, _op text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
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
        "in": "%s = ANY (%s)",
        "not": "NOT (%s)",
        "between": "%s BETWEEN %s AND %s",
        "lower":"lower(%s)"
    }'::jsonb;
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
        RETURN format((items_path(j->0->>'property')).eq, j->1);
    END IF;
END IF;

IF op ilike 't_%' or op = 'anyinteracts' THEN
    RETURN temporal_op_query(op, j);
END IF;

IF op ilike 's_%' or op = 'intersects' THEN
    RETURN spatial_op_query(op, j);
END IF;


IF jtype = 'object' THEN
    RAISE NOTICE 'parsing object';
    IF j ? 'property' THEN
        -- Convert the property to be used as an identifier
        return (items_path(j->>'property')).path_txt;
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
    RAISE NOTICE 'Parsing array into args. j: %', j;
    -- If any argument is numeric, cast any text arguments to numeric
    IF j @? '$[*] ? (@.type() == "number")' THEN
        SELECT INTO args
            array_agg(concat('(',cql_query_op(e),')::numeric'))
        FROM jsonb_array_elements(j) e;
    ELSE
        SELECT INTO args
            array_agg(cql_query_op(e))
        FROM jsonb_array_elements(j) e;
    END IF;
END IF;
RAISE NOTICE 'ARGS: %', args;

IF args IS NULL OR cardinality(args) < 1 THEN
    RAISE NOTICE 'No Args';
    RETURN '';
END IF;

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

-- If the op is in the ops json then run using the template in the json
IF ops ? op THEN
    RAISE NOTICE 'ARGS: % MAPPED: %',args, array_map_literal(args);

    RETURN format(concat('(',ops->>op,')'), VARIADIC args);
END IF;

RETURN j->>0;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.cql_to_where(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
search jsonb := _search;
BEGIN
RAISE NOTICE 'SEARCH CQL 1: %', search;

-- Convert any old style stac query to cql
search := query_to_cqlfilter(search);

RAISE NOTICE 'SEARCH CQL 2: %', search;

-- Convert item,collection,datetime,bbox,intersects to cql
search := add_filters_to_cql(search);

RAISE NOTICE 'SEARCH CQL Final: %', search;

RETURN cql_query_op(search->'filter');
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.create_cursor(q text)
 RETURNS refcursor
 LANGUAGE plpgsql
AS $function$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR EXECUTE q;
    RETURN curs;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.estimated_count(_where text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
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
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_sort_dir(sort_item jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT CASE WHEN sort_item->>'direction' ILIKE 'desc%' THEN 'DESC' ELSE 'ASC' END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_token_filter(_search jsonb DEFAULT '{}'::jsonb, token_rec jsonb DEFAULT NULL::jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
token_id text;
filters text[] := '{}'::text[];
prev boolean := TRUE;
field text;
dir text;
sort record;
orfilters text[] := '{}'::text[];
andfilters text[] := '{}'::text[];
output text;
BEGIN
-- If no token provided return NULL
IF token_rec IS NULL THEN
    IF NOT (_search ? 'token' AND
            (
                (_search->>'token' ILIKE 'prev:%')
                OR
                (_search->>'token' ILIKE 'next:%')
            )
    ) THEN
        RETURN '';
    END IF;
    prev := (_search->>'token' ILIKE 'prev:%');
    token_id := substr(_search->>'token', 6);
    SELECT to_jsonb(items) INTO token_rec FROM items WHERE id=token_id;
END IF;
RAISE NOTICE 'TOKEN ID: %', token_rec->'id';

CREATE TEMP TABLE sorts (
    _row int GENERATED ALWAYS AS IDENTITY NOT NULL,
    _field text PRIMARY KEY,
    _dir text NOT NULL,
    _val text
) ON COMMIT DROP;

-- Make sure we only have distinct columns to sort with taking the first one we get
INSERT INTO sorts (_field, _dir)
    SELECT
        (items_path(value->>'field')).path,
        get_sort_dir(value)
    FROM
        jsonb_array_elements(coalesce(_search->'sort','[{"field":"datetime","direction":"desc"}]'))
ON CONFLICT DO NOTHING
;

-- Get the first sort direction provided. As the id is a primary key, if there are any
-- sorts after id they won't do anything, so make sure that id is the last sort item.
SELECT _dir INTO dir FROM sorts ORDER BY _row ASC LIMIT 1;
IF EXISTS (SELECT 1 FROM sorts WHERE _field = 'id') THEN
    DELETE FROM sorts WHERE _row > (SELECT _row FROM sorts WHERE _field = 'id');
ELSE
    INSERT INTO sorts (_field, _dir) VALUES ('id', dir);
END IF;

-- Add value from looked up item to the sorts table
UPDATE sorts SET _val=quote_literal(token_rec->>_field);

-- Check if all sorts are the same direction and use row comparison
-- to filter
IF (SELECT count(DISTINCT _dir) FROM sorts) = 1 THEN
    SELECT format(
            '(%s) %s (%s)',
            concat_ws(', ', VARIADIC array_agg(quote_ident(_field))),
            CASE WHEN (prev AND dir = 'ASC') OR (NOT prev AND dir = 'DESC') THEN '<' ELSE '>' END,
            concat_ws(', ', VARIADIC array_agg(_val))
    ) INTO output FROM sorts
    WHERE token_rec ? _field
    ;
ELSE
    FOR sort IN SELECT * FROM sorts ORDER BY _row asc LOOP
        RAISE NOTICE 'SORT: %', sort;
        IF sort._row = 1 THEN
            orfilters := orfilters || format('(%s %s %s)',
                quote_ident(sort._field),
                CASE WHEN (prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC') THEN '<' ELSE '>' END,
                sort._val
            );
        ELSE
            orfilters := orfilters || format('(%s AND %s %s %s)',
                array_to_string(andfilters, ' AND '),
                quote_ident(sort._field),
                CASE WHEN (prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC') THEN '<' ELSE '>' END,
                sort._val
            );

        END IF;
        andfilters := andfilters || format('%s = %s',
            quote_ident(sort._field),
            sort._val
        );
    END LOOP;
    output := array_to_string(orfilters, ' OR ');
END IF;
DROP TABLE IF EXISTS sorts;
RETURN concat('(',coalesce(output,'true'),')');
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_path(dotpath text, OUT field text, OUT path text, OUT path_txt text, OUT jsonpath text, OUT eq text)
 RETURNS record
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE
AS $function$
DECLARE
path_elements text[];
last_element text;
BEGIN
dotpath := replace(trim(dotpath), 'properties.', '');

IF dotpath = '' THEN
    RETURN;
END IF;

path_elements := string_to_array(dotpath, '.');
jsonpath := NULL;

IF path_elements[1] IN ('id','geometry','datetime','collection_id') THEN
    field := path_elements[1];
    path_elements := path_elements[2:];
ELSIF path_elements[1] IN ('links', 'assets', 'stac_version', 'stac_extensions') THEN
    field := 'content';
ELSE
    field := 'content';
    path_elements := '{properties}'::text[] || path_elements;
END IF;
IF cardinality(path_elements)<1 THEN
    path := field;
    path_txt := field;
    jsonpath := '$';
    eq := format($F$ %s = %%s $F$, field);
    RETURN;
END IF;


last_element := path_elements[cardinality(path_elements)];
path_elements := path_elements[1:cardinality(path_elements)-1];
jsonpath := concat(array_to_string('{$}'::text[] || array_map_ident(path_elements), '.'), '.', quote_ident(last_element));
path_elements := array_map_literal(path_elements);
path     := format($F$ items.properties->%s $F$, quote_literal(dotpath));
path_txt := format($F$ items.properties->>%s $F$, quote_literal(dotpath));
eq := format($F$ items.properties @? '$.%s[*] ? (@ == %%s) '$F$, quote_ident(dotpath));


RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.notice(text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    --IF current_setting('pgstac.debug')::boolean THEN
        RAISE NOTICE 'NOTICE FROM FUNC: % %', $1, clock_timestamp();
        RETURN TRUE;
    --END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.parse_dtrange(_indate jsonb, OUT _tstzrange tstzrange)
 RETURNS tstzrange
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
WITH t AS (
    SELECT CASE
        WHEN jsonb_typeof(_indate) = 'array' THEN
            textarr(_indate)
        ELSE
            regexp_split_to_array(
                btrim(_indate::text,'"'),
                '/'
            )
        END AS arr
)
, t1 AS (
    SELECT
        CASE
            WHEN array_upper(arr,1) = 1 OR arr[1] = '..' OR arr[1] IS NULL THEN '-infinity'::timestamptz
            ELSE arr[1]::timestamptz
        END AS st,
        CASE
            WHEN array_upper(arr,1) = 1 THEN arr[1]::timestamptz
            WHEN arr[2] = '..' OR arr[2] IS NULL THEN 'infinity'::timestamptz
            ELSE arr[2]::timestamptz
        END AS et
    FROM t
)
SELECT
    tstzrange(st,et)
FROM t1;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.parse_sort_dir(_dir text, reverse boolean DEFAULT false)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
WITH t AS (
    SELECT COALESCE(upper(_dir), 'ASC') as d
) SELECT
    CASE
        WHEN NOT reverse THEN d
        WHEN d = 'ASC' THEN 'DESC'
        WHEN d = 'DESC' THEN 'ASC'
    END
FROM t;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_cursor(_where text DEFAULT 'TRUE'::text, _orderby text DEFAULT 'datetime DESC, id DESC'::text, _dtrange tstzrange DEFAULT tstzrange('-infinity'::timestamp with time zone, 'infinity'::timestamp with time zone))
 RETURNS SETOF refcursor
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    partition_query text;
    main_query text;
    batchcount int;
    counter int := 0;
    p record;
    cursors refcursor;
BEGIN
IF _orderby ILIKE 'datetime d%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange DESC;
    $q$);
ELSIF _orderby ILIKE 'datetime a%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange ASC
        ;
    $q$);
ELSE
    partition_query := format($q$
        SELECT 'items' as partition WHERE $1 IS NOT NULL;
    $q$);
END IF;
RAISE NOTICE 'Partition Query: %', partition_query;
FOR p IN
    EXECUTE partition_query USING (_dtrange)
LOOP
    IF lower(_dtrange)::timestamptz > '-infinity' THEN
        _where := concat(_where,format(' AND datetime >= %L',lower(_dtrange)::timestamptz::text));
    END IF;
    IF upper(_dtrange)::timestamptz < 'infinity' THEN
        _where := concat(_where,format(' AND datetime <= %L',upper(_dtrange)::timestamptz::text));
    END IF;

    main_query := format($q$
        SELECT * FROM %I items
        WHERE %s
        ORDER BY %s
    $q$, p.partition::text, _where, _orderby
    );

    RETURN NEXT create_cursor(main_query);
END LOOP;
RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.properties(_item items)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT properties_idx(_item.content);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.properties_idx(content jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        ARRAY[key]::text[] as fullpath,
        value
    FROM jsonb_each(content->'properties')
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
select
    array_to_string(path, '.') as path,
    value
FROM extract_all
WHERE
    jsonb_typeof(value) NOT IN ('array','object')
), grouped AS (
SELECT path, jsonb_agg(distinct value) vals FROM paths group by path
) SELECT jsonb_object_agg(path, CASE WHEN jsonb_array_length(vals)=1 THEN vals->0 ELSE vals END) - '{datetime}'::text[] FROM grouped
;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.query_to_cqlfilter(j jsonb)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
-- Translates anything passed in through the deprecated "query" into equivalent CQL
WITH t AS (
    SELECT key as property, value as ops
        FROM jsonb_each(j->'query')
), t2 AS (
    SELECT property, (jsonb_each(ops)).*
        FROM t WHERE jsonb_typeof(ops) = 'object'
    UNION ALL
    SELECT property, 'eq', ops
        FROM t WHERE jsonb_typeof(ops) != 'object'
), t3 AS (
SELECT
    jsonb_strip_nulls(jsonb_build_object(
        'and',
        jsonb_agg(
            jsonb_build_object(
                key,
                jsonb_build_array(
                    jsonb_build_object('property',property),
                    value
                )
            )
        )
    )) as qcql FROM t2
)
SELECT
    CASE WHEN qcql IS NOT NULL AND notice(qcql::text) THEN
        jsonb_set(j, '{filter}', cql_and_append(j, qcql)) - 'query'
    ELSE j
    END
FROM t3
;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.sort_dir_to_op(_dir text, prev boolean DEFAULT false)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
WITH t AS (
    SELECT COALESCE(upper(_dir), 'ASC') as d
) SELECT
    CASE
        WHEN d = 'ASC' AND prev THEN '<='
        WHEN d = 'DESC' AND prev THEN '>='
        WHEN d = 'ASC' THEN '>='
        WHEN d = 'DESC' THEN '<='
    END
FROM t;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.sort_sqlorderby(_search jsonb DEFAULT NULL::jsonb, reverse boolean DEFAULT false)
 RETURNS text
 LANGUAGE sql
AS $function$
WITH sorts AS (
    SELECT
        (items_path(value->>'field')).path as key,
        parse_sort_dir(value->>'direction', reverse) as dir
    FROM jsonb_array_elements(
        '[]'::jsonb
        ||
        coalesce(_search->'sort','[{"field":"datetime", "direction":"desc"}]')
        ||
        '[{"field":"id","direction":"desc"}]'::jsonb
    )
)
SELECT array_to_string(
    array_agg(concat(key, ' ', dir)),
    ', '
) FROM sorts;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.spatial_op_query(op text, args jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
geom text;
j jsonb := args->1;
BEGIN
op := lower(op);
RAISE NOTICE 'Constructing spatial query OP: %, ARGS: %', op, args;
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

RETURN format('%s(geometry, %L::geometry)', op, geom);
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.stac_daterange(value jsonb)
 RETURNS tstzrange
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET "TimeZone" TO 'UTC'
AS $function$
SELECT tstzrange(stac_datetime(value),stac_end_datetime(value));
$function$
;

CREATE OR REPLACE FUNCTION pgstac.stac_end_datetime(value jsonb)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET "TimeZone" TO 'UTC'
AS $function$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'end_datetime')::timestamptz
);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.temporal_op_query(op text, args jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
ll text := 'stac_datetime(content)';
lh text := 'stac_end_datetime(content)';
rrange tstzrange;
rl text;
rh text;
outq text;
BEGIN
rrange := parse_dtrange(args->1);
RAISE NOTICE 'Constructing temporal query OP: %, ARGS: %, RRANGE: %', op, args, rrange;
op := lower(op);
rl := format('%L::timestamptz', lower(rrange));
rh := format('%L::timestamptz', upper(rrange));
outq := CASE op
    WHEN 't_before'       THEN 'lh < rl'
    WHEN 't_after'        THEN 'll > rh'
    WHEN 't_meets'        THEN 'lh = rl'
    WHEN 't_metby'        THEN 'll = rh'
    WHEN 't_overlaps'     THEN 'll < rl AND rl < lh < rh'
    WHEN 't_overlappedby' THEN 'rl < ll < rh AND lh > rh'
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
outq := regexp_replace(outq, '\mll\M', ll);
outq := regexp_replace(outq, '\mlh\M', lh);
outq := regexp_replace(outq, '\mrl\M', rl);
outq := regexp_replace(outq, '\mrh\M', rh);
outq := format('(%s)', outq);
RETURN outq;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.flip_jsonb_array(j jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
SELECT jsonb_agg(value) FROM (SELECT value FROM jsonb_array_elements(j) WITH ORDINALITY ORDER BY ordinality DESC) as t;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    _where text := cql_to_where(_search);
    token_where text := get_token_filter(_search, null::jsonb);
    full_where text;
    orderby text := sort_sqlorderby(_search);
    token_type text := substr(_search->>'token',1,4);
    _limit int := coalesce((_search->>'limit')::int, 10);
    curs refcursor;
    exit_flag boolean := FALSE;
    estimated_count bigint;
    cntr int := 0;
    iter_record items%ROWTYPE;
    first_record items%ROWTYPE;
    last_record items%ROWTYPE;
    out_records jsonb := '[]'::jsonb;
    partitions_scanned int := 0;
    prev_query text;
    next text;
    prev_id text;
    has_next boolean := false;
    has_prev boolean := false;
    prev text;
    total_query text;
    total_count bigint;
    context jsonb;
    collection jsonb;
    includes text[];
    excludes text[];
BEGIN

IF trim(_where) = '' THEN
    _where := NULL;
END IF;
_where := coalesce(_where, ' TRUE ');

IF trim(token_where) = '' THEN
    token_where := NULL;
END IF;
full_where := concat_ws(' AND ', _where, token_where);

IF token_type='prev' THEN
    token_where := get_token_filter(_search, null::jsonb);
    orderby := sort_sqlorderby(_search, TRUE);
END IF;

RAISE NOTICE 'WHERE: %, TOKEN_WHERE: %, ORDERBY: %', _where, token_where, orderby;
RAISE NOTICE 'FULL_WHERE: %', full_where;

FOR curs IN
    SELECT *
    FROM partition_cursor(
        full_where,
        orderby
    )
LOOP
    partitions_scanned := partitions_scanned + 1;
    RAISE NOTICE 'Partitions Scanned: %', partitions_scanned;
    LOOP
        FETCH curs into iter_record;
        EXIT WHEN NOT FOUND;
        cntr := cntr + 1;
        last_record := iter_record;
        IF cntr = 1 THEN
            first_record := last_record;
        END IF;
        IF cntr <= _limit THEN
            out_records := out_records || last_record.content;
            --next := last_record.id;
        ELSIF cntr > _limit THEN
            has_next := true;
            exit_flag := TRUE;
            EXIT;
        END IF;
    END LOOP;
    IF exit_flag THEN
        exit;
    END IF;
END LOOP;


-- Flip things around if this was the result of a prev token query
IF token_type='prev' THEN
    out_records := flip_jsonb_array(out_records);
    first_record := last_record;
END IF;

-- If this query has a token, see if there is data before the first record
IF _search ? 'token' THEN
    prev_query := format(
        'SELECT 1 FROM items WHERE %s LIMIT 1',
        concat_ws(
            ' AND ',
            _where,
            trim(get_token_filter(_search, to_jsonb(first_record)))
        )
    );
    RAISE NOTICE 'Query to get previous record: % --- %', prev_query, first_record;
    EXECUTE prev_query INTO has_prev;
    IF FOUND and has_prev IS NOT NULL THEN
        RAISE NOTICE 'Query results from prev query: %', has_prev;
        has_prev := TRUE;
    END IF;
END IF;
has_prev := COALESCE(has_prev, FALSE);

RAISE NOTICE 'token_type: %, has_next: %, has_prev: %', token_type, has_next, has_prev;
IF has_prev THEN
    prev := out_records->0->>'id';
END IF;
IF has_next OR token_type='prev' THEN
    next := out_records->-1->>'id';
END IF;



-- include/exclude any fields following fields extension
IF _search ? 'fields' THEN
    IF _search->'fields' ? 'exclude' THEN
        excludes=textarr(_search->'fields'->'exclude');
    END IF;
    IF _search->'fields' ? 'include' THEN
        includes=textarr(_search->'fields'->'include');
        IF array_length(includes, 1)>0 AND NOT 'id' = ANY (includes) THEN
            includes = includes || '{id}';
        END IF;
    END IF;
    --RAISE NOTICE 'Includes: %, Excludes: %', includes, excludes;
    --RAISE NOTICE 'out_records: %', out_records;
    SELECT jsonb_agg(filter_jsonb(row, includes, excludes)) INTO out_records FROM jsonb_array_elements(out_records) row;
END IF;

-- context setting is the max number of rows to do a full count
-- if the estimated rows is less than the context setting we
-- will count all the rows
estimated_count := estimated_count(full_where);
RAISE NOTICE 'Estimated Count: %', estimated_count;
IF _search ? 'context' THEN
    IF (_search->>'context')::int > estimated_count THEN
        total_query := format(
            'SELECT count(*) FROM items WHERE %s',
            full_where
        );
        RAISE NOTICE 'Query to get total count: %', total_query;
        EXECUTE total_query INTO total_count;
        RAISE NOTICE 'Total Records for Query: %', total_count;
    END IF;
END IF;

context := jsonb_strip_nulls(jsonb_build_object(
    'limit', _limit,
    'matched', total_count,
    'returned', jsonb_array_length(out_records)
));

collection := jsonb_build_object(
    'type', 'FeatureCollection',
    'features', out_records,
    'next', next,
    'prev', prev,
    'context', context
);

RETURN collection;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.stac_datetime(value jsonb)
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET "TimeZone" TO 'UTC'
AS $function$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'start_datetime')::timestamptz
);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.textarr(_js jsonb)
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
  SELECT
    CASE jsonb_typeof(_js)
        WHEN 'array' THEN ARRAY(SELECT jsonb_array_elements_text(_js))
        ELSE ARRAY[_js->>0]
    END
;
$function$
;

CREATE INDEX properties_idx ON pgstac.items USING gin (properties_idx(content) jsonb_path_ops);

INSERT INTO migrations (version) VALUES ('0.3.0');
