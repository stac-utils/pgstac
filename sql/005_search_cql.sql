
SET SEARCH_PATH TO pgstac_test, pgstac, public;

CREATE OR REPLACE FUNCTION items_path(text) RETURNS text AS $$
SELECT $1;
$$ LANGUAGE SQL;

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

CREATE OR REPLACE FUNCTION parse_dtrange(IN _indate jsonb, OUT _tstzrange tstzrange) AS $$
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
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


DROP FUNCTION IF EXISTS bbox_geom;
CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS geometry AS $$
SELECT CASE jsonb_array_length(_bbox)
    WHEN 4 THEN
        ST_SetSRID(ST_MakeEnvelope(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float,
            (_bbox->>3)::float
        ),4326)
    WHEN 6 THEN
    ST_SetSRID(ST_3DMakeBox(
        ST_MakePoint(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float
        ),
        ST_MakePoint(
            (_bbox->>3)::float,
            (_bbox->>4)::float,
            (_bbox->>5)::float
        )
    ),4326)
    ELSE null END;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION cql_and_append(existing jsonb, newfilters jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE SQL;


-- ADDs base filters (ids, collections, datetime, bbox, intersects) that are
-- added outside of the filter/query in the stac request


CREATE OR REPLACE FUNCTION add_filters_to_cql(j jsonb) RETURNS jsonb AS $$
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
    newprop := format('{"anyinteracts":[{"property":"datetime"}, %s]}', j->'datetime')::jsonb;
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;
/*
IF j ? 'bbox' THEN
    newprop := jsonb_build_object("intersects":[{"property":"geom"}, j->>'bbox']);
    newprops := jsonb_insert(newprops, '{1}', newprop;
END IF;
IF j ? 'intersects' THEN
    newprop := jsonb_build_object("intersects":[{"property":"geom"}, j->>'intersects']);
    newprops := jsonb_insert(newprops, '{1}', newprop;
END IF;
*/
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
$$ LANGUAGE PLPGSQL;

-- Translates anything passed in through the deprecated "query" into equivalent CQL

CREATE OR REPLACE FUNCTION notice(text) RETURNS boolean AS $$
DECLARE
BEGIN
RAISE NOTICE 'NOTICE FROM FUNC: %', $1;
RETURN TRUE;
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS query_to_cqlfilter;


CREATE OR REPLACE FUNCTION query_to_cqlfilter(j jsonb) RETURNS jsonb AS $$
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
--SELECT qcql FROM t3
;
$$ LANGUAGE SQL;

/* cql_query_op -- Parses a CQL query operation, recursing when necessary
     IN jsonb -- a subelement from a valid stac query
     IN text -- the operator being used on elements passed in
     RETURNS a SQL fragment to be used in a WHERE clause
*/
DROP FUNCTION IF EXISTS cql_query_op;
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
        RETURN bbox_geom(j)::text;


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

IF ops ? op THEN
    RAISE NOTICE 'ARGS: % MAPPED: %',args, array_map_literal(args);
    RETURN format(concat('(',ops->>op,')'), VARIADIC args);
END IF;

RETURN j->>0;

END;
$$ LANGUAGE PLPGSQL;

/* Functions to create an iterable of cursors over partitions. */
CREATE OR REPLACE FUNCTION create_cursor(q text) RETURNS refcursor AS $$
DECLARE
curs refcursor;
BEGIN
OPEN curs FOR EXECUTE q;
RETURN curs;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION partition_cursor(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity')
) RETURNS SETOF refcursor AS $$
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
        SELECT * FROM %I
        WHERE %s
        ORDER BY %s
    $q$, p.partition::text, _where, _orderby
    );

    RETURN NEXT create_cursor(main_query);
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;



CREATE OR REPLACE FUNCTION cql_to_where(_search jsonb = '{}'::jsonb) RETURNS text AS $$
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
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION parse_sort_dir(_dir text, reverse boolean default false) RETURNS text AS $$
WITH t AS (
    SELECT COALESCE(upper(_dir), 'ASC') as d
) SELECT
    CASE
        WHEN NOT reverse THEN d
        WHEN d = 'ASC' THEN 'DESC'
        WHEN d = 'DESC' THEN 'ASC'
    END
FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sort_dir_to_op(_dir text, prev boolean default false) RETURNS text AS $$
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
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

DROP FUNCTION IF EXISTS sort_sqlorderby;
CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE
) RETURNS text AS $$
WITH sorts AS (
    SELECT
        items_path(value->>'field') as key,
        parse_sort_dir(value->>'direction', reverse) as dir
    FROM jsonb_array_elements(
        '[]'::jsonb
        ||
        coalesce(_search->'sort','[{"field":"datetime", "direction":"desc"}]')
        ||
        '[{"field":"id","direction":"desc"}]'::jsonb
    )
)
SELECT concat_ws(
    ', ',
    VARIADIC array_agg(concat(key, ' ', dir))
) FROM sorts;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_sort_dir(sort_item jsonb) RETURNS text AS $$
SELECT CASE WHEN sort_item->>'direction' ILIKE 'desc%' THEN 'DESC' ELSE 'ASC' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


DROP FUNCTION IF EXISTS get_token_filter;
CREATE OR REPLACE FUNCTION get_token_filter(_search jsonb = '{}'::jsonb, token_rec jsonb DEFAULT NULL) RETURNS text AS $$
DECLARE
token_id text;
filters text[] := '{}';
prev boolean := TRUE;
field text;
dir text;
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
    _dir text NOT NULL
) ON COMMIT DROP;
INSERT INTO sorts (_field, _dir)
    SELECT DISTINCT ON (items_path(value->>'field'))
        items_path(value->>'field'),
        get_sort_dir(value)
    FROM
        jsonb_array_elements(coalesce(_search->'sort','[{"field":"datetime","direction":"desc"}]'))
    ;


SELECT _dir INTO dir FROM sorts ORDER BY _row ASC LIMIT 1;
IF EXISTS (SELECT 1 FROM sorts WHERE _field = 'id') THEN
    DELETE FROM sorts WHERE _row > (SELECT _row FROM sorts WHERE _field = 'id');
ELSE
    INSERT INTO sorts (_field, _dir) VALUES ('id', dir);
END IF;


-- Check for shortcuts if all sorts are the same direction
IF dir IS NOT NULL THEN
    SELECT format(
            '(%s) %s (%s)',
            concat_ws(', ', VARIADIC array_agg(quote_ident(_field))),
            CASE WHEN (prev AND dir = 'ASC') OR (NOT prev AND dir = 'DESC') THEN '<' ELSE '>' END,
            concat_ws(', ', VARIADIC array_agg(quote_literal(token_rec->>_field)))
    ) INTO output FROM sorts
    WHERE token_rec ? _field
    ;
END IF;


-- FOR field, dir IN SELECT value->>'field', value->>'direction' FROM jsonb_array_elements(
--         '[]'::jsonb
--         ||
--         coalesce(_search->'sort','[{"field":"datetime","direction":"desc"}]')
--         ||
--         '[{"field":"id","direction":"desc"}]'::jsonb
--     ) LOOP
--  filters := filters || format(
--      '%s %s %L',
--      items_path(field),
--      sort_dir_to_op(dir, prev),
--      token_rec->>items_path(field)
--  );
-- END LOOP;
-- RETURN concat_ws(' AND ', VARIADIC filters);
DROP TABLE IF EXISTS sorts;
RETURN output;
END;
$$ LANGUAGE PLPGSQL;

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

DROP FUNCTION IF EXISTS search;
CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
_where text := cql_to_where(_search);
token_where text := get_token_filter(_search);
full_where text;
orderby text := sort_sqlorderby(_search);
token_type text := substr(_search->>'token',1,4);
_limit int := coalesce((_search->>'limit')::int, 10);
curs refcursor;
exit_flag boolean := FALSE;
estimated_count bigint;
cntr bigint := 0;
first_record record;
last_record record;
out_records jsonb := '[]';
partitions_scanned int := 0;
prev_query text;
next text;
prev_id text;
prev text;
total_query text;
total_count bigint;
context jsonb;
collection jsonb;


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
    token_where := get_token_filter(_search);
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
        FETCH curs into last_record;
        EXIT WHEN NOT FOUND;
        cntr := cntr + 1;
        RAISE NOTICE 'cntr: %, id: %',cntr, last_record.id;
        IF cntr = 1 THEN
            first_record := last_record;
        END IF;
        IF cntr <= _limit THEN
            RAISE NOTICE 'cntr: %, limit: %', cntr, _limit;
            out_records := out_records || last_record.content;
            next := last_record.id;
        ELSIF cntr > _limit + 1 THEN
            exit_flag := TRUE;
            EXIT;
        END IF;
    END LOOP;
    IF exit_flag THEN
        exit;
    END IF;
END LOOP;

-- If we did not find an "extra" row then we don't return a next link
IF last_record IS NULL THEN
    next := null;
END IF;

IF token_type = 'prev' THEN
    next := prev;
END IF;

-- If this query has a token, see if there is data before the first record
IF _search ? 'token' THEN
    prev_query := format(
        'SELECT id FROM items WHERE %s ORDER BY %s LIMIT 1',
        concat_ws(
            ' AND ',
            _where,
            trim(get_token_filter(_search, to_jsonb(first_record)))
        ),
        sort_sqlorderby(_search, TRUE)
    );
    RAISE NOTICE 'Query to get previous record: %', prev_query;
    EXECUTE prev_query INTO prev_id;
    IF FOUND and prev_id is not null THEN
        IF token_type = 'prev' THEN
            next := prev_id;
        ELSE
            prev := prev_id;
        END IF;
    END IF;
END IF;

-- Flip things around if this was the result of a prev token query
IF token_type='prev' THEN
    out_records := flip_jsonb_array(out_records);
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
    'estimated_matched', estimated_count,
    'returned', jsonb_array_length(out_records)
));

collection := jsonb_build_object(
    'type', 'FeatureCollection',
    'features', out_records,
    'timeStamp', now(),
    'next', next,
    'prev', prev,
    'context', context
);

RETURN collection;
END;
$$ LANGUAGE PLPGSQL;

select * from search('{"filter":{"like":[{"property":"id"},"LC08"]}}');
--*/
