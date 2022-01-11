SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.cql2_query(j jsonb, recursion integer DEFAULT 0)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
args jsonb := j->'args';
jtype text := jsonb_typeof(j->'args');
op text := lower(j->>'op');
arg jsonb;
argtext text;
argstext text[] := '{}'::text[];
inobj jsonb;
_numeric text := '';
ops jsonb :=
    '{
        "eq": "%s = %s",
        "lt": "%s < %s",
        "lte": "%s <= %s",
        "gt": "%s > %s",
        "gte": "%s >= %s",
        "le": "%s <= %s",
        "ge": "%s >= %s",
        "=": "%s = %s",
        "<": "%s < %s",
        "<=": "%s <= %s",
        ">": "%s > %s",
        ">=": "%s >= %s",
        "like": "%s LIKE %s",
        "ilike": "%s ILIKE %s",
        "+": "%s + %s",
        "-": "%s - %s",
        "*": "%s * %s",
        "/": "%s / %s",
        "in": "%s = ANY (%s)",
        "not": "NOT (%s)",
        "between": "%s BETWEEN (%2$s)[1] AND (%2$s)[2]",
        "lower":" lower(%s)",
        "upper":" upper(%s)",
        "isnull": "%s IS NULL"
    }'::jsonb;
ret text;

BEGIN
RAISE NOTICE 'j: %s', j;
IF j ? 'filter' THEN
    RETURN cql2_query(j->'filter');
END IF;

IF j ? 'upper' THEN
RAISE NOTICE 'upper %s',jsonb_build_object(
            'op', 'upper',
            'args', jsonb_build_array( j-> 'upper')
        ) ;
    RETURN cql2_query(
        jsonb_build_object(
            'op', 'upper',
            'args', jsonb_build_array( j-> 'upper')
        )
    );
END IF;

IF j ? 'lower' THEN
    RETURN cql2_query(
        jsonb_build_object(
            'op', 'lower',
            'args', jsonb_build_array( j-> 'lower')
        )
    );
END IF;

IF j ? 'args' AND jsonb_typeof(args) != 'array' THEN
    args := jsonb_build_array(args);
END IF;
-- END Cases where no further nesting is expected
IF j ? 'op' THEN
    -- Special case to use JSONB index for equality
    IF op = 'eq'
        AND args->0 ? 'property'
        AND jsonb_typeof(args->1) IN ('number', 'string')
        AND (items_path(args->0->>'property')).eq IS NOT NULL
    THEN
        RETURN format((items_path(args->0->>'property')).eq, args->1);
    END IF;

    -- Temporal Query
    IF op ilike 't_%' or op = 'anyinteracts' THEN
        RETURN temporal_op_query(op, args);
    END IF;

    -- Spatial Query
    IF op ilike 's_%' or op = 'intersects' THEN
        RETURN spatial_op_query(op, args);
    END IF;

    -- In Query - separate into separate eq statements so that we can use eq jsonb optimization
    IF op = 'in' THEN
        RAISE NOTICE '% IN args: %', repeat('     ', recursion), args;
        SELECT INTO inobj
            jsonb_agg(
                jsonb_build_object(
                    'op', 'eq',
                    'args', jsonb_build_array( args->0 , v)
                )
            )
        FROM jsonb_array_elements( args->1) v;
        RETURN cql2_query(jsonb_build_object('op','or','args',inobj));
    END IF;
END IF;

IF j ? 'property' THEN
    RETURN (items_path(j->>'property')).path_txt;
END IF;

IF j ? 'timestamp' THEN
    RETURN quote_literal(j->>'timestamp');
END IF;

RAISE NOTICE '%jtype: %',repeat('     ', recursion), jtype;
IF jsonb_typeof(j) = 'number' THEN
    RETURN format('%L::numeric', j->>0);
END IF;

IF jsonb_typeof(j) = 'string' THEN
    RETURN quote_literal(j->>0);
END IF;

IF jsonb_typeof(j) = 'array' THEN
    IF j @? '$[*] ? (@.type() == "number")' THEN
        RETURN CONCAT(quote_literal(textarr(j)::text), '::numeric[]');
    ELSE
        RETURN CONCAT(quote_literal(textarr(j)::text), '::text[]');
    END IF;
END IF;
RAISE NOTICE 'ARGS after array cleaning: %', args;

RAISE NOTICE '%beforeargs op: %, args: %',repeat('     ', recursion), op, args;
IF j ? 'args' THEN
    FOR arg in SELECT * FROM jsonb_array_elements(args) LOOP
        argtext := cql2_query(arg, recursion + 1);
        RAISE NOTICE '%     -- arg: %, argtext: %', repeat('     ', recursion), arg, argtext;
        argstext := argstext || argtext;
    END LOOP;
END IF;
RAISE NOTICE '%afterargs op: %, argstext: %',repeat('     ', recursion), op, argstext;


IF op IN ('and', 'or') THEN
    RAISE NOTICE 'inand op: %, argstext: %', op, argstext;
    SELECT
        concat(' ( ',array_to_string(array_agg(e), concat(' ',op,' ')),' ) ')
        INTO ret
        FROM unnest(argstext) e;
        RETURN ret;
END IF;

IF ops ? op THEN
    IF argstext[2] ~* 'numeric' THEN
        argstext := ARRAY[concat('(',argstext[1],')::numeric')] || argstext[2:3];
    END IF;
    RETURN format(concat('(',ops->>op,')'), VARIADIC argstext);
END IF;

RAISE NOTICE '%op: %, argstext: %',repeat('     ', recursion), op, argstext;

RETURN NULL;
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
        WHEN _indate ? 'timestamp' THEN
            ARRAY[_indate->>'timestamp', 'infinity']
        WHEN _indate ? 'interval' THEN
            textarr(_indate->'interval')
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



SELECT set_version('0.4.2');
