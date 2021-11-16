SET SEARCH_PATH to pgstac, public;
drop function if exists "pgstac"."context"();

drop function if exists "pgstac"."context_estimated_cost"();

drop function if exists "pgstac"."context_estimated_count"();

drop function if exists "pgstac"."context_stats_ttl"();

drop function if exists "pgstac"."get_setting"(setting text, INOUT _default anynonarray);

create table "pgstac"."pgstac_settings" (
    "name" text not null,
    "value" text not null
);


CREATE UNIQUE INDEX pgstac_settings_pkey ON pgstac.pgstac_settings USING btree (name);

alter table "pgstac"."pgstac_settings" add constraint "pgstac_settings_pkey" PRIMARY KEY using index "pgstac_settings_pkey";

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.context(conf jsonb DEFAULT NULL::jsonb)
 RETURNS text
 LANGUAGE sql
AS $function$
  SELECT get_setting('context', conf);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context_estimated_cost(conf jsonb DEFAULT NULL::jsonb)
 RETURNS double precision
 LANGUAGE sql
AS $function$
  SELECT get_setting('context_estimated_cost', conf)::float;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context_estimated_count(conf jsonb DEFAULT NULL::jsonb)
 RETURNS integer
 LANGUAGE sql
AS $function$
  SELECT get_setting('context_estimated_count', conf)::int;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context_stats_ttl(conf jsonb DEFAULT NULL::jsonb)
 RETURNS interval
 LANGUAGE sql
AS $function$
  SELECT get_setting('context_stats_ttl', conf)::interval;
$function$
;

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
        "between": "%s BETWEEN %s AND %s",
        "lower":" lower(%s)",
        "upper":" upper(%s)",
        "isnull": "%s IS NULL"
    }'::jsonb;
ret text;

BEGIN
IF j ? 'filter' THEN
    RETURN cql2_query(j->'filter');
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
    RETURN format(concat('(',ops->>op,')'), VARIADIC argstext);
END IF;

RAISE NOTICE '%op: %, argstext: %',repeat('     ', recursion), op, argstext;

RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_setting(_setting text, conf jsonb DEFAULT NULL::jsonb)
 RETURNS text
 LANGUAGE sql
AS $function$
SELECT COALESCE(
  conf->>_setting,
  current_setting(concat('pgstac.',_setting), TRUE),
  (SELECT value FROM pgstac_settings WHERE name=_setting)
);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.cql_to_where(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
search jsonb := _search;
_where text;
BEGIN

RAISE NOTICE 'SEARCH CQL Final: %', search;
IF (search ? 'filter-lang' AND search->>'filter-lang' = 'cql-json') OR get_setting('default-filter-lang', _search->'conf')='cql-json' THEN
    search := query_to_cqlfilter(search);
    search := add_filters_to_cql(search);
    _where := cql_query_op(search->'filter');
ELSE
    _where := cql2_query(search->'filter');
END IF;

IF trim(_where) = '' THEN
    _where := NULL;
END IF;
_where := coalesce(_where, ' TRUE ');
RETURN _where;
END;
$function$
;



SELECT set_version('0.4.0');
