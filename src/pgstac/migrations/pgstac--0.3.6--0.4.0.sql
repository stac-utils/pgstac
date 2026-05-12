SET SEARCH_PATH to pgstac, public;
drop function if exists "pgstac"."context"();

drop function if exists "pgstac"."context_estimated_cost"();

drop function if exists "pgstac"."context_estimated_count"();

drop function if exists "pgstac"."context_stats_ttl"();

drop function if exists "pgstac"."get_setting"(setting text, INOUT _default anynonarray);

drop function if exists "pgstac"."where_stats"(inwhere text, updatestats boolean);

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

CREATE OR REPLACE FUNCTION pgstac.where_stats(inwhere text, updatestats boolean DEFAULT false, conf jsonb DEFAULT NULL::jsonb)
 RETURNS search_wheres
 LANGUAGE plpgsql
AS $function$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    partitions text[];
    sw search_wheres%ROWTYPE;
BEGIN
    SELECT * INTO sw FROM search_wheres WHERE _where=inwhere FOR UPDATE;

    -- Update statistics if explicitly set, if statistics do not exist, or statistics ttl has expired
    IF NOT updatestats THEN
        RAISE NOTICE 'Checking if update is needed.';
        RAISE NOTICE 'Stats Last Updated: %', sw.statslastupdated;
        RAISE NOTICE 'TTL: %, Age: %', context_stats_ttl(conf), now() - sw.statslastupdated;
        RAISE NOTICE 'Context: %, Existing Total: %', context(conf), sw.total_count;
        IF
            sw.statslastupdated IS NULL
            OR (now() - sw.statslastupdated) > context_stats_ttl(conf)
            OR (context(conf) != 'off' AND sw.total_count IS NULL)
        THEN
            updatestats := TRUE;
        END IF;
    END IF;

    sw._where := inwhere;
    sw.lastused := now();
    sw.usecount := coalesce(sw.usecount,0) + 1;

    IF NOT updatestats THEN
        UPDATE search_wheres SET
            lastused = sw.lastused,
            usecount = sw.usecount
        WHERE _where = inwhere
        RETURNING * INTO sw
        ;
        RETURN sw;
    END IF;
    -- Use explain to get estimated count/cost and a list of the partitions that would be hit by the query
    t := clock_timestamp();
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
    INTO explain_json;
    RAISE NOTICE 'Time for just the explain: %', clock_timestamp() - t;
    WITH t AS (
        SELECT j->>0 as p FROM
            jsonb_path_query(
                explain_json,
                'strict $.**."Relation Name" ? (@ != null)'
            ) j
    ), ordered AS (
        SELECT p FROM t ORDER BY p DESC
        -- SELECT p FROM t JOIN items_partitions
        --     ON (t.p = items_partitions.partition)
        -- ORDER BY pstart DESC
    )
    SELECT array_agg(p) INTO partitions FROM ordered;
    i := clock_timestamp() - t;
    RAISE NOTICE 'Time for explain + join: %', clock_timestamp() - t;



    sw.statslastupdated := now();
    sw.estimated_count := explain_json->0->'Plan'->'Plan Rows';
    sw.estimated_cost := explain_json->0->'Plan'->'Total Cost';
    sw.time_to_estimate := extract(epoch from i);
    sw.partitions := partitions;

    -- Do a full count of rows if context is set to on or if auto is set and estimates are low enough
    IF
        context(conf) = 'on'
        OR
        ( context(conf) = 'auto' AND
            (
                sw.estimated_count < context_estimated_count(conf)
                OR
                sw.estimated_cost < context_estimated_cost(conf)
            )
        )
    THEN
        t := clock_timestamp();
        RAISE NOTICE 'Calculating actual count...';
        EXECUTE format(
            'SELECT count(*) FROM items WHERE %s',
            inwhere
        ) INTO sw.total_count;
        i := clock_timestamp() - t;
        RAISE NOTICE 'Actual Count: % -- %', sw.total_count, i;
        sw.time_to_count := extract(epoch FROM i);
    ELSE
        sw.total_count := NULL;
        sw.time_to_count := NULL;
    END IF;


    INSERT INTO search_wheres SELECT sw.*
    ON CONFLICT (_where)
    DO UPDATE
        SET
            lastused = sw.lastused,
            usecount = sw.usecount,
            statslastupdated = sw.statslastupdated,
            estimated_count = sw.estimated_count,
            estimated_cost = sw.estimated_cost,
            time_to_estimate = sw.time_to_estimate,
            partitions = sw.partitions,
            total_count = sw.total_count,
            time_to_count = sw.time_to_count
    ;
    RETURN sw;
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
args text[] := NULL;

BEGIN
RAISE NOTICE 'j: %, op: %, jtype: %', j, op, jtype;

-- for in, convert value, list to array syntax to match other ops
IF op = 'in'  and j ? 'value' and j ? 'list' THEN
    j := jsonb_build_array( j->'value', j->'list');
    jtype := 'array';
    RAISE NOTICE 'IN: j: %, jtype: %', j, jtype;
END IF;

IF op = 'between' and j ? 'value' and j ? 'lower' and j ? 'upper' THEN
    j := jsonb_build_array( j->'value', j->'lower', j->'upper');
    jtype := 'array';
    RAISE NOTICE 'BETWEEN: j: %, jtype: %', j, jtype;
END IF;

IF op = 'not' AND jtype = 'object' THEN
    j := jsonb_build_array( j );
    jtype := 'array';
    RAISE NOTICE 'NOT: j: %, jtype: %', j, jtype;
END IF;

-- Set Lower Case on Both Arguments When Case Insensitive Flag Set
IF op in ('eq','lt','lte','gt','gte','like') AND jsonb_typeof(j->2) = 'boolean' THEN
    IF (j->>2)::boolean THEN
        RETURN format(concat('(',ops->>op,')'), cql_query_op(jsonb_build_array(j->0), 'lower'), cql_query_op(jsonb_build_array(j->1), 'lower'));
    END IF;
END IF;

-- Special Case when comparing a property in a jsonb field to a string or number using eq
-- Allows to leverage GIN index on jsonb fields
IF op = 'eq' THEN
    IF j->0 ? 'property'
        AND jsonb_typeof(j->1) IN ('number','string')
        AND (items_path(j->0->>'property')).eq IS NOT NULL
    THEN
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

IF jtype = 'array' AND op IS NULL THEN
    RAISE NOTICE 'Parsing array into array arg. j: %', j;
    SELECT format($f$ '{%s}'::text[] $f$, string_agg(e,',')) INTO ret FROM jsonb_array_elements_text(j) e;
    RETURN ret;
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
    --RETURN args;
END IF;
RAISE NOTICE 'ARGS after array cleaning: %', args;

IF op IS NULL THEN
    RETURN args::text[];
END IF;

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

CREATE OR REPLACE FUNCTION pgstac.items_staging_ignore_insert_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    p record;
    _partitions text[];
BEGIN
    CREATE TEMP TABLE new_partitions ON COMMIT DROP AS
    SELECT
        items_partition_name(stac_datetime(content)) as partition,
        date_trunc('week', min(stac_datetime(content))) as partition_start
    FROM newdata
    GROUP BY 1;

    -- set statslastupdated in cache to be old enough cache always regenerated

    SELECT array_agg(partition) INTO _partitions FROM new_partitions;
    UPDATE search_wheres
        SET
            statslastupdated = NULL
        WHERE _where IN (
            SELECT _where FROM search_wheres sw WHERE sw.partitions && _partitions
            FOR UPDATE SKIP LOCKED
        )
    ;

    FOR p IN SELECT new_partitions.partition, new_partitions.partition_start, new_partitions.partition_start + '1 week'::interval as partition_end FROM new_partitions
    LOOP
        RAISE NOTICE 'Getting partition % ready.', p.partition;
        IF NOT items_partition_exists(p.partition) THEN
            RAISE NOTICE 'Creating partition %.', p.partition;
            PERFORM items_partition_create_worker(p.partition, p.partition_start, p.partition_end);
        END IF;
        PERFORM drop_partition_constraints(p.partition);
    END LOOP;

    INSERT INTO items (id, geometry, collection_id, datetime, end_datetime, properties, content)
        SELECT
            content->>'id',
            stac_geom(content),
            content->>'collection',
            stac_datetime(content),
            stac_end_datetime(content),
            properties_idx(content),
            content
        FROM newdata
        ON CONFLICT (datetime, id) DO NOTHING
    ;
    DELETE FROM items_staging;


    FOR p IN SELECT new_partitions.partition FROM new_partitions
    LOOP
        RAISE NOTICE 'Setting constraints for partition %.', p.partition;
        PERFORM partition_checks(p.partition);
    END LOOP;
    DROP TABLE IF EXISTS new_partitions;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_staging_insert_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    p record;
    _partitions text[];
BEGIN
    CREATE TEMP TABLE new_partitions ON COMMIT DROP AS
    SELECT
        items_partition_name(stac_datetime(content)) as partition,
        date_trunc('week', min(stac_datetime(content))) as partition_start
    FROM newdata
    GROUP BY 1;

    -- set statslastupdated in cache to be old enough cache always regenerated

    SELECT array_agg(partition) INTO _partitions FROM new_partitions;
    UPDATE search_wheres
        SET
            statslastupdated = NULL
        WHERE _where IN (
            SELECT _where FROM search_wheres sw WHERE sw.partitions && _partitions
            FOR UPDATE SKIP LOCKED
        )
    ;
    FOR p IN SELECT new_partitions.partition, new_partitions.partition_start, new_partitions.partition_start + '1 week'::interval as partition_end FROM new_partitions
    LOOP
        RAISE NOTICE 'Getting partition % ready.', p.partition;
        IF NOT items_partition_exists(p.partition) THEN
            RAISE NOTICE 'Creating partition %.', p.partition;
            PERFORM items_partition_create_worker(p.partition, p.partition_start, p.partition_end);
        END IF;
        PERFORM drop_partition_constraints(p.partition);
    END LOOP;
    INSERT INTO items (id, geometry, collection_id, datetime, end_datetime, properties, content)
        SELECT
            content->>'id',
            stac_geom(content),
            content->>'collection',
            stac_datetime(content),
            stac_end_datetime(content),
            properties_idx(content),
            content
        FROM newdata
    ;
    DELETE FROM items_staging;


    FOR p IN SELECT new_partitions.partition FROM new_partitions
    LOOP
        RAISE NOTICE 'Setting constraints for partition %.', p.partition;
        PERFORM partition_checks(p.partition);
    END LOOP;
    DROP TABLE IF EXISTS new_partitions;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_staging_upsert_insert_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    p record;
    _partitions text[];
BEGIN
    CREATE TEMP TABLE new_partitions ON COMMIT DROP AS
    SELECT
        items_partition_name(stac_datetime(content)) as partition,
        date_trunc('week', min(stac_datetime(content))) as partition_start
    FROM newdata
    GROUP BY 1;

    -- set statslastupdated in cache to be old enough cache always regenerated

    SELECT array_agg(partition) INTO _partitions FROM new_partitions;
    UPDATE search_wheres
        SET
            statslastupdated = NULL
        WHERE _where IN (
            SELECT _where FROM search_wheres sw WHERE sw.partitions && _partitions
            FOR UPDATE SKIP LOCKED
        )
    ;

    FOR p IN SELECT new_partitions.partition, new_partitions.partition_start, new_partitions.partition_start + '1 week'::interval as partition_end FROM new_partitions
    LOOP
        RAISE NOTICE 'Getting partition % ready.', p.partition;
        IF NOT items_partition_exists(p.partition) THEN
            RAISE NOTICE 'Creating partition %.', p.partition;
            PERFORM items_partition_create_worker(p.partition, p.partition_start, p.partition_end);
        END IF;
        PERFORM drop_partition_constraints(p.partition);
    END LOOP;

    INSERT INTO items (id, geometry, collection_id, datetime, end_datetime, properties, content)
        SELECT
            content->>'id',
            stac_geom(content),
            content->>'collection',
            stac_datetime(content),
            stac_end_datetime(content),
            properties_idx(content),
            content
        FROM newdata
        ON CONFLICT (datetime, id) DO UPDATE SET
            content = EXCLUDED.content
            WHERE items.content IS DISTINCT FROM EXCLUDED.content
        ;
    DELETE FROM items_staging;


    FOR p IN SELECT new_partitions.partition FROM new_partitions
    LOOP
        RAISE NOTICE 'Setting constraints for partition %.', p.partition;
        PERFORM partition_checks(p.partition);
    END LOOP;
    DROP TABLE IF EXISTS new_partitions;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET jit TO 'off'
AS $function$
DECLARE
    searches searches%ROWTYPE;
    _where text;
    token_where text;
    full_where text;
    orderby text;
    query text;
    token_type text := substr(_search->>'token',1,4);
    _limit int := coalesce((_search->>'limit')::int, 10);
    curs refcursor;
    cntr int := 0;
    iter_record items%ROWTYPE;
    first_record items%ROWTYPE;
    last_record items%ROWTYPE;
    out_records jsonb := '[]'::jsonb;
    prev_query text;
    next text;
    prev_id text;
    has_next boolean := false;
    has_prev boolean := false;
    prev text;
    total_count bigint;
    context jsonb;
    collection jsonb;
    includes text[];
    excludes text[];
    exit_flag boolean := FALSE;
    batches int := 0;
    timer timestamptz := clock_timestamp();
    pstart timestamptz;
    pend timestamptz;
    pcurs refcursor;
    search_where search_wheres%ROWTYPE;
BEGIN
searches := search_query(_search);
_where := searches._where;
orderby := searches.orderby;
search_where := where_stats(_where);
total_count := coalesce(search_where.total_count, search_where.estimated_count);


IF token_type='prev' THEN
    token_where := get_token_filter(_search, null::jsonb);
    orderby := sort_sqlorderby(_search, TRUE);
END IF;
IF token_type='next' THEN
    token_where := get_token_filter(_search, null::jsonb);
END IF;

full_where := concat_ws(' AND ', _where, token_where);
RAISE NOTICE 'FULL QUERY % %', full_where, clock_timestamp()-timer;
timer := clock_timestamp();

CREATE TEMP TABLE results (content jsonb) ON COMMIT DROP;


FOR query IN SELECT partition_queries(full_where, orderby, search_where.partitions) LOOP
    timer := clock_timestamp();
    query := format('%s LIMIT %s', query, _limit + 1);
    RAISE NOTICE 'Partition Query: %', query;
    batches := batches + 1;
    -- curs = create_cursor(query);
    OPEN curs FOR EXECUTE query;
    LOOP
        FETCH curs into iter_record;
        EXIT WHEN NOT FOUND;
        cntr := cntr + 1;
        last_record := iter_record;
        IF cntr = 1 THEN
            first_record := last_record;
        END IF;
        IF cntr <= _limit THEN
            INSERT INTO results (content) VALUES (last_record.content);
            -- out_records := out_records || last_record.content;

        ELSIF cntr > _limit THEN
            has_next := true;
            exit_flag := true;
            EXIT;
        END IF;
    END LOOP;
    CLOSE curs;
    RAISE NOTICE 'Query took %. Total Time %', clock_timestamp()-timer, ftime();
    timer := clock_timestamp();
    EXIT WHEN exit_flag;
END LOOP;
RAISE NOTICE 'Scanned through % partitions.', batches;

SELECT jsonb_agg(content) INTO out_records FROM results;

DROP TABLE results;


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
    SELECT jsonb_agg(filter_jsonb(row, includes, excludes)) INTO out_records FROM jsonb_array_elements(out_records) row;
END IF;

IF context(_search->'conf') != 'off' THEN
    context := jsonb_strip_nulls(jsonb_build_object(
        'limit', _limit,
        'matched', total_count,
        'returned', coalesce(jsonb_array_length(out_records), 0)
    ));
ELSE
    context := jsonb_strip_nulls(jsonb_build_object(
        'limit', _limit,
        'returned', coalesce(jsonb_array_length(out_records), 0)
    ));
END IF;

collection := jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce(out_records, '[]'::jsonb),
    'next', next,
    'prev', prev,
    'context', context
);

RETURN collection;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search_query(_search jsonb DEFAULT '{}'::jsonb, updatestats boolean DEFAULT false, _metadata jsonb DEFAULT '{}'::jsonb)
 RETURNS searches
 LANGUAGE plpgsql
AS $function$
DECLARE
    search searches%ROWTYPE;
    pexplain jsonb;
    t timestamptz;
    i interval;
BEGIN
SELECT * INTO search FROM searches
WHERE hash=search_hash(_search, _metadata) FOR UPDATE;

-- Calculate the where clause if not already calculated
IF search._where IS NULL THEN
    search._where := cql_to_where(_search);
END IF;

-- Calculate the order by clause if not already calculated
IF search.orderby IS NULL THEN
    search.orderby := sort_sqlorderby(_search);
END IF;

PERFORM where_stats(search._where, updatestats, _search->'conf');

search.lastused := now();
search.usecount := coalesce(search.usecount, 0) + 1;
INSERT INTO searches (search, _where, orderby, lastused, usecount, metadata)
VALUES (_search, search._where, search.orderby, search.lastused, search.usecount, _metadata)
ON CONFLICT (hash) DO
UPDATE SET
    _where = EXCLUDED._where,
    orderby = EXCLUDED.orderby,
    lastused = EXCLUDED.lastused,
    usecount = EXCLUDED.usecount,
    metadata = EXCLUDED.metadata
RETURNING * INTO search
;
RETURN search;

END;
$function$
;

INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '1000000'),
  ('context_stats_ttl', '1 day'),
  ('default-filter-lang', 'cql2-json')
ON CONFLICT DO NOTHING
;


SELECT set_version('0.4.0');
