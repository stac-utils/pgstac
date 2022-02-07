SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.base_stac_query(j jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
_where text := '';
dtrange tstzrange;
geom geometry;
BEGIN

    IF j ? 'ids' THEN
        _where := format('%s AND id = ANY (%L) ', _where, textarr(j->'ids'));
    END IF;
    IF j ? 'collections' THEN
        _where := format('%s AND collection_id = ANY (%L) ', _where, textarr(j->'collections'));
    END IF;

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        _where := format('%s AND datetime <= %L::timestamptz AND end_datetime >= %L::timestamptz ',
            _where,
            upper(dtrange),
            lower(dtrange)
        );
    END IF;

    IF j ? 'bbox' THEN
        geom := bbox_geom(j->'bbox');
        _where := format('%s AND geometry && %L',
            _where,
            geom
        );
    END IF;

    IF j ? 'intersects' THEN
        geom := st_fromgeojson(j->>'intersects');
        _where := format('%s AND st_intersects(geometry, %L)',
            _where,
            geom
        );
    END IF;

    RETURN _where;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.item_by_id(_id text)
 RETURNS pgstac.items
 LANGUAGE plpgsql
AS $function$
DECLARE
    i items%ROWTYPE;
BEGIN
    SELECT * INTO i FROM items WHERE id=_id LIMIT 1;
    RETURN i;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.cql_to_where(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
filterlang text;
search jsonb := _search;
base_where text;
_where text;
BEGIN

RAISE NOTICE 'SEARCH CQL Final: %', search;
filterlang := COALESCE(
    search->>'filter-lang',
    get_setting('default-filter-lang', _search->'conf')
);

base_where := base_stac_query(search);

IF filterlang = 'cql-json' THEN
    search := query_to_cqlfilter(search);
    -- search := add_filters_to_cql(search);
    _where := cql_query_op(search->'filter');
ELSE
    _where := cql2_query(search->'filter');
END IF;

IF trim(_where) = '' THEN
    _where := NULL;
END IF;
_where := coalesce(_where, ' TRUE ');
RETURN format('( %s ) %s ', _where, base_where);
END;
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
    token_where text;
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
            RETURN NULL;
        END IF;
        prev := (_search->>'token' ILIKE 'prev:%');
        token_id := substr(_search->>'token', 6);
        SELECT to_jsonb(items) INTO token_rec FROM item_by_id(token_id) as items;
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
            jsonb_array_elements(coalesce(_search->'sortby','[{"field":"datetime","direction":"desc"}]'))
    ON CONFLICT DO NOTHING
    ;
    RAISE NOTICE 'sorts 1: %', (SELECT jsonb_agg(to_json(sorts)) FROM sorts);
    -- Get the first sort direction provided. As the id is a primary key, if there are any
    -- sorts after id they won't do anything, so make sure that id is the last sort item.
    SELECT _dir INTO dir FROM sorts ORDER BY _row ASC LIMIT 1;
    IF EXISTS (SELECT 1 FROM sorts WHERE _field = 'id') THEN
        DELETE FROM sorts WHERE _row > (SELECT _row FROM sorts WHERE _field = 'id' ORDER BY _row ASC);
    ELSE
        INSERT INTO sorts (_field, _dir) VALUES ('id', dir);
    END IF;

    -- Add value from looked up item to the sorts table
    UPDATE sorts SET _val=quote_literal(token_rec->>_field);

    -- Check if all sorts are the same direction and use row comparison
    -- to filter
    RAISE NOTICE 'sorts 2: %', (SELECT jsonb_agg(to_json(sorts)) FROM sorts);

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
    token_where := concat('(',coalesce(output,'true'),')');
    IF trim(token_where) = '' THEN
        token_where := NULL;
    END IF;
    RAISE NOTICE 'TOKEN_WHERE: |%|',token_where;
    RETURN token_where;
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
    id text;
BEGIN
CREATE TEMP TABLE results (content jsonb) ON COMMIT DROP;
-- if ids is set, short circuit and just use direct ids query for each id
-- skip any paging or caching
-- hard codes ordering in the same order as the array of ids
IF _search ? 'ids' THEN
    FOR id IN SELECT jsonb_array_elements_text(_search->'ids') LOOP
        INSERT INTO results (content) SELECT content FROM item_by_id(id);
    END LOOP;
    SELECT INTO total_count count(*) FROM results;
ELSE
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
END IF;

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

CREATE OR REPLACE FUNCTION pgstac.where_stats(inwhere text, updatestats boolean DEFAULT false, conf jsonb DEFAULT NULL::jsonb)
 RETURNS pgstac.search_wheres
 LANGUAGE plpgsql
AS $function$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    partitions text[];
    sw search_wheres%ROWTYPE;
    inwhere_hash text := md5(inwhere);
BEGIN
    SELECT * INTO sw FROM search_wheres WHERE _where=inwhere_hash FOR UPDATE;

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
        WHERE _where = inwhere_hash
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


    INSERT INTO search_wheres
        (_where, lastused, usecount, statslastupdated, estimated_count, estimated_cost, time_to_estimate, partitions, total_count, time_to_count)
    SELECT sw._where, sw.lastused, sw.usecount, sw.statslastupdated, sw.estimated_count, sw.estimated_cost, sw.time_to_estimate, sw.partitions, sw.total_count, sw.time_to_count
    ON CONFLICT ((md5(_where)))
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



SELECT set_version('0.4.4');
