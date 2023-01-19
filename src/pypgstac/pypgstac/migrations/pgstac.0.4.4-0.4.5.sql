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
        geom := st_geomfromgeojson(j->>'intersects');
        _where := format('%s AND st_intersects(geometry, %L)',
            _where,
            geom
        );
    END IF;

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
        ON CONFLICT DO NOTHING
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
    id text;
BEGIN
CREATE TEMP TABLE results (content jsonb) ON COMMIT DROP;
-- if ids is set, short circuit and just use direct ids query for each id
-- skip any paging or caching
-- hard codes ordering in the same order as the array of ids
IF _search ? 'ids' THEN
    FOR id IN SELECT jsonb_array_elements_text(_search->'ids') LOOP
        INSERT INTO results (content) SELECT content FROM item_by_id(id) WHERE content IS NOT NULL;
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

SELECT jsonb_agg(content) INTO out_records FROM results WHERE content is not NULL;

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



SELECT set_version('0.4.5');
