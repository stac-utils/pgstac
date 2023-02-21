SET SEARCH_PATH to pgstac, public;
alter table "pgstac"."partitions" drop constraint "partitions_collection_fkey";

drop function if exists "pgstac"."content_hydrate"(_item jsonb, _collection jsonb, fields jsonb);

drop function if exists "pgstac"."content_slim"(_item jsonb, _collection jsonb);

drop function if exists "pgstac"."key_filter"(k text, val jsonb, INOUT kf jsonb, OUT include boolean);

drop function if exists "pgstac"."strip_assets"(a jsonb);

alter table "pgstac"."partitions" add constraint "partitions_collection_fkey" FOREIGN KEY (collection) REFERENCES pgstac.collections(id) ON DELETE CASCADE;

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.content_hydrate(_base_item jsonb, _item jsonb, fields jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT merge_jsonb(
            jsonb_fields(_item, fields),
            jsonb_fields(_base_item, fields)
    );
$function$
;

CREATE OR REPLACE FUNCTION pgstac.explode_dotpaths(j jsonb)
 RETURNS SETOF text[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT string_to_array(p, '.') as e FROM jsonb_array_elements_text(j) p;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.explode_dotpaths_recurse(j jsonb)
 RETURNS SETOF text[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    WITH RECURSIVE t AS (
        SELECT e FROM explode_dotpaths(j) e
        UNION ALL
        SELECT e[1:cardinality(e)-1]
        FROM t
        WHERE cardinality(e)>1
    ) SELECT e FROM t;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.jsonb_exclude(j jsonb, f jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
    excludes jsonb := f-> 'exclude';
    outj jsonb := j;
    path text[];
BEGIN
    IF
        excludes IS NULL
        OR jsonb_array_length(excludes) = 0
    THEN
        RETURN j;
    ELSE
        FOR path IN SELECT explode_dotpaths(excludes) LOOP
            outj := outj #- path;
        END LOOP;
    END IF;
    RETURN outj;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.jsonb_fields(j jsonb, f jsonb DEFAULT '{"fields": []}'::jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT jsonb_exclude(jsonb_include(j, f), f);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.jsonb_include(j jsonb, f jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
    includes jsonb := f-> 'include';
    outj jsonb := '{}'::jsonb;
    path text[];
BEGIN
    IF
        includes IS NULL
        OR jsonb_array_length(includes) = 0
    THEN
        RETURN j;
    ELSE
        includes := includes || '["id","collection"]'::jsonb;
        FOR path IN SELECT explode_dotpaths(includes) LOOP
            outj := jsonb_set_nested(outj, path, j #> path);
        END LOOP;
    END IF;
    RETURN outj;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.jsonb_set_nested(j jsonb, path text[], val jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
BEGIN
    IF cardinality(path) > 1 THEN
        FOR i IN 1..(cardinality(path)-1) LOOP
            IF j #> path[:i] IS NULL THEN
                j := jsonb_set_lax(j, path[:i], '{}', TRUE);
            END IF;
        END LOOP;
    END IF;
    RETURN jsonb_set_lax(j, path, val, true);

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.merge_jsonb(_a jsonb, _b jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT
    CASE
        WHEN _a = '"𒍟※"'::jsonb THEN NULL
        WHEN _a IS NULL OR jsonb_typeof(_a) = 'null' THEN _b
        WHEN jsonb_typeof(_a) = 'object' AND jsonb_typeof(_b) = 'object' THEN
            (
                SELECT
                    jsonb_strip_nulls(
                        jsonb_object_agg(
                            key,
                            merge_jsonb(a.value, b.value)
                        )
                    )
                FROM
                    jsonb_each(coalesce(_a,'{}'::jsonb)) as a
                FULL JOIN
                    jsonb_each(coalesce(_b,'{}'::jsonb)) as b
                USING (key)
            )
        WHEN
            jsonb_typeof(_a) = 'array'
            AND jsonb_typeof(_b) = 'array'
            AND jsonb_array_length(_a) = jsonb_array_length(_b)
        THEN
            (
                SELECT jsonb_agg(m) FROM
                    ( SELECT
                        merge_jsonb(
                            jsonb_array_elements(_a),
                            jsonb_array_elements(_b)
                        ) as m
                    ) as l
            )
        ELSE _a
    END
    ;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partitions_delete_trigger_func()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    q text;
BEGIN
    RAISE NOTICE 'Partition Delete Trigger. %', OLD.name;
    EXECUTE format($q$
            DROP TABLE IF EXISTS %I CASCADE;
            $q$,
            OLD.name
        );
    RAISE NOTICE 'Dropped partition.';
    RETURN OLD;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.strip_jsonb(_a jsonb, _b jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT
    CASE

        WHEN (_a IS NULL OR jsonb_typeof(_a) = 'null') AND _b IS NOT NULL AND jsonb_typeof(_b) != 'null' THEN '"𒍟※"'::jsonb
        WHEN _b IS NULL OR jsonb_typeof(_a) = 'null' THEN _a
        WHEN _a = _b AND jsonb_typeof(_a) = 'object' THEN '{}'::jsonb
        WHEN _a = _b THEN NULL
        WHEN jsonb_typeof(_a) = 'object' AND jsonb_typeof(_b) = 'object' THEN
            (
                SELECT
                    jsonb_strip_nulls(
                        jsonb_object_agg(
                            key,
                            strip_jsonb(a.value, b.value)
                        )
                    )
                FROM
                    jsonb_each(_a) as a
                FULL JOIN
                    jsonb_each(_b) as b
                USING (key)
            )
        WHEN
            jsonb_typeof(_a) = 'array'
            AND jsonb_typeof(_b) = 'array'
            AND jsonb_array_length(_a) = jsonb_array_length(_b)
        THEN
            (
                SELECT jsonb_agg(m) FROM
                    ( SELECT
                        strip_jsonb(
                            jsonb_array_elements(_a),
                            jsonb_array_elements(_b)
                        ) as m
                    ) as l
            )
        ELSE _a
    END
    ;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.collection_base_item(content jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT jsonb_build_object(
        'type', 'Feature',
        'stac_version', content->'stac_version',
        'assets', content->'item_assets',
        'collection', content->'id'
    );
$function$
;

CREATE OR REPLACE FUNCTION pgstac.collections_trigger_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    q text;
    partition_name text := format('_items_%s', NEW.key);
    partition_exists boolean := false;
    partition_empty boolean := true;
    err_context text;
    loadtemp boolean := FALSE;
BEGIN
    RAISE NOTICE 'Collection Trigger. % %', NEW.id, NEW.key;
    SELECT relid::text INTO partition_name
    FROM pg_partition_tree('items')
    WHERE relid::text = partition_name;
    IF FOUND THEN
        partition_exists := true;
        partition_empty := table_empty(partition_name);
    ELSE
        partition_exists := false;
        partition_empty := true;
        partition_name := format('_items_%s', NEW.key);
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS DISTINCT FROM OLD.partition_trunc AND partition_empty THEN
        q := format($q$
            DROP TABLE IF EXISTS %I CASCADE;
            $q$,
            partition_name
        );
        EXECUTE q;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS DISTINCT FROM OLD.partition_trunc AND partition_exists AND NOT partition_empty THEN
        q := format($q$
            CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
            DROP TABLE IF EXISTS %I CASCADE;
            $q$,
            partition_name,
            partition_name
        );
        EXECUTE q;
        loadtemp := TRUE;
        partition_empty := TRUE;
        partition_exists := FALSE;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS NOT DISTINCT FROM OLD.partition_trunc THEN
        RETURN NEW;
    END IF;
    IF NEW.partition_trunc IS NULL AND partition_empty THEN
        RAISE NOTICE '% % % %',
            partition_name,
            NEW.id,
            concat(partition_name,'_id_idx'),
            partition_name
        ;
        q := format($q$
            CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L);
            CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
            $q$,
            partition_name,
            NEW.id,
            concat(partition_name,'_id_idx'),
            partition_name
        );
        RAISE NOTICE 'q: %', q;
        BEGIN
            EXECUTE q;
            EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'Partition % already exists.', partition_name;
        WHEN others THEN
            GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
            RAISE INFO 'Error Name:%',SQLERRM;
            RAISE INFO 'Error State:%', SQLSTATE;
            RAISE INFO 'Error Context:%', err_context;
        END;

        ALTER TABLE partitions DISABLE TRIGGER partitions_delete_trigger;
        DELETE FROM partitions WHERE collection=NEW.id AND name=partition_name;
        ALTER TABLE partitions ENABLE TRIGGER partitions_delete_trigger;

        INSERT INTO partitions (collection, name) VALUES (NEW.id, partition_name);
    ELSIF partition_empty THEN
        q := format($q$
            CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L)
                PARTITION BY RANGE (datetime);
            $q$,
            partition_name,
            NEW.id
        );
        RAISE NOTICE 'q: %', q;
        BEGIN
            EXECUTE q;
            EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'Partition % already exists.', partition_name;
        WHEN others THEN
            GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
            RAISE INFO 'Error Name:%',SQLERRM;
            RAISE INFO 'Error State:%', SQLSTATE;
            RAISE INFO 'Error Context:%', err_context;
        END;
        ALTER TABLE partitions DISABLE TRIGGER partitions_delete_trigger;
        DELETE FROM partitions WHERE collection=NEW.id AND name=partition_name;
        ALTER TABLE partitions ENABLE TRIGGER partitions_delete_trigger;
    ELSE
        RAISE EXCEPTION 'Cannot modify partition % unless empty', partition_name;
    END IF;
    IF loadtemp THEN
        RAISE NOTICE 'Moving data into new partitions.';
         q := format($q$
            WITH p AS (
                SELECT
                    collection,
                    datetime as datetime,
                    end_datetime as end_datetime,
                    (partition_name(
                        collection,
                        datetime
                    )).partition_name as name
                FROM changepartitionstaging
            )
            INSERT INTO partitions (collection, datetime_range, end_datetime_range)
                SELECT
                    collection,
                    tstzrange(min(datetime), max(datetime), '[]') as datetime_range,
                    tstzrange(min(end_datetime), max(end_datetime), '[]') as end_datetime_range
                FROM p
                    GROUP BY collection, name
                ON CONFLICT (name) DO UPDATE SET
                    datetime_range = EXCLUDED.datetime_range,
                    end_datetime_range = EXCLUDED.end_datetime_range
            ;
            INSERT INTO %I SELECT * FROM changepartitionstaging;
            DROP TABLE IF EXISTS changepartitionstaging;
            $q$,
            partition_name
        );
        EXECUTE q;
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.content_hydrate(_item pgstac.items, _collection pgstac.collections, fields jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
    content jsonb;
    base_item jsonb := _collection.base_item;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := content_hydrate(
        jsonb_build_object(
            'id', _item.id,
            'geometry', geom,
            'collection', _item.collection,
            'type', 'Feature'
        ) || _item.content,
        _collection.base_item,
        fields
    );

    RETURN output;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.content_nonhydrated(_item pgstac.items, fields jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := jsonb_build_object(
                'id', _item.id,
                'geometry', geom,
                'collection', _item.collection,
                'type', 'Feature'
            ) || _item.content;
    RETURN output;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.content_slim(_item jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT strip_jsonb(_item - '{id,geometry,collection,type}'::text[], collection_base_item(_item->>'collection')) - '{id,geometry,collection,type}'::text[];
$function$
;

CREATE OR REPLACE FUNCTION pgstac.delete_item(_id text, _collection text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id AND (_collection IS NULL OR collection=_collection) RETURNING * INTO STRICT out;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.include_field(f text, fields jsonb DEFAULT '{}'::jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
    includes jsonb := fields->'include';
    excludes jsonb := fields->'exclude';
BEGIN
    IF f IS NULL THEN
        RETURN NULL;
    END IF;


    IF
        jsonb_typeof(excludes) = 'array'
        AND jsonb_array_length(excludes)>0
        AND excludes ? f
    THEN
        RETURN FALSE;
    END IF;

    IF
        (
            jsonb_typeof(includes) = 'array'
            AND jsonb_array_length(includes) > 0
            AND includes ? f
        ) OR
        (
            includes IS NULL
            OR jsonb_typeof(includes) = 'null'
            OR jsonb_array_length(includes) = 0
        )
    THEN
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_name(collection text, dt timestamp with time zone, OUT partition_name text, OUT partition_range tstzrange)
 RETURNS record
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    c RECORD;
    parent_name text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;
    parent_name := format('_items_%s', c.key);


    IF c.partition_trunc = 'year' THEN
        partition_name := format('%s_%s', parent_name, to_char(dt,'YYYY'));
    ELSIF c.partition_trunc = 'month' THEN
        partition_name := format('%s_%s', parent_name, to_char(dt,'YYYYMM'));
    ELSE
        partition_name := parent_name;
        partition_range := tstzrange('-infinity'::timestamptz, 'infinity'::timestamptz, '[]');
    END IF;
    IF partition_range IS NULL THEN
        partition_range := tstzrange(
            date_trunc(c.partition_trunc::text, dt),
            date_trunc(c.partition_trunc::text, dt) + concat('1 ', c.partition_trunc)::interval
        );
    END IF;
    RETURN;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pgstac', 'public'
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
    first_record jsonb;
    first_item items%ROWTYPE;
    last_item items%ROWTYPE;
    last_record jsonb;
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
    INSERT INTO results
    SELECT
        CASE WHEN _search->'conf'->>'nohydrate' IS NOT NULL AND (_search->'conf'->>'nohydrate')::boolean = true THEN
            content_nonhydrated(items, _search->'fields')
        ELSE
            content_hydrate(items, _search->'fields')
        END
    FROM items WHERE
        items.id = ANY(to_text_array(_search->'ids'))
        AND
            CASE WHEN _search ? 'collections' THEN
                items.collection = ANY(to_text_array(_search->'collections'))
            ELSE TRUE
            END
    ORDER BY items.datetime desc, items.id desc
    ;
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

            IF _search->'conf'->>'nohydrate' IS NOT NULL AND (_search->'conf'->>'nohydrate')::boolean = true THEN
                last_record := content_nonhydrated(iter_record, _search->'fields');
            ELSE
                last_record := content_hydrate(iter_record, _search->'fields');
            END IF;
            last_item := iter_record;
            IF cntr = 1 THEN
                first_item := last_item;
                first_record := last_record;
            END IF;
            IF cntr <= _limit THEN
                INSERT INTO results (content) VALUES (last_record);
            ELSIF cntr > _limit THEN
                has_next := true;
                exit_flag := true;
                EXIT;
            END IF;
        END LOOP;
        CLOSE curs;
        RAISE NOTICE 'Query took %.', clock_timestamp()-timer;
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
            trim(get_token_filter(_search, to_jsonb(first_item)))
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

IF has_prev THEN
    prev := out_records->0->>'id';
END IF;
IF has_next OR token_type='prev' THEN
    next := out_records->-1->>'id';
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

CREATE OR REPLACE FUNCTION pgstac.stac_daterange(value jsonb)
 RETURNS tstzrange
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE
    props jsonb := value;
    dt timestamptz;
    edt timestamptz;
BEGIN
    IF props ? 'properties' THEN
        props := props->'properties';
    END IF;
    IF
        props ? 'start_datetime'
        AND props->>'start_datetime' IS NOT NULL
        AND props ? 'end_datetime'
        AND props->>'end_datetime' IS NOT NULL
    THEN
        dt := props->>'start_datetime';
        edt := props->>'end_datetime';
        IF dt > edt THEN
            RAISE EXCEPTION 'start_datetime must be < end_datetime';
        END IF;
    ELSE
        dt := props->>'datetime';
        edt := props->>'datetime';
    END IF;
    IF dt is NULL OR edt IS NULL THEN
        RAISE NOTICE 'DT: %, EDT: %', dt, edt;
        RAISE EXCEPTION 'Either datetime (%) or both start_datetime (%) and end_datetime (%) must be set.', props->>'datetime',props->>'start_datetime',props->>'end_datetime';
    END IF;
    RETURN tstzrange(dt, edt, '[]');
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.update_item(content jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    old items %ROWTYPE;
    out items%ROWTYPE;
BEGIN
    PERFORM delete_item(content->>'id', content->>'collection');
    PERFORM create_item(content);
END;
$function$
;

CREATE TRIGGER partitions_delete_trigger BEFORE DELETE ON pgstac.partitions FOR EACH ROW EXECUTE FUNCTION pgstac.partitions_delete_trigger_func();



SELECT set_version('0.6.0');
