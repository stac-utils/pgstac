SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.get_token_val_str(_field text, _item pgstac.items)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
literal text;
BEGIN
RAISE NOTICE '% %', _field, _item;
CREATE TEMP TABLE _token_item ON COMMIT DROP AS SELECT (_item).*;
EXECUTE format($q$ SELECT quote_literal(%s) FROM _token_item $q$, _field) INTO literal;
DROP TABLE IF EXISTS _token_item;
RETURN literal;
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
    token_item items%ROWTYPE;
BEGIN
    RAISE NOTICE 'Getting Token Filter. % %', _search, token_rec;
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
        SELECT to_jsonb(items) INTO token_rec
        FROM items WHERE id=token_id;
    END IF;
    RAISE NOTICE 'TOKEN ID: % %', token_rec, token_rec->'id';


    RAISE NOTICE 'TOKEN ID: % %', token_rec, token_rec->'id';
    token_item := jsonb_populate_record(null::items, token_rec);
    RAISE NOTICE 'TOKEN ITEM ----- %', token_item;


    CREATE TEMP TABLE sorts (
        _row int GENERATED ALWAYS AS IDENTITY NOT NULL,
        _field text PRIMARY KEY,
        _dir text NOT NULL,
        _val text
    ) ON COMMIT DROP;

    -- Make sure we only have distinct columns to sort with taking the first one we get
    INSERT INTO sorts (_field, _dir)
        SELECT
            (queryable(value->>'field')).expression,
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
    UPDATE sorts SET _val=get_token_val_str(_field, token_item);

    -- Check if all sorts are the same direction and use row comparison
    -- to filter
    RAISE NOTICE 'sorts 2: %', (SELECT jsonb_agg(to_json(sorts)) FROM sorts);

        FOR sort IN SELECT * FROM sorts ORDER BY _row asc LOOP
            RAISE NOTICE 'SORT: %', sort;
            IF sort._row = 1 THEN
                IF sort._val IS NULL THEN
                    orfilters := orfilters || format('(%s IS NOT NULL)', sort._field);
                ELSE
                    orfilters := orfilters || format('(%s %s %s)',
                        sort._field,
                        CASE WHEN (prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC') THEN '<' ELSE '>' END,
                        sort._val
                    );
                END IF;
            ELSE
                IF sort._val IS NULL THEN
                    orfilters := orfilters || format('(%s AND %s IS NOT NULL)',
                    array_to_string(andfilters, ' AND '), sort._field);
                ELSE
                    orfilters := orfilters || format('(%s AND %s %s %s)',
                        array_to_string(andfilters, ' AND '),
                        sort._field,
                        CASE WHEN (prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC') THEN '<' ELSE '>' END,
                        sort._val
                    );
                END IF;
            END IF;
            IF sort._val IS NULL THEN
                andfilters := andfilters || format('%s IS NULL',
                    sort._field
                );
            ELSE
                andfilters := andfilters || format('%s = %s',
                    sort._field,
                    sort._val
                );
            END IF;
        END LOOP;
        output := array_to_string(orfilters, ' OR ');

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

CREATE OR REPLACE FUNCTION pgstac.items_staging_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    p record;
    _partitions text[];
    ts timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Creating Partitions. %', clock_timestamp() - ts;
    WITH ranges AS (
        SELECT
            n.content->>'collection' as collection,
            stac_daterange(n.content->'properties') as dtr
        FROM newdata n
    ), p AS (
        SELECT
            collection,
            lower(dtr) as datetime,
            upper(dtr) as end_datetime,
            (partition_name(
                collection,
                lower(dtr)
            )).partition_name as name
        FROM ranges
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

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT
            (content_dehydrate(content)).*
        FROM newdata;
        DELETE FROM items_staging;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT
            (content_dehydrate(content)).*
        FROM newdata
        ON CONFLICT DO NOTHING;
        DELETE FROM items_staging_ignore;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        WITH staging_formatted AS (
            SELECT (content_dehydrate(content)).* FROM newdata
        ), deletes AS (
            DELETE FROM items i USING staging_formatted s
                WHERE
                    i.id = s.id
                    AND i.collection = s.collection
                    AND i IS DISTINCT FROM s
            RETURNING i.id, i.collection
        )
        INSERT INTO items
        SELECT s.* FROM
            staging_formatted s
            ON CONFLICT DO NOTHING;
        DELETE FROM items_staging_upsert;
    END IF;
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$function$
;



SELECT set_version('0.6.12');
