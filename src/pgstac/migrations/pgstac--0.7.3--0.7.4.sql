RESET ROLE;
DO $$
DECLARE
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis') THEN
    CREATE EXTENSION IF NOT EXISTS postgis;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='btree_gist') THEN
    CREATE EXTENSION IF NOT EXISTS btree_gist;
  END IF;
END;
$$ LANGUAGE PLPGSQL;

DO $$
  BEGIN
    CREATE ROLE pgstac_admin;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    CREATE ROLE pgstac_read;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    CREATE ROLE pgstac_ingest;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;


GRANT pgstac_admin TO current_user;

-- Function to make sure pgstac_admin is the owner of items
CREATE OR REPLACE FUNCTION pgstac_admin_owns() RETURNS VOID AS $$
DECLARE
  f RECORD;
BEGIN
  FOR f IN (
    SELECT
      concat(
        oid::regproc::text,
        '(',
        coalesce(pg_get_function_identity_arguments(oid),''),
        ')'
      ) AS name,
      CASE prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' WHEN 'a' THEN 'AGGREGATE' END as typ
    FROM pg_proc
    WHERE
      pronamespace=to_regnamespace('pgstac')
      AND proowner != to_regrole('pgstac_admin')
      AND proname NOT LIKE 'pg_stat%'
  )
  LOOP
    BEGIN
      EXECUTE format('ALTER %s %s OWNER TO pgstac_admin;', f.typ, f.name);
    EXCEPTION WHEN others THEN
      RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
  END LOOP;
  FOR f IN (
    SELECT
      oid::regclass::text as name,
      CASE relkind
        WHEN 'i' THEN 'INDEX'
        WHEN 'I' THEN 'INDEX'
        WHEN 'p' THEN 'TABLE'
        WHEN 'r' THEN 'TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'S' THEN 'SEQUENCE'
        ELSE NULL
      END as typ
    FROM pg_class
    WHERE relnamespace=to_regnamespace('pgstac') and relowner != to_regrole('pgstac_admin') AND relkind IN ('r','p','v','S') AND relname NOT LIKE 'pg_stat'
  )
  LOOP
    BEGIN
      EXECUTE format('ALTER %s %s OWNER TO pgstac_admin;', f.typ, f.name);
    EXCEPTION WHEN others THEN
      RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;
SELECT pgstac_admin_owns();

CREATE SCHEMA IF NOT EXISTS pgstac AUTHORIZATION pgstac_admin;

GRANT ALL ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_admin;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_admin;

ALTER ROLE pgstac_admin SET SEARCH_PATH TO pgstac, public;
ALTER ROLE pgstac_read SET SEARCH_PATH TO pgstac, public;
ALTER ROLE pgstac_ingest SET SEARCH_PATH TO pgstac, public;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;

GRANT pgstac_read TO pgstac_ingest;
GRANT ALL ON SCHEMA pgstac TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;

SET ROLE pgstac_admin;

SET SEARCH_PATH TO pgstac, public;

DO $$
  BEGIN
    DROP FUNCTION IF EXISTS analyze_items;
  EXCEPTION WHEN others THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;
DO $$
  BEGIN
    DROP FUNCTION IF EXISTS validate_constraints;
  EXCEPTION WHEN others THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;
SET client_min_messages TO WARNING;
SET SEARCH_PATH to pgstac, public;
-- BEGIN migra calculated SQL
drop function if exists "pgstac"."get_token_filter"(_search jsonb, token_rec jsonb);

create unlogged table "pgstac"."format_item_cache" (
    "id" text not null,
    "collection" text not null,
    "fields" text not null,
    "hydrated" boolean not null,
    "output" jsonb,
    "lastused" timestamp with time zone default now(),
    "usecount" integer default 1,
    "timetoformat" double precision
);


CREATE INDEX format_item_cache_lastused_idx ON pgstac.format_item_cache USING btree (lastused);

CREATE UNIQUE INDEX format_item_cache_pkey ON pgstac.format_item_cache USING btree (collection, id, fields, hydrated);

alter table "pgstac"."format_item_cache" add constraint "format_item_cache_pkey" PRIMARY KEY using index "format_item_cache_pkey";

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.age_ms(a timestamp with time zone, b timestamp with time zone DEFAULT clock_timestamp())
 RETURNS double precision
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT abs(extract(epoch from age(a,b)) * 1000);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.format_item(_item pgstac.items, _fields jsonb DEFAULT '{}'::jsonb, _hydrated boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    cache bool := get_setting_bool('format_cache');
    _output jsonb := null;
    t timestamptz := clock_timestamp();
BEGIN
    IF cache THEN
        SELECT output INTO _output FROM format_item_cache
        WHERE id=_item.id AND collection=_item.collection AND fields=_fields::text AND hydrated=_hydrated;
    END IF;
    IF _output IS NULL THEN
        IF _hydrated THEN
            _output := content_hydrate(_item, _fields);
        ELSE
            _output := content_nonhydrated(_item, _fields);
        END IF;
    END IF;
    IF cache THEN
        INSERT INTO format_item_cache (id, collection, fields, hydrated, output, timetoformat)
            VALUES (_item.id, _item.collection, _fields::text, _hydrated, _output, age_ms(t))
            ON CONFLICT(collection, id, fields, hydrated) DO
                UPDATE
                    SET lastused=now(), usecount = format_item_cache.usecount + 1
        ;
    END IF;
    RETURN _output;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_token_filter(_sortby jsonb DEFAULT '[{"field": "datetime", "direction": "desc"}]'::jsonb, token_item pgstac.items DEFAULT NULL::pgstac.items, prev boolean DEFAULT false, inclusive boolean DEFAULT false)
 RETURNS text
 LANGUAGE plpgsql
 SET transform_null_equals TO 'true'
AS $function$
DECLARE
    ltop text := '<';
    gtop text := '>';
    dir text;
    sort record;
    orfilter text := '';
    orfilters text[] := '{}'::text[];
    andfilters text[] := '{}'::text[];
    output text;
    token_where text;
BEGIN
    _sortby := _sortby || jsonb_build_object('field','id','direction',_sortby->0->>'direction');
    RAISE NOTICE 'Getting Token Filter. % %', _sortby, token_item;
    IF inclusive THEN
        orfilters := orfilters || format('( id=%L AND collection=%L )' , token_item.id, token_item.collection);
    END IF;

    FOR sort IN
        WITH s1 AS (
            SELECT
                _row,
                (queryable(value->>'field')).expression as _field,
                (value->>'field' = 'id') as _isid,
                get_sort_dir(value) as _dir
            FROM jsonb_array_elements(_sortby)
            WITH ORDINALITY AS t(value, _row)
        )
        SELECT
            _row,
            _field,
            _dir,
            get_token_val_str(_field, token_item) as _val
        FROM s1
        WHERE _row <= (SELECT min(_row) FROM s1 WHERE _isid)
    LOOP
        orfilter := NULL;
        RAISE NOTICE 'SORT: %', sort;
        IF sort._val IS NOT NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            orfilter := format($f$(
                (%s %s %s) OR (%s IS NULL)
            )$f$,
            sort._field,
            ltop,
            sort._val,
            sort._val
            );
        ELSIF sort._val IS NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            RAISE NOTICE '< but null';
            orfilter := format('%s IS NOT NULL', sort._field);
        ELSIF sort._val IS NULL THEN
            RAISE NOTICE '> but null';
        ELSE
            orfilter := format($f$(
                (%s %s %s) OR (%s IS NULL)
            )$f$,
            sort._field,
            gtop,
            sort._val,
            sort._field
            );
        END IF;
        RAISE NOTICE 'ORFILTER: %', orfilter;

        IF orfilter IS NOT NULL THEN
            IF sort._row = 1 THEN
                orfilters := orfilters || orfilter;
            ELSE
                orfilters := orfilters || format('(%s AND %s)', array_to_string(andfilters, ' AND '), orfilter);
            END IF;
        END IF;
        IF sort._val IS NOT NULL THEN
            andfilters := andfilters || format('%s = %s', sort._field, sort._val);
        ELSE
            andfilters := andfilters || format('%s IS NULL', sort._field);
        END IF;
    END LOOP;

    output := array_to_string(orfilters, ' OR ');

    token_where := concat('(',coalesce(output,'true'),')');
    IF trim(token_where) = '' THEN
        token_where := NULL;
    END IF;
    RAISE NOTICE 'TOKEN_WHERE: %',token_where;
    RETURN token_where;
    END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_token_record(_token text, OUT prev boolean, OUT item pgstac.items)
 RETURNS record
 LANGUAGE plpgsql
 STABLE STRICT
AS $function$
DECLARE
    _itemid text := _token;
    _collectionid text;
BEGIN
    IF _token IS NULL THEN
        RETURN;
    END IF;
    RAISE NOTICE 'Looking for token: %', _token;
    prev := FALSE;
    IF _token ILIKE 'prev:%' THEN
        _itemid := replace(_token, 'prev:','');
        prev := TRUE;
    ELSIF _token ILIKE 'next:%' THEN
        _itemid := replace(_token, 'next:', '');
    END IF;
    SELECT id INTO _collectionid FROM collections WHERE _itemid LIKE concat(id,':%');
    IF FOUND THEN
        _itemid := replace(_itemid, concat(_collectionid,':'), '');
        SELECT * INTO item FROM items WHERE id=_itemid AND collection=_collectionid;
    ELSE
        SELECT * INTO item FROM items WHERE id=_itemid;
    END IF;
    IF item IS NULL THEN
        RAISE EXCEPTION 'Could not find item using token: % item: % collection: %', _token, _itemid, _collectionid;
    END IF;
    RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search_rows(_where text DEFAULT 'TRUE'::text, _orderby text DEFAULT 'datetime DESC, id DESC'::text, partitions text[] DEFAULT NULL::text[], _limit integer DEFAULT 10)
 RETURNS SETOF pgstac.items
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    base_query text;
    query text;
    sdate timestamptz;
    edate timestamptz;
    n int;
    records_left int := _limit;
BEGIN
IF _where IS NULL OR trim(_where) = '' THEN
    _where = ' TRUE ';
END IF;
RAISE NOTICE 'Getting chunks for % %', _where, _orderby;

base_query := $q$
    SELECT * FROM items
    WHERE
    datetime >= %L AND datetime < %L
    AND (%s)
    ORDER BY %s
    LIMIT %L
$q$;

IF _orderby ILIKE 'datetime d%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 DESC LOOP
        RAISE NOTICE 'Running Query for % to %', sdate, edate;
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE LOG 'QUERY: %', query;
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE NOTICE 'Returned %/% Rows From % to %. % to go.', n, _limit, sdate, edate, records_left;
        IF records_left <= 0 THEN
            RETURN;
        END IF;
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RAISE NOTICE 'Running Query for % to %', sdate, edate;
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE LOG 'QUERY: %', query;
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE NOTICE 'Returned %/% Rows From % to %. % to go.', n, _limit, sdate, edate, records_left;
        IF records_left <= 0 THEN
            RETURN;
        END IF;
    END LOOP;
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
        LIMIT %L
    $q$, _where, _orderby, _limit
    );
    RAISE LOG 'QUERY: %', query;
    RETURN QUERY EXECUTE query;
END IF;
RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_token_val_str(_field text, _item pgstac.items)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    q text;
    literal text;
BEGIN
    q := format($q$ SELECT quote_literal(%s) FROM (SELECT $1.*) as r;$q$, _field);
    EXECUTE q INTO literal USING _item;
    RETURN literal;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_after_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    p text;
    t timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Updating partition stats %', t;
    FOR p IN SELECT DISTINCT partition
        FROM newdata n JOIN partition_sys_meta p
        ON (n.collection=p.collection AND n.datetime <@ p.partition_dtrange)
    LOOP
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L, %L);', p, true));
    END LOOP;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        DELETE FROM format_item_cache c USING newdata n WHERE c.collection = n.collection AND c.id = n.id;
    END IF;
    RAISE NOTICE 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.queryables_constraint_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    allcollections text[];
BEGIN
    RAISE NOTICE 'Making sure that name/collection is unique for queryables %', NEW;
    IF NEW.collection_ids IS NOT NULL THEN
        IF EXISTS (
            SELECT 1 FROM
                collections
                LEFT JOIN
                unnest(NEW.collection_ids) c
                ON (collections.id = c)
                WHERE c IS NULL
        ) THEN
            RAISE foreign_key_violation USING MESSAGE = format(
                'One or more collections in %s do not exist.', NEW.collection_ids
            );
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF EXISTS (
            SELECT 1 FROM queryables q
            WHERE
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                )
        ) THEN
            RAISE unique_violation USING MESSAGE = format(
                'There is already a queryable for %s for a collection in %s: %s',
                NEW.name,
                NEW.collection_ids,
				(SELECT json_agg(row_to_json(q)) FROM queryables q WHERE
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                ))
            );
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF EXISTS (
            SELECT 1 FROM queryables q
            WHERE
                q.id != NEW.id
                AND
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                )
        ) THEN
            RAISE unique_violation
            USING MESSAGE = format(
                'There is already a queryable for %s for a collection in %s',
                NEW.name,
                NEW.collection_ids
            );
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    searches searches%ROWTYPE;
    _where text;
    orderby text;
    search_where search_wheres%ROWTYPE;
    total_count bigint;
    token record;
    token_prev boolean;
    token_item items%ROWTYPE;
    token_where text;
    full_where text;
    timer timestamptz := clock_timestamp();
    hydrate bool := NOT (_search->'conf'->>'nohydrate' IS NOT NULL AND (_search->'conf'->>'nohydrate')::boolean = true);
    prev text;
    next text;
    context jsonb;
    collection jsonb;
    out_records jsonb;
    out_len int;
    _limit int := coalesce((_search->>'limit')::int, 10);
    _querylimit int;
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    has_prev boolean := FALSE;
    has_next boolean := FALSE;
BEGIN
    searches := search_query(_search);
    _where := searches._where;
    orderby := searches.orderby;
    search_where := where_stats(_where);
    total_count := coalesce(search_where.total_count, search_where.estimated_count);
    RAISE NOTICE 'SEARCH:TOKEN: %', _search->>'token';
    token := get_token_record(_search->>'token');
    RAISE NOTICE '***TOKEN: %', token;
    _querylimit := _limit + 1;
    IF token IS NOT NULL THEN
        token_prev := token.prev;
        token_item := token.item;
        token_where := get_token_filter(_search->'sortby', token_item, token_prev, FALSE);
        RAISE LOG 'TOKEN_WHERE: %', token_where;
        IF token_prev THEN -- if we are using a prev token, we know has_next is true
            RAISE LOG 'There is a previous token, so automatically setting has_next to true';
            has_next := TRUE;
            orderby := sort_sqlorderby(_search, TRUE);
        ELSE
            RAISE LOG 'There is a next token, so automatically setting has_prev to true';
            has_prev := TRUE;

        END IF;
    ELSE -- if there was no token, we know there is no prev
        RAISE LOG 'There is no token, so we know there is no prev. setting has_prev to false';
        has_prev := FALSE;
    END IF;

    full_where := concat_ws(' AND ', _where, token_where);
    RAISE NOTICE 'FULL WHERE CLAUSE: %', full_where;
    RAISE NOTICE 'Time to get counts and build query %', age_ms(timer);
    timer := clock_timestamp();

    IF hydrate THEN
        RAISE NOTICE 'Getting hydrated data.';
    ELSE
        RAISE NOTICE 'Getting non-hydrated data.';
    END IF;
    RAISE NOTICE 'CACHE SET TO %', get_setting_bool('format_cache');
    SELECT jsonb_agg(format_item(i, _fields, hydrate)) INTO out_records
    FROM search_rows(
        full_where,
        orderby,
        search_where.partitions,
        _querylimit
    ) as i;

    RAISE NOTICE 'Time to fetch rows %', age_ms(timer);
    timer := clock_timestamp();


    IF token_prev THEN
        out_records := flip_jsonb_array(out_records);
    END IF;

    RAISE NOTICE 'Query returned % records.', jsonb_array_length(out_records);
    RAISE LOG 'TOKEN:   % %', token_item.id, token_item.collection;
    RAISE LOG 'RECORD_1: % %', out_records->0->>'id', out_records->0->>'collection';
    RAISE LOG 'RECORD-1: % %', out_records->-1->>'id', out_records->-1->>'collection';

    -- REMOVE records that were from our token
    IF out_records->0->>'id' = token_item.id AND out_records->0->>'collection' = token_item.collection THEN
        out_records := out_records - 0;
    ELSIF out_records->-1->>'id' = token_item.id AND out_records->-1->>'collection' = token_item.collection THEN
        out_records := out_records - -1;
    END IF;

    out_len := jsonb_array_length(out_records);

    IF out_len = _limit + 1 THEN
        IF token_prev THEN
            has_prev := TRUE;
            out_records := out_records - 0;
        ELSE
            has_next := TRUE;
            out_records := out_records - -1;
        END IF;
    END IF;

    IF has_next THEN
        next := concat(out_records->-1->>'collection', ':', out_records->-1->>'id');
        RAISE NOTICE 'HAS NEXT | %', next;
    END IF;

    IF has_prev THEN
        prev := concat(out_records->0->>'collection', ':', out_records->0->>'id');
        RAISE NOTICE 'HAS PREV | %', prev;
    END IF;

    RAISE NOTICE 'Time to get prev/next %', age_ms(timer);
    timer := clock_timestamp();

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

    RAISE NOTICE 'Time to build final json %', age_ms(timer);
    timer := clock_timestamp();

    RAISE NOTICE 'Total Time: %', age_ms(current_timestamp);
    RAISE NOTICE 'RETURNING % records. NEXT: %. PREV: %', collection->'context'->>'returned', collection->>'next', collection->>'prev';
    RETURN collection;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.sort_sqlorderby(_search jsonb DEFAULT NULL::jsonb, reverse boolean DEFAULT false)
 RETURNS text
 LANGUAGE sql
AS $function$
    WITH sortby AS (
        SELECT coalesce(_search->'sortby','[{"field":"datetime", "direction":"desc"}]') as sort
    ), withid AS (
        SELECT CASE
            WHEN sort @? '$[*] ? (@.field == "id")' THEN sort
            ELSE sort || '[{"field":"id", "direction":"desc"}]'::jsonb
            END as sort
        FROM sortby
    ), withid_rows AS (
        SELECT jsonb_array_elements(sort) as value FROM withid
    ),sorts AS (
        SELECT
            coalesce(
                (queryable(value->>'field')).expression
            ) as key,
            parse_sort_dir(value->>'direction', reverse) as dir
        FROM withid_rows
    )
    SELECT array_to_string(
        array_agg(concat(key, ' ', dir)),
        ', '
    ) FROM sorts;
$function$
;



-- END migra calculated SQL
DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('eo:cloud_cover','{"$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fieldsproperties/eo:cloud_cover"}','to_int','BTREE');
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DELETE FROM queryables a USING queryables b
  WHERE a.name = b.name AND a.collection_ids IS NOT DISTINCT FROM b.collection_ids AND a.id > b.id;


INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default_filter_lang', 'cql2-json'),
  ('additional_properties', 'true'),
  ('use_queue', 'false'),
  ('queue_timeout', '10 minutes'),
  ('update_collection_extent', 'false'),
  ('format_cache', 'false')
ON CONFLICT DO NOTHING
;

ALTER FUNCTION to_text COST 5000;
ALTER FUNCTION to_float COST 5000;
ALTER FUNCTION to_int COST 5000;
ALTER FUNCTION to_tstz COST 5000;
ALTER FUNCTION to_text_array COST 5000;


GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

SELECT update_partition_stats_q(partition) FROM partitions;
SELECT set_version('0.7.4');
