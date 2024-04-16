SET client_min_messages TO WARNING;
SET SEARCH_PATH to pgstac, public;
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
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='btree_gist') THEN
    CREATE EXTENSION IF NOT EXISTS unaccent;
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
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;
RESET ROLE;

SET ROLE pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;
RESET ROLE;

SET SEARCH_PATH TO pgstac, public;
SET ROLE pgstac_admin;

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

-- Install these idempotently as migrations do not put them before trying to modify the collections table


CREATE OR REPLACE FUNCTION collection_geom(content jsonb)
RETURNS geometry AS $$
    WITH box AS (SELECT content->'extent'->'spatial'->'bbox'->0 as box)
    SELECT
        st_makeenvelope(
            (box->>0)::float,
            (box->>1)::float,
            (box->>2)::float,
            (box->>3)::float,
            4326
        )
    FROM box;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION collection_datetime(content jsonb)
RETURNS timestamptz AS $$
    SELECT
        CASE
            WHEN
                (content->'extent'->'temporal'->'interval'->0->>0) IS NULL
            THEN '-infinity'::timestamptz
            ELSE
                (content->'extent'->'temporal'->'interval'->0->>0)::timestamptz
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION collection_enddatetime(content jsonb)
RETURNS timestamptz AS $$
    SELECT
        CASE
            WHEN
                (content->'extent'->'temporal'->'interval'->0->>1) IS NULL
            THEN 'infinity'::timestamptz
            ELSE
                (content->'extent'->'temporal'->'interval'->0->>1)::timestamptz
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;
-- BEGIN migra calculated SQL
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.collection_search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
DECLARE
    out_records jsonb;
    number_matched bigint := collection_search_matched(_search);
    number_returned bigint;
    _limit int := coalesce((_search->>'limit')::float::int, 10);
    _offset int := coalesce((_search->>'offset')::float::int, 0);
    links jsonb := '[]';
    ret jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'), '/collections');
    prevoffset int;
    nextoffset int;
BEGIN
    SELECT
        coalesce(jsonb_agg(c), '[]')
    INTO out_records
    FROM collection_search_rows(_search) c;

    number_returned := jsonb_array_length(out_records);

    IF _limit <= number_matched THEN --need to have paging links
        nextoffset := least(_offset + _limit, number_matched - 1);
        prevoffset := greatest(_offset - _limit, 0);
        IF _offset = 0 THEN -- no previous paging

            links := jsonb_build_array(
                jsonb_build_object(
                    'rel', 'next',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', nextoffset),
                    'merge', TRUE
                )
            );
        ELSE
            links := jsonb_build_array(
                jsonb_build_object(
                    'rel', 'prev',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', prevoffset),
                    'merge', TRUE
                ),
                jsonb_build_object(
                    'rel', 'next',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', nextoffset),
                    'merge', TRUE
                )
            );
        END IF;
    END IF;

    ret := jsonb_build_object(
        'collections', out_records,
        'numberMatched', number_matched,
        'numberReturned', number_returned,
        'links', links
    );
    RETURN ret;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.cql2_query(j jsonb, wrapper text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
 STABLE
AS $function$
#variable_conflict use_variable
DECLARE
    args jsonb := j->'args';
    arg jsonb;
    op text := lower(j->>'op');
    cql2op RECORD;
    literal text;
    _wrapper text;
    leftarg text;
    rightarg text;
    prop text;
    extra_props bool := pgstac.additional_properties();
BEGIN
    IF j IS NULL OR (op IS NOT NULL AND args IS NULL) THEN
        RETURN NULL;
    END IF;
    RAISE NOTICE 'CQL2_QUERY: %', j;

    -- check if all properties are represented in the queryables
    IF NOT extra_props THEN
        FOR prop IN
            SELECT DISTINCT p->>0
            FROM jsonb_path_query(j, 'strict $.**.property') p
            WHERE p->>0 NOT IN ('id', 'datetime', 'geometry', 'end_datetime', 'collection')
        LOOP
            IF (queryable(prop)).nulled_wrapper IS NULL THEN
                RAISE EXCEPTION 'Term % is not found in queryables.', prop;
            END IF;
        END LOOP;
    END IF;

    IF j ? 'filter' THEN
        RETURN cql2_query(j->'filter');
    END IF;

    IF j ? 'upper' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'upper', 'args', j->'upper'));
    END IF;

    IF j ? 'lower' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'lower', 'args', j->'lower'));
    END IF;

    -- Temporal Query
    IF op ilike 't_%' or op = 'anyinteracts' THEN
        RETURN temporal_op_query(op, args);
    END IF;

    -- If property is a timestamp convert it to text to use with
    -- general operators
    IF j ? 'timestamp' THEN
        RETURN format('%L::timestamptz', to_tstz(j->'timestamp'));
    END IF;
    IF j ? 'interval' THEN
        RAISE EXCEPTION 'Please use temporal operators when using intervals.';
        RETURN NONE;
    END IF;

    -- Spatial Query
    IF op ilike 's_%' or op = 'intersects' THEN
        RETURN spatial_op_query(op, args);
    END IF;

    IF op IN ('a_equals','a_contains','a_contained_by','a_overlaps') THEN
        IF args->0 ? 'property' THEN
            leftarg := format('to_text_array(%s)', (queryable(args->0->>'property')).path);
        END IF;
        IF args->1 ? 'property' THEN
            rightarg := format('to_text_array(%s)', (queryable(args->1->>'property')).path);
        END IF;
        RETURN FORMAT(
            '%s %s %s',
            COALESCE(leftarg, quote_literal(to_text_array(args->0))),
            CASE op
                WHEN 'a_equals' THEN '='
                WHEN 'a_contains' THEN '@>'
                WHEN 'a_contained_by' THEN '<@'
                WHEN 'a_overlaps' THEN '&&'
            END,
            COALESCE(rightarg, quote_literal(to_text_array(args->1)))
        );
    END IF;

    IF op = 'in' THEN
        RAISE NOTICE 'IN : % % %', args, jsonb_build_array(args->0), args->1;
        args := jsonb_build_array(args->0) || (args->1);
        RAISE NOTICE 'IN2 : %', args;
    END IF;



    IF op = 'between' THEN
        args = jsonb_build_array(
            args->0,
            args->1,
            args->2
        );
    END IF;

    -- Make sure that args is an array and run cql2_query on
    -- each element of the array
    RAISE NOTICE 'ARGS PRE: %', args;
    IF j ? 'args' THEN
        IF jsonb_typeof(args) != 'array' THEN
            args := jsonb_build_array(args);
        END IF;

        IF jsonb_path_exists(args, '$[*] ? (@.property == "id" || @.property == "datetime" || @.property == "end_datetime" || @.property == "collection")') THEN
            wrapper := NULL;
        ELSE
            -- if any of the arguments are a property, try to get the property_wrapper
            FOR arg IN SELECT jsonb_path_query(args, '$[*] ? (@.property != null)') LOOP
                RAISE NOTICE 'Arg: %', arg;
                wrapper := (queryable(arg->>'property')).nulled_wrapper;
                RAISE NOTICE 'Property: %, Wrapper: %', arg, wrapper;
                IF wrapper IS NOT NULL THEN
                    EXIT;
                END IF;
            END LOOP;

            -- if the property was not in queryables, see if any args were numbers
            IF
                wrapper IS NULL
                AND jsonb_path_exists(args, '$[*] ? (@.type()=="number")')
            THEN
                wrapper := 'to_float';
            END IF;
            wrapper := coalesce(wrapper, 'to_text');
        END IF;

        SELECT jsonb_agg(cql2_query(a, wrapper))
            INTO args
        FROM jsonb_array_elements(args) a;
    END IF;
    RAISE NOTICE 'ARGS: %', args;

    IF op IN ('and', 'or') THEN
        RETURN
            format(
                '(%s)',
                array_to_string(to_text_array(args), format(' %s ', upper(op)))
            );
    END IF;

    IF op = 'in' THEN
        RAISE NOTICE 'IN --  % %', args->0, to_text(args->0);
        RETURN format(
            '%s IN (%s)',
            to_text(args->0),
            array_to_string((to_text_array(args))[2:], ',')
        );
    END IF;

    -- Look up template from cql2_ops
    IF j ? 'op' THEN
        SELECT * INTO cql2op FROM cql2_ops WHERE  cql2_ops.op ilike op;
        IF FOUND THEN
            -- If specific index set in queryables for a property cast other arguments to that type

            RETURN format(
                cql2op.template,
                VARIADIC (to_text_array(args))
            );
        ELSE
            RAISE EXCEPTION 'Operator % Not Supported.', op;
        END IF;
    END IF;


    IF wrapper IS NOT NULL THEN
        RAISE NOTICE 'Wrapping % with %', j, wrapper;
        IF j ? 'property' THEN
            RETURN format('%I(%s)', wrapper, (queryable(j->>'property')).path);
        ELSE
            RETURN format('%I(%L)', wrapper, j);
        END IF;
    ELSIF j ? 'property' THEN
        RETURN quote_ident(j->>'property');
    END IF;

    RETURN quote_literal(to_text(j));
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
    init_ts timestamptz := clock_timestamp();
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
        RAISE LOG 'TOKEN_WHERE: % (%ms from search start)', token_where, age_ms(timer);
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
    RAISE NOTICE 'Time to set hydration/formatting %', age_ms(timer);
    timer := clock_timestamp();
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

    collection := jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb),
        'next', next,
        'prev', prev
    );

    IF context(_search->'conf') != 'off' THEN
        collection := collection || jsonb_strip_nulls(jsonb_build_object(
            'numberMatched', total_count,
            'numberReturned', coalesce(jsonb_array_length(out_records), 0)
        ));
    ELSE
        collection := collection || jsonb_strip_nulls(jsonb_build_object(
            'numberReturned', coalesce(jsonb_array_length(out_records), 0)
        ));
    END IF;

    IF get_setting_bool('timing', _search->'conf') THEN
        collection = collection || jsonb_build_object('timing', age_ms(init_ts));
    END IF;

    RAISE NOTICE 'Time to build final json %', age_ms(timer);
    timer := clock_timestamp();

    RAISE NOTICE 'Total Time: %', age_ms(current_timestamp);
    RAISE NOTICE 'RETURNING % records. NEXT: %. PREV: %', collection->>'numberReturned', collection->>'next', collection->>'prev';
    RETURN collection;
END;
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
  ('format_cache', 'false'),
  ('readonly', 'false')
ON CONFLICT DO NOTHING
;


INSERT INTO cql2_ops (op, template, types) VALUES
    ('eq', '%s = %s', NULL),
    ('neq', '%s != %s', NULL),
    ('ne', '%s != %s', NULL),
    ('!=', '%s != %s', NULL),
    ('<>', '%s != %s', NULL),
    ('lt', '%s < %s', NULL),
    ('lte', '%s <= %s', NULL),
    ('gt', '%s > %s', NULL),
    ('gte', '%s >= %s', NULL),
    ('le', '%s <= %s', NULL),
    ('ge', '%s >= %s', NULL),
    ('=', '%s = %s', NULL),
    ('<', '%s < %s', NULL),
    ('<=', '%s <= %s', NULL),
    ('>', '%s > %s', NULL),
    ('>=', '%s >= %s', NULL),
    ('like', '%s LIKE %s', NULL),
    ('ilike', '%s ILIKE %s', NULL),
    ('+', '%s + %s', NULL),
    ('-', '%s - %s', NULL),
    ('*', '%s * %s', NULL),
    ('/', '%s / %s', NULL),
    ('not', 'NOT (%s)', NULL),
    ('between', '%s BETWEEN %s AND %s', NULL),
    ('isnull', '%s IS NULL', NULL),
    ('upper', 'upper(%s)', NULL),
    ('lower', 'lower(%s)', NULL),
    ('casei', 'upper(%s)', NULL),
    ('accenti', 'unaccent(%s)', NULL)
ON CONFLICT (op) DO UPDATE
    SET
        template = EXCLUDED.template
;


ALTER FUNCTION to_text COST 5000;
ALTER FUNCTION to_float COST 5000;
ALTER FUNCTION to_int COST 5000;
ALTER FUNCTION to_tstz COST 5000;
ALTER FUNCTION to_text_array COST 5000;

ALTER FUNCTION update_partition_stats SECURITY DEFINER;
ALTER FUNCTION partition_after_triggerfunc SECURITY DEFINER;
ALTER FUNCTION drop_table_constraints SECURITY DEFINER;
ALTER FUNCTION create_table_constraints SECURITY DEFINER;
ALTER FUNCTION check_partition SECURITY DEFINER;
ALTER FUNCTION repartition SECURITY DEFINER;
ALTER FUNCTION where_stats SECURITY DEFINER;
ALTER FUNCTION search_query SECURITY DEFINER;
ALTER FUNCTION format_item SECURITY DEFINER;
ALTER FUNCTION maintain_index SECURITY DEFINER;

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

REVOKE ALL PRIVILEGES ON PROCEDURE run_queued_queries FROM public;
GRANT ALL ON PROCEDURE run_queued_queries TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_queries_intransaction FROM public;
GRANT ALL ON FUNCTION run_queued_queries_intransaction TO pgstac_admin;

RESET ROLE;

SET ROLE pgstac_ingest;
SELECT update_partition_stats_q(partition) FROM partitions_view;
SELECT set_version('unreleased');
