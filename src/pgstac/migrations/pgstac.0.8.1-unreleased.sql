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

ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;

ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;

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

CREATE OR REPLACE FUNCTION pgstac.additional_properties()
 RETURNS boolean
 LANGUAGE sql
AS $function$
    SELECT pgstac.get_setting_bool('additional_properties');
$function$
;

CREATE OR REPLACE FUNCTION pgstac.check_partition(_collection text, _dtrange tstzrange, _edtrange tstzrange)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    c RECORD;
    pm RECORD;
    _partition_name text;
    _partition_dtrange tstzrange;
    _constraint_dtrange tstzrange;
    _constraint_edtrange tstzrange;
    q text;
    deferrable_q text;
    err_context text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;

    IF c.partition_trunc IS NOT NULL THEN
        _partition_dtrange := tstzrange(
            date_trunc(c.partition_trunc, lower(_dtrange)),
            date_trunc(c.partition_trunc, lower(_dtrange)) + (concat('1 ', c.partition_trunc))::interval,
            '[)'
        );
    ELSE
        _partition_dtrange :=  '[-infinity, infinity]'::tstzrange;
    END IF;

    IF NOT _partition_dtrange @> _dtrange THEN
        RAISE EXCEPTION 'dtrange % is greater than the partition size % for collection %', _dtrange, c.partition_trunc, _collection;
    END IF;


    IF c.partition_trunc = 'year' THEN
        _partition_name := format('_items_%s_%s', c.key, to_char(lower(_partition_dtrange),'YYYY'));
    ELSIF c.partition_trunc = 'month' THEN
        _partition_name := format('_items_%s_%s', c.key, to_char(lower(_partition_dtrange),'YYYYMM'));
    ELSE
        _partition_name := format('_items_%s', c.key);
    END IF;

    SELECT * INTO pm FROM partition_sys_meta WHERE collection=_collection AND partition_dtrange @> _dtrange;
    IF FOUND THEN
        RAISE NOTICE '% % %', _edtrange, _dtrange, pm;
        _constraint_edtrange :=
            tstzrange(
                least(
                    lower(_edtrange),
                    nullif(lower(pm.constraint_edtrange), '-infinity')
                ),
                greatest(
                    upper(_edtrange),
                    nullif(upper(pm.constraint_edtrange), 'infinity')
                ),
                '[]'
            );
        _constraint_dtrange :=
            tstzrange(
                least(
                    lower(_dtrange),
                    nullif(lower(pm.constraint_dtrange), '-infinity')
                ),
                greatest(
                    upper(_dtrange),
                    nullif(upper(pm.constraint_dtrange), 'infinity')
                ),
                '[]'
            );

        IF pm.constraint_edtrange @> _edtrange AND pm.constraint_dtrange @> _dtrange THEN
            RETURN pm.partition;
        ELSE
            PERFORM drop_table_constraints(_partition_name);
        END IF;
    ELSE
        _constraint_edtrange := _edtrange;
        _constraint_dtrange := _dtrange;
    END IF;
    RAISE NOTICE 'Creating partition % %', _partition_name, _partition_dtrange;
    IF c.partition_trunc IS NULL THEN
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                GRANT ALL ON %I to pgstac_ingest;
            $q$,
            _partition_name,
            _collection,
            concat(_partition_name,'_pk'),
            _partition_name,
            _partition_name
        );
    ELSE
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L) PARTITION BY RANGE (datetime);
                CREATE TABLE IF NOT EXISTS %I partition OF %I FOR VALUES FROM (%L) TO (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                GRANT ALL ON %I TO pgstac_ingest;
            $q$,
            format('_items_%s', c.key),
            _collection,
            _partition_name,
            format('_items_%s', c.key),
            lower(_partition_dtrange),
            upper(_partition_dtrange),
            format('%s_pk', _partition_name),
            _partition_name,
            _partition_name
        );
    END IF;

    BEGIN
        EXECUTE q;
    EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'Partition % already exists.', _partition_name;
        WHEN others THEN
            GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
            RAISE INFO 'Error Name:%',SQLERRM;
            RAISE INFO 'Error State:%', SQLSTATE;
            RAISE INFO 'Error Context:%', err_context;
    END;
    PERFORM maintain_partitions(_partition_name);
    PERFORM update_partition_stats_q(_partition_name, true);
    REFRESH MATERIALIZED VIEW partitions;
    REFRESH MATERIALIZED VIEW partition_steps;
    RETURN _partition_name;
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
            WHERE p->>0 NOT IN ('id', 'datetime', 'end_datetime', 'collection')
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
            args->1->0,
            args->1->1
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

CREATE OR REPLACE FUNCTION pgstac.get_queryables(_collection_ids text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
BEGIN
    -- Build up queryables if the input contains valid collection ids or is empty
    IF EXISTS (
        SELECT 1 FROM collections
        WHERE
            _collection_ids IS NULL
            OR cardinality(_collection_ids) = 0
            OR id = ANY(_collection_ids)
    )
    THEN
        RETURN (
            WITH base AS (
                SELECT
                    unnest(collection_ids) as collection_id,
                    name,
                    coalesce(definition, '{"type":"string"}'::jsonb) as definition
                FROM queryables
                WHERE
                    _collection_ids IS NULL OR
                    _collection_ids = '{}'::text[] OR
                    _collection_ids && collection_ids
                UNION ALL
                SELECT null, name, coalesce(definition, '{"type":"string"}'::jsonb) as definition
                FROM queryables WHERE collection_ids IS NULL OR collection_ids = '{}'::text[]
            ), g AS (
                SELECT
                    name,
                    first_notnull(definition) as definition,
                    jsonb_array_unique_merge(definition->'enum') as enum,
                    jsonb_min(definition->'minimum') as minimum,
                    jsonb_min(definition->'maxiumn') as maximum
                FROM base
                GROUP BY 1
            )
            SELECT
                jsonb_build_object(
                    '$schema', 'http://json-schema.org/draft-07/schema#',
                    '$id', '',
                    'type', 'object',
                    'title', 'STAC Queryables.',
                    'properties', jsonb_object_agg(
                        name,
                        definition
                        ||
                        jsonb_strip_nulls(jsonb_build_object(
                            'enum', enum,
                            'minimum', minimum,
                            'maximum', maximum
                        ))
                    ),
                    'additionalProperties', pgstac.additional_properties()
                )
                FROM g
        );
    ELSE
        RETURN NULL;
    END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.maintain_partition_queries(part text DEFAULT 'items'::text, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false, idxconcurrently boolean DEFAULT false)
 RETURNS SETOF text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
   rec record;
BEGIN
    FOR rec IN (
        WITH p AS (
           SELECT
                relid::text as partition,
                replace(replace(
                    CASE
                        WHEN level = 1 THEN pg_get_expr(c.relpartbound, c.oid)
                        ELSE pg_get_expr(parent.relpartbound, parent.oid)
                    END,
                    'FOR VALUES IN (''',''), ''')',
                    ''
                ) AS collection
            FROM pg_partition_tree('items')
            JOIN pg_class c ON (relid::regclass = c.oid)
            JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
        ), i AS (
            SELECT
                partition,
                indexname,
                regexp_replace(btrim(replace(replace(indexdef, indexname, ''),'pgstac.',''),' \t\n'), '[ ]+', ' ', 'g') as iidx,
                COALESCE(
                    (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
                    (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'))[1],
                    CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime' ELSE NULL END
                ) AS field
            FROM
                pg_indexes
                JOIN p ON (tablename=partition)
        ), q AS (
            SELECT
                name AS field,
                collection,
                partition,
                format(indexdef(queryables), partition) as qidx
            FROM queryables, unnest_collection(queryables.collection_ids) collection
                JOIN p USING (collection)
            WHERE property_index_type IS NOT NULL OR name IN ('datetime','geometry','id')
        )
        SELECT * FROM i FULL JOIN q USING (field, partition)
        WHERE lower(iidx) IS DISTINCT FROM lower(qidx)
    ) LOOP
        IF rec.iidx IS NULL THEN
            IF idxconcurrently THEN
                RETURN NEXT replace(rec.qidx, 'INDEX', 'INDEX CONCURRENTLY');
            ELSE
                RETURN NEXT rec.qidx;
            END IF;
        ELSIF rec.qidx IS NULL AND dropindexes THEN
            RETURN NEXT format('DROP INDEX IF EXISTS %I;', rec.indexname);
        ELSIF lower(rec.qidx) != lower(rec.iidx) THEN
            IF dropindexes THEN
                RETURN NEXT format('DROP INDEX IF EXISTS %I; %s;', rec.indexname, rec.qidx);
            ELSE
                IF idxconcurrently THEN
                    RETURN NEXT replace(rec.qidx, 'INDEX', 'INDEX CONCURRENTLY');
                ELSE
                    RETURN NEXT rec.qidx;
                END IF;
            END IF;
        ELSIF rebuildindexes and rec.indexname IS NOT NULL THEN
            IF idxconcurrently THEN
                RETURN NEXT format('REINDEX INDEX CONCURRENTLY %I;', rec.indexname);
            ELSE
                RETURN NEXT format('REINDEX INDEX %I;', rec.indexname);
            END IF;
        END IF;
    END LOOP;
    RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.maintain_partitions(part text DEFAULT 'items'::text, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false)
 RETURNS void
 LANGUAGE sql
 SECURITY DEFINER
AS $function$
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partition_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.repartition(_collection text, _partition_trunc text, triggered boolean DEFAULT false)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    c RECORD;
    q text;
    from_trunc text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;
    IF triggered THEN
        RAISE NOTICE 'Converting % to % partitioning via Trigger', _collection, _partition_trunc;
    ELSE
        RAISE NOTICE 'Converting % from using % to % partitioning', _collection, c.partition_trunc, _partition_trunc;
        IF c.partition_trunc IS NOT DISTINCT FROM _partition_trunc THEN
            RAISE NOTICE 'Collection % already set to use partition by %', _collection, _partition_trunc;
            RETURN _collection;
        END IF;
    END IF;

    IF EXISTS (SELECT 1 FROM partitions_view WHERE collection=_collection LIMIT 1) THEN
        EXECUTE format(
            $q$
                CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
                DROP TABLE IF EXISTS %I CASCADE;
                WITH p AS (
                    SELECT
                        collection,
                        CASE
                            WHEN %L IS NULL THEN '-infinity'::timestamptz
                            ELSE date_trunc(%L, datetime)
                        END as d,
                        tstzrange(min(datetime),max(datetime),'[]') as dtrange,
                        tstzrange(min(end_datetime),max(end_datetime),'[]') as edtrange
                    FROM changepartitionstaging
                    GROUP BY 1,2
                ) SELECT check_partition(collection, dtrange, edtrange) FROM p;
                INSERT INTO items SELECT * FROM changepartitionstaging;
                DROP TABLE changepartitionstaging;
            $q$,
            concat('_items_', c.key),
            concat('_items_', c.key),
            c.partition_trunc,
            c.partition_trunc
        );
    END IF;
    RETURN _collection;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.update_partition_stats(_partition text, istrigger boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 STRICT SECURITY DEFINER
AS $function$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    cdtrange tstzrange;
    cedtrange tstzrange;
    extent geometry;
    collection text;
BEGIN
    RAISE NOTICE 'Updating stats for %.', _partition;
    EXECUTE format(
        $q$
            SELECT
                tstzrange(min(datetime), max(datetime),'[]'),
                tstzrange(min(end_datetime), max(end_datetime), '[]')
            FROM %I
        $q$,
        _partition
    ) INTO dtrange, edtrange;
    extent := st_estimatedextent('pgstac', _partition, 'geometry');
    INSERT INTO partition_stats (partition, dtrange, edtrange, spatial, last_updated)
        SELECT _partition, dtrange, edtrange, extent, now()
        ON CONFLICT (partition) DO
            UPDATE SET
                dtrange=EXCLUDED.dtrange,
                edtrange=EXCLUDED.edtrange,
                spatial=EXCLUDED.spatial,
                last_updated=EXCLUDED.last_updated
    ;

    SELECT
        constraint_dtrange, constraint_edtrange, pv.collection
        INTO cdtrange, cedtrange, collection
    FROM partitions_view pv WHERE partition = _partition;
    REFRESH MATERIALIZED VIEW partitions;
    REFRESH MATERIALIZED VIEW partition_steps;


    RAISE NOTICE 'Checking if we need to modify constraints. cdtrange: % dtrange: % cedtrange: % edtrange: %', cdtrange, dtrange, cedtrange, edtrange;
    IF
        (cdtrange IS DISTINCT FROM dtrange OR edtrange IS DISTINCT FROM cedtrange)
        AND NOT istrigger
    THEN
        RAISE NOTICE 'Modifying Constraints';
        RAISE NOTICE 'Existing % %', cdtrange, cedtrange;
        RAISE NOTICE 'New      % %', dtrange, edtrange;
        PERFORM drop_table_constraints(_partition);
        PERFORM create_table_constraints(_partition, dtrange, edtrange);
    END IF;
    RAISE NOTICE 'Checking if we need to update collection extents.';
    IF get_setting_bool('update_collection_extent') THEN
        RAISE NOTICE 'updating collection extent for %', collection;
        PERFORM run_or_queue(format($q$
            UPDATE collections
            SET content = jsonb_set_lax(
                content,
                '{extent}'::text[],
                collection_extent(%L, FALSE),
                true,
                'use_json_null'
            ) WHERE id=%L
            ;
        $q$, collection, collection));
    ELSE
        RAISE NOTICE 'Not updating collection extent for %', collection;
    END IF;

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
  ('format_cache', 'false')
ON CONFLICT DO NOTHING
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
ALTER FUNCTION maintain_partition_queries SECURITY DEFINER;
ALTER FUNCTION maintain_partitions SECURITY DEFINER;

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

RESET ROLE;

SET ROLE pgstac_ingest;
SELECT update_partition_stats_q(partition) FROM partitions_view;
SELECT set_version('unreleased');
