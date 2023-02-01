CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

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

DROP FUNCTION IF EXISTS analyze_items;
DROP FUNCTION IF EXISTS validate_constraints;
SET client_min_messages TO WARNING;
SET SEARCH_PATH to pgstac, public;
-- BEGIN migra calculated SQL
drop trigger if exists "queryables_collection_trigger" on "pgstac"."collections";

drop trigger if exists "partitions_delete_trigger" on "pgstac"."partitions";

drop trigger if exists "partitions_trigger" on "pgstac"."partitions";

drop trigger if exists "queryables_trigger" on "pgstac"."queryables";

alter table "pgstac"."partitions" drop constraint "partitions_collection_fkey";

alter table "pgstac"."partitions" drop constraint "prange";

drop function if exists "pgstac"."create_queryable_indexes"();

drop function if exists "pgstac"."partition_collection"(collection text, strategy pgstac.partition_trunc_strategy);

drop function if exists "pgstac"."partitions_delete_trigger_func"();

drop function if exists "pgstac"."partitions_trigger_func"();

drop function if exists "pgstac"."queryables_trigger_func"();

drop view if exists "pgstac"."partition_steps";

alter table "pgstac"."partitions" drop constraint "partitions_pkey";

drop index if exists "pgstac"."partitions_pkey";

select 1; -- drop index if exists "pgstac"."prange";

drop index if exists "pgstac"."partitions_range_idx";

drop table "pgstac"."partitions";

create table "pgstac"."partition_stats" (
    "partition" text not null,
    "dtrange" tstzrange,
    "edtrange" tstzrange,
    "spatial" geometry,
    "last_updated" timestamp with time zone,
    "keys" text[]
);


create table "pgstac"."query_queue" (
    "query" text not null,
    "added" timestamp with time zone default now()
);


create table "pgstac"."query_queue_history" (
    "query" text,
    "added" timestamp with time zone not null,
    "finished" timestamp with time zone not null default now(),
    "error" text
);


alter table "pgstac"."collections" alter column "partition_trunc" set data type text using "partition_trunc"::text;

drop type "pgstac"."partition_trunc_strategy";

CREATE UNIQUE INDEX partition_stats_pkey ON pgstac.partition_stats USING btree (partition);

CREATE UNIQUE INDEX query_queue_pkey ON pgstac.query_queue USING btree (query);

CREATE INDEX partitions_range_idx ON pgstac.partition_stats USING gist (dtrange);

alter table "pgstac"."partition_stats" add constraint "partition_stats_pkey" PRIMARY KEY using index "partition_stats_pkey";

alter table "pgstac"."query_queue" add constraint "query_queue_pkey" PRIMARY KEY using index "query_queue_pkey";

alter table "pgstac"."collections" add constraint "collections_partition_trunc_check" CHECK ((partition_trunc = ANY (ARRAY['year'::text, 'month'::text]))) not valid;

alter table "pgstac"."collections" validate constraint "collections_partition_trunc_check";

set check_function_bodies = off;

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
            $q$,
            _partition_name,
            _collection,
            concat(_partition_name,'_pk'),
            _partition_name
        );
    ELSE
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L) PARTITION BY RANGE (datetime);
                CREATE TABLE IF NOT EXISTS %I partition OF %I FOR VALUES FROM (%L) TO (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
            $q$,
            format('_items_%s', c.key),
            _collection,
            _partition_name,
            format('_items_%s', c.key),
            lower(_partition_dtrange),
            upper(_partition_dtrange),
            format('%s_pk', _partition_name),
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
    PERFORM create_table_constraints(_partition_name, _constraint_dtrange, _constraint_edtrange);
    RETURN _partition_name;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.constraint_tstzrange(expr text)
 RETURNS tstzrange
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
    WITH t AS (
        SELECT regexp_matches(
            expr,
            E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
        ) AS m
    ) SELECT tstzrange(m[1]::timestamptz, m[2]::timestamptz) FROM t
    ;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.content_dehydrate(item_content jsonb, collection_content jsonb)
 RETURNS pgstac.items
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT
        item_content->>'id' as id,
        stac_geom(item_content) as geometry,
        item_content->>'collection' as collection,
        stac_datetime(item_content) as datetime,
        stac_end_datetime(item_content) as end_datetime,
        (item_content
            - '{id,geometry,collection,type,assets}'::text[]
        )
            ||
        jsonb_build_object(
            'assets',
            ( strip_jsonb(item_content->'assets', collection_content->'item_assets'
            )
            )
        )
    ;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.create_table_constraints(t text, _dtrange tstzrange, _edtrange tstzrange)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    q text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM partitions WHERE partition=t) THEN
        RETURN NULL;
    END IF;
    RAISE NOTICE 'Creating Table Constraints for % % %', t, _dtrange, _edtrange;
    q :=format(
        $q$
            ALTER TABLE %I
                ADD CONSTRAINT %I
                    CHECK (
                        (datetime >= %L)
                        AND (datetime <= %L)
                        AND (end_datetime >= %L)
                        AND (end_datetime <= %L)
                    ) NOT VALID
            ;
        $q$,
        t,
        format('%s_dt', t),
        lower(_dtrange),
        upper(_dtrange),
        lower(_edtrange),
        upper(_edtrange)
    );
    PERFORM run_or_queue(q);
    q :=format(
        $q$
            ALTER TABLE %I
                VALIDATE CONSTRAINT %I
            ;
        $q$,
        t,
        format('%s_dt', t)
    );
    PERFORM run_or_queue(q);
    RETURN t;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.drop_table_constraints(t text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    q text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM partitions WHERE partition=t) THEN
        RETURN NULL;
    END IF;
    FOR q IN SELECT FORMAT(
        $q$
            ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        $q$,
        t,
        conname
    ) FROM pg_constraint
        WHERE conrelid=t::regclass::oid AND contype='c'
    LOOP
        EXECUTE q;
    END LOOP;
    RETURN t;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.dt_constraint(coid oid, OUT dt tstzrange, OUT edt tstzrange)
 RETURNS record
 LANGUAGE plpgsql
 STABLE STRICT
AS $function$
DECLARE
    expr text := pg_get_constraintdef(coid);
    matches timestamptz[];
BEGIN
    IF expr = 'CHECK (((datetime IS NULL) AND (end_datetime IS NULL)))' THEN
        dt := tstzrange('-infinity','-infinity');
        edt := tstzrange('-infinity', '-infinity');
        RETURN;
    END IF;
    WITH f AS (SELECT (regexp_matches(expr, E'([0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9])', 'g'))[1] f)
    SELECT array_agg(f::timestamptz) INTO matches FROM f;
    IF cardinality(matches) = 4 THEN
        dt := tstzrange(matches[1], matches[2],'[]');
        edt := tstzrange(matches[3], matches[4], '[]');
        RETURN;
    ELSIF cardinality(matches) = 2 THEN
        edt := tstzrange(matches[1], matches[2],'[]');
        RETURN;
    END IF;
    RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_setting_bool(_setting text, conf jsonb DEFAULT NULL::jsonb)
 RETURNS boolean
 LANGUAGE sql
AS $function$
SELECT COALESCE(
  conf->>_setting,
  current_setting(concat('pgstac.',_setting), TRUE),
  (SELECT value FROM pgstac.pgstac_settings WHERE name=_setting),
  'FALSE'
)::boolean;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.maintain_partition_queries(part text DEFAULT 'items'::text, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false)
 RETURNS SETOF text
 LANGUAGE plpgsql
AS $function$
DECLARE
    parent text;
    level int;
    isleaf bool;
    collection collections%ROWTYPE;
    subpart text;
    baseidx text;
    queryable_name text;
    queryable_property_index_type text;
    queryable_property_wrapper text;
    queryable_parsed RECORD;
    deletedidx pg_indexes%ROWTYPE;
    q text;
    idx text;
    collection_partition bigint;
BEGIN
    RAISE NOTICE 'Maintaining partition: %', part;

    -- Get root partition
    SELECT parentrelid::text, pt.isleaf, pt.level
        INTO parent, isleaf, level
    FROM pg_partition_tree('items') pt
    WHERE relid::text = part;
    IF NOT FOUND THEN
        RAISE NOTICE 'Partition % Does Not Exist In Partition Tree', part;
        RETURN;
    END IF;

    -- If this is a parent partition, recurse to leaves
    IF NOT isleaf THEN
        FOR subpart IN
            SELECT relid::text
            FROM pg_partition_tree(part)
            WHERE relid::text != part
        LOOP
            RAISE NOTICE 'Recursing to %', subpart;
            RETURN QUERY SELECT * FROM maintain_partition_queries(subpart, dropindexes, rebuildindexes);
        END LOOP;
        RETURN; -- Don't continue since not an end leaf
    END IF;


    -- Get collection
    collection_partition := ((regexp_match(part, E'^_items_([0-9]+)'))[1])::bigint;
    RAISE NOTICE 'COLLECTION PARTITION: %', collection_partition;
    SELECT * INTO STRICT collection
    FROM collections
    WHERE key = collection_partition;
    RAISE NOTICE 'COLLECTION ID: %s', collection.id;


    -- Create temp table with existing indexes
    CREATE TEMP TABLE existing_indexes ON COMMIT DROP AS
    SELECT *
    FROM pg_indexes
    WHERE schemaname='pgstac' AND tablename=part;


    -- Check if index exists for each queryable.
    FOR
        queryable_name,
        queryable_property_index_type,
        queryable_property_wrapper
    IN
        SELECT
            name,
            COALESCE(property_index_type, 'BTREE'),
            COALESCE(property_wrapper, 'to_text')
        FROM queryables
        WHERE
            name NOT in ('id', 'datetime', 'geometry')
            AND (
                collection_ids IS NULL
                OR collection_ids = '{}'::text[]
                OR collection.id = ANY (collection_ids)
            )
        UNION ALL
        SELECT 'datetime desc, end_datetime', 'BTREE', ''
        UNION ALL
        SELECT 'geometry', 'GIST', ''
        UNION ALL
        SELECT 'id', 'BTREE', ''
    LOOP
        baseidx := format(
            $q$ ON %I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
            part,
            queryable_property_index_type,
            queryable_property_wrapper,
            queryable_name
        );
        RAISE NOTICE 'BASEIDX: %', baseidx;
        RAISE NOTICE 'IDXSEARCH: %', format($q$[(']%s[')]$q$, queryable_name);
        -- If index already exists, delete it from existing indexes type table
        DELETE FROM existing_indexes
        WHERE indexdef ~* format($q$[(']%s[')]$q$, queryable_name)
        RETURNING * INTO deletedidx;
        RAISE NOTICE 'EXISTING INDEX: %', deletedidx;
        IF NOT FOUND THEN -- index did not exist, create it
            RETURN NEXT format('CREATE INDEX CONCURRENTLY %s;', baseidx);
        ELSIF rebuildindexes THEN
            RETURN NEXT format('REINDEX %I CONCURRENTLY;', deletedidx.indexname);
        END IF;
    END LOOP;

    -- Remove indexes that were not expected
    IF dropindexes THEN
        FOR idx IN SELECT indexname::text FROM existing_indexes
        LOOP
            RETURN NEXT format('DROP INDEX IF EXISTS %I;', idx);
        END LOOP;
    END IF;

    DROP TABLE existing_indexes;
    RETURN;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.maintain_partitions(part text DEFAULT 'items'::text, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false)
 RETURNS void
 LANGUAGE sql
AS $function$
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partitions_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
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
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L);', p));
    END LOOP;
    RAISE NOTICE 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_extent(part text)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    collection_partition collections%ROWTYPE;
    geom_extent geometry;
    mind timestamptz;
    maxd timestamptz;
    extent jsonb;
BEGIN
    EXECUTE FORMAT(
        '
        SELECT
            min(datetime),
            max(end_datetime)
        FROM %I;
        ',
        part
    ) INTO mind, maxd;
    geom_extent := ST_EstimatedExtent(part, 'geometry');
    extent := jsonb_build_object(
        'extent', jsonb_build_object(
            'spatial', jsonb_build_object(
                'bbox', to_jsonb(array[array[st_xmin(geom_extent), st_ymin(geom_extent), st_xmax(geom_extent), st_ymax(geom_extent)]])
            ),
            'temporal', jsonb_build_object(
                'interval', to_jsonb(array[array[mind, maxd]])
            )
        )
    );
    RETURN extent;
END;
$function$
;

create or replace view "pgstac"."partition_sys_meta" as  SELECT (pg_partition_tree.relid)::text AS partition,
    replace(replace(
        CASE
            WHEN (pg_partition_tree.level = 1) THEN pg_get_expr(c.relpartbound, c.oid)
            ELSE pg_get_expr(parent.relpartbound, parent.oid)
        END, 'FOR VALUES IN ('''::text, ''::text), ''')'::text, ''::text) AS collection,
    pg_partition_tree.level,
    c.reltuples,
    c.relhastriggers,
    COALESCE(pgstac.constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity'::timestamp with time zone, 'infinity'::timestamp with time zone, '[]'::text)) AS partition_dtrange,
    COALESCE((pgstac.dt_constraint(edt.oid)).dt, pgstac.constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity'::timestamp with time zone, 'infinity'::timestamp with time zone, '[]'::text)) AS constraint_dtrange,
    COALESCE((pgstac.dt_constraint(edt.oid)).edt, tstzrange('-infinity'::timestamp with time zone, 'infinity'::timestamp with time zone, '[]'::text)) AS constraint_edtrange
   FROM (((pg_partition_tree('pgstac.items'::regclass) pg_partition_tree(relid, parentrelid, isleaf, level)
     JOIN pg_class c ON (((pg_partition_tree.relid)::oid = c.oid)))
     JOIN pg_class parent ON ((((pg_partition_tree.parentrelid)::oid = parent.oid) AND pg_partition_tree.isleaf)))
     LEFT JOIN pg_constraint edt ON (((edt.conrelid = c.oid) AND (edt.contype = 'c'::"char"))))
  WHERE pg_partition_tree.isleaf;


create or replace view "pgstac"."partitions" as  SELECT partition_sys_meta.partition,
    partition_sys_meta.collection,
    partition_sys_meta.level,
    partition_sys_meta.reltuples,
    partition_sys_meta.relhastriggers,
    partition_sys_meta.partition_dtrange,
    partition_sys_meta.constraint_dtrange,
    partition_sys_meta.constraint_edtrange,
    partition_stats.dtrange,
    partition_stats.edtrange,
    partition_stats.spatial,
    partition_stats.last_updated,
    partition_stats.keys
   FROM (pgstac.partition_sys_meta
     LEFT JOIN pgstac.partition_stats USING (partition));


create or replace view "pgstac"."pgstac_indexes" as  SELECT i.schemaname,
    i.tablename,
    i.indexname,
    i.indexdef,
    COALESCE((regexp_match(i.indexdef, '\(([a-zA-Z]+)\)'::text))[1], (regexp_match(i.indexdef, '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_]+)''::text'::text))[1],
        CASE
            WHEN (i.indexdef ~* '\(datetime desc, end_datetime\)'::text) THEN 'datetime_end_datetime'::text
            ELSE NULL::text
        END) AS field,
    pg_table_size(((i.indexname)::text)::regclass) AS index_size,
    pg_size_pretty(pg_table_size(((i.indexname)::text)::regclass)) AS index_size_pretty,
    s.n_distinct,
    ((s.most_common_vals)::text)::text[] AS most_common_vals,
    ((s.most_common_freqs)::text)::text[] AS most_common_freqs,
    ((s.histogram_bounds)::text)::text[] AS histogram_bounds,
    s.correlation
   FROM (pg_indexes i
     LEFT JOIN pg_stats s ON ((s.tablename = i.indexname)))
  WHERE ((i.schemaname = 'pgstac'::name) AND (i.tablename ~ '_items_'::text));


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

    IF EXISTS (SELECT 1 FROM partitions WHERE collection=_collection LIMIT 1) THEN
        EXECUTE format(
            $q$
                CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
                DROP TABLE IF EXISTS %I CASCADE;
                WITH p AS (
                    SELECT
                        collection,
                        CASE WHEN %L IS NULL THEN '-infinity'::timestamptz
                        ELSE date_trunc(%L, datetime)
                        END as d,
                        tstzrange(min(datetime),max(datetime),'[]') as dtrange,
                        tstzrange(min(datetime),max(datetime),'[]') as edtrange
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

CREATE OR REPLACE FUNCTION pgstac.run_or_queue(query text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    use_queue text := COALESCE(get_setting('use_queue'), 'FALSE')::boolean;
BEGIN
    IF get_setting_bool('debug') THEN
        RAISE NOTICE '%', query;
    END IF;
    IF use_queue THEN
        INSERT INTO query_queue (query) VALUES (query) ON CONFLICT DO NOTHING;
    ELSE
        EXECUTE query;
    END IF;
    RETURN;
END;
$function$
;

CREATE OR REPLACE PROCEDURE pgstac.run_queued_queries()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    qitem query_queue%ROWTYPE;
    timeout text := get_setting('queue_timeout');
    timeout_ts timestamptz;
    error text;
BEGIN
    IF timeout IS NULL THEN
        timeout := '10 minutes';
    END IF;
    timeout_ts := clock_timestamp() + timeout::interval;
    WHILE TRUE AND clock_timestamp() < timeout_ts LOOP
        SELECT * INTO qitem FROM query_queue ORDER BY added DESC LIMIT 1 FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN
            EXIT;
        END IF;
        BEGIN
            RAISE NOTICE 'RUNNING QUERY: %', qitem.query;
            EXECUTE qitem.query;
            EXCEPTION WHEN others THEN
                error := format('%s | %s', SQLERRM, SQLSTATE);
        END;
        INSERT INTO query_queue_history (query, added, finished, error)
            VALUES (qitem.query, qitem.added, clock_timestamp(), error);
        DELETE FROM query_queue WHERE query = qitem.query;
        COMMIT;
    END LOOP;
END;
$procedure$
;

CREATE OR REPLACE FUNCTION pgstac.run_queued_queries_intransaction()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    qitem query_queue%ROWTYPE;
    timeout text := get_setting('queue_timeout');
    timeout_ts timestamptz;
    error text;
    cnt int := 0;
BEGIN
    IF timeout IS NULL THEN
        timeout := '10 minutes';
    END IF;
    timeout_ts := clock_timestamp() + timeout::interval;
    WHILE TRUE AND clock_timestamp() < timeout_ts LOOP
        SELECT * INTO qitem FROM query_queue ORDER BY added DESC LIMIT 1 FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN
            RETURN cnt;
        END IF;
        cnt := cnt + 1;
        BEGIN
            RAISE NOTICE 'RUNNING QUERY: %', qitem.query;
            EXECUTE qitem.query;
            EXCEPTION WHEN others THEN
                error := format('%s | %s', SQLERRM, SQLSTATE);
        END;
        INSERT INTO query_queue_history (query, added, finished, error)
            VALUES (qitem.query, qitem.added, clock_timestamp(), error);
        DELETE FROM query_queue WHERE query = qitem.query;
    END LOOP;
    RETURN cnt;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.update_partition_stats(_partition text)
 RETURNS void
 LANGUAGE plpgsql
 STRICT
AS $function$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    extent geometry;
BEGIN
    RAISE NOTICE 'Updating stats for %', _partition;
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
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.update_partition_stats_q(_partition text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    PERFORM run_or_queue(
        format('SELECT update_partition_stats(%L);', _partition)
    );
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.all_collections()
 RETURNS jsonb
 LANGUAGE sql
 SET search_path TO 'pgstac', 'public'
AS $function$
    SELECT jsonb_agg(content) FROM collections;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.collections_trigger_func()
 RETURNS trigger
 LANGUAGE plpgsql
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
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS DISTINCT FROM OLD.partition_trunc THEN
        PERFORM repartition(NEW.id, NEW.partition_trunc, TRUE);
    END IF;
    RETURN NEW;
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
    part text;
    ts timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Creating Partitions. %', clock_timestamp() - ts;

    FOR part IN WITH t AS (
        SELECT
            n.content->>'collection' as collection,
            stac_daterange(n.content->'properties') as dtr,
            partition_trunc
        FROM newdata n JOIN collections ON (n.content->>'collection'=collections.id)
    ), p AS (
        SELECT
            collection,
            COALESCE(date_trunc(partition_trunc::text, lower(dtr)),'-infinity') as d,
            tstzrange(min(lower(dtr)),max(lower(dtr)),'[]') as dtrange,
            tstzrange(min(upper(dtr)),max(upper(dtr)),'[]') as edtrange
        FROM t
        GROUP BY 1,2
    ) SELECT check_partition(collection, dtrange, edtrange) FROM p LOOP
        RAISE NOTICE 'Partition %', part;
    END LOOP;

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT
            (content_dehydrate(content)).*
        FROM newdata;
        RAISE NOTICE 'Doing the delete. %', clock_timestamp() - ts;
        DELETE FROM items_staging;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT
            (content_dehydrate(content)).*
        FROM newdata
        ON CONFLICT DO NOTHING;
        RAISE NOTICE 'Doing the delete. %', clock_timestamp() - ts;
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
        RAISE NOTICE 'Doing the delete. %', clock_timestamp() - ts;
        DELETE FROM items_staging_upsert;
    END IF;
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$function$
;

create or replace view "pgstac"."partition_steps" as  SELECT partitions.partition AS name,
    date_trunc('month'::text, lower(partitions.partition_dtrange)) AS sdate,
    (date_trunc('month'::text, upper(partitions.partition_dtrange)) + '1 mon'::interval) AS edate
   FROM pgstac.partitions
  WHERE ((partitions.partition_dtrange IS NOT NULL) AND (partitions.partition_dtrange <> 'empty'::tstzrange))
  ORDER BY partitions.dtrange;


CREATE TRIGGER items_after_delete_trigger AFTER UPDATE ON pgstac.items REFERENCING NEW TABLE AS newdata FOR EACH STATEMENT EXECUTE FUNCTION pgstac.partition_after_triggerfunc();

CREATE TRIGGER items_after_insert_trigger AFTER INSERT ON pgstac.items REFERENCING NEW TABLE AS newdata FOR EACH STATEMENT EXECUTE FUNCTION pgstac.partition_after_triggerfunc();

CREATE TRIGGER items_after_update_trigger AFTER DELETE ON pgstac.items REFERENCING OLD TABLE AS newdata FOR EACH STATEMENT EXECUTE FUNCTION pgstac.partition_after_triggerfunc();



-- END migra calculated SQL
INSERT INTO queryables (name, definition) VALUES
('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}'),
('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}'),
('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}')
ON CONFLICT DO NOTHING;

INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
('eo:cloud_cover','{"$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fieldsproperties/eo:cloud_cover"}','to_int','BTREE')
ON CONFLICT DO NOTHING;


INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default_filter_lang', 'cql2-json'),
  ('additional_properties', 'true'),
  ('index_build_on_trigger', 'true'),
  ('use_queue', 'false'),
  ('queue_timeout', '10 minutes')
ON CONFLICT DO NOTHING
;


GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

SELECT set_version('0.7.0');
