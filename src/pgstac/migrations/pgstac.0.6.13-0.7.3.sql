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
drop trigger if exists "queryables_collection_trigger" on "pgstac"."collections";

drop trigger if exists "partitions_delete_trigger" on "pgstac"."partitions";

drop trigger if exists "partitions_trigger" on "pgstac"."partitions";

alter table "pgstac"."partitions" drop constraint "partitions_collection_fkey";

alter table "pgstac"."partitions" drop constraint "prange";

drop function if exists "pgstac"."create_queryable_indexes"();

drop function if exists "pgstac"."partition_collection"(collection text, strategy pgstac.partition_trunc_strategy);

drop function if exists "pgstac"."partitions_delete_trigger_func"();

drop function if exists "pgstac"."partitions_trigger_func"();

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

CREATE INDEX queryables_collection_idx ON pgstac.queryables USING gin (collection_ids);

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
    PERFORM maintain_partitions(_partition_name);
    PERFORM update_partition_stats_q(_partition_name, true);
    RETURN _partition_name;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.collection_extent(_collection text, runupdate boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    geom_extent geometry;
    mind timestamptz;
    maxd timestamptz;
    extent jsonb;
BEGIN
    IF runupdate THEN
        PERFORM update_partition_stats_q(partition)
        FROM partitions WHERE collection=_collection;
    END IF;
    SELECT
        min(lower(dtrange)),
        max(upper(edtrange)),
        st_extent(spatial)
    INTO
        mind,
        maxd,
        geom_extent
    FROM partitions
    WHERE collection=_collection;

    IF geom_extent IS NOT NULL AND mind IS NOT NULL AND maxd IS NOT NULL THEN
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
    END IF;
    RETURN NULL;
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
    IF _dtrange = 'empty' AND _edtrange = 'empty' THEN
        q :=format(
            $q$
                DO $block$
                BEGIN
                    ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
                    ALTER TABLE %I
                        ADD CONSTRAINT %I
                            CHECK (((datetime IS NULL) AND (end_datetime IS NULL))) NOT VALID
                    ;
                    ALTER TABLE %I
                        VALIDATE CONSTRAINT %I
                    ;

                EXCEPTION WHEN others THEN
                    RAISE WARNING '%%, Issue Altering Constraints. Please run update_partition_stats(%I)', SQLERRM USING ERRCODE = SQLSTATE;
                END;
                $block$;
            $q$,
            t,
            format('%s_dt', t),
            t,
            format('%s_dt', t),
            t,
            format('%s_dt', t),
            t
        );
    ELSE
        q :=format(
            $q$
                DO $block$
                BEGIN

                    ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
                    ALTER TABLE %I
                        ADD CONSTRAINT %I
                            CHECK (
                                (datetime >= %L)
                                AND (datetime <= %L)
                                AND (end_datetime >= %L)
                                AND (end_datetime <= %L)
                            ) NOT VALID
                    ;
                    ALTER TABLE %I
                        VALIDATE CONSTRAINT %I
                    ;

                EXCEPTION WHEN others THEN
                    RAISE WARNING '%%, Issue Altering Constraints. Please run update_partition_stats(%I)', SQLERRM USING ERRCODE = SQLSTATE;
                END;
                $block$;
            $q$,
            t,
            format('%s_dt', t),
            t,
            format('%s_dt', t),
            lower(_dtrange),
            upper(_dtrange),
            lower(_edtrange),
            upper(_edtrange),
            t,
            format('%s_dt', t),
            t
        );
    END IF;
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
    IF expr LIKE '%NULL%' THEN
        dt := tstzrange(null::timestamptz, null::timestamptz);
        edt := tstzrange(null::timestamptz, null::timestamptz);
        RETURN;
    END IF;
    WITH f AS (SELECT (regexp_matches(expr, E'([0-9]{4}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]\.?[0-9]*)', 'g'))[1] f)
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

CREATE OR REPLACE FUNCTION pgstac.indexdef(q pgstac.queryables)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
    DECLARE
        out text;
    BEGIN
        IF q.name = 'id' THEN
            out := 'CREATE UNIQUE INDEX ON %I USING btree (id)';
        ELSIF q.name = 'datetime' THEN
            out := 'CREATE INDEX ON %I USING btree (datetime DESC, end_datetime)';
        ELSIF q.name = 'geometry' THEN
            out := 'CREATE INDEX ON %I USING gist (geometry)';
        ELSE
            out := format($q$CREATE INDEX ON %%I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
                lower(COALESCE(q.property_index_type, 'BTREE')),
                lower(COALESCE(q.property_wrapper, 'to_text')),
                q.name
            );
        END IF;
        RETURN btrim(out, ' \n\t');
    END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.maintain_partition_queries(part text DEFAULT 'items'::text, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false, idxconcurrently boolean DEFAULT false)
 RETURNS SETOF text
 LANGUAGE plpgsql
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
AS $function$
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partition_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.normalize_indexdef(def text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
DECLARE
BEGIN
    def := btrim(def, ' \n\t');
	def := regexp_replace(def, '^CREATE (UNIQUE )?INDEX ([^ ]* )?ON (ONLY )?([^ ]* )?', '', 'i');
    RETURN def;
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
    RAISE NOTICE 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
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
    regexp_replace(btrim(replace(replace(i.indexdef, (i.indexname)::text, ''::text), 'pgstac.'::text, ''::text), ' \t\n'::text), '[ ]+'::text, ' '::text, 'g'::text) AS idx,
    COALESCE((regexp_match(i.indexdef, '\(([a-zA-Z]+)\)'::text))[1], (regexp_match(i.indexdef, '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'::text))[1],
        CASE
            WHEN (i.indexdef ~* '\(datetime desc, end_datetime\)'::text) THEN 'datetime'::text
            ELSE NULL::text
        END) AS field,
    pg_table_size(((i.indexname)::text)::regclass) AS index_size,
    pg_size_pretty(pg_table_size(((i.indexname)::text)::regclass)) AS index_size_pretty
   FROM pg_indexes i
  WHERE ((i.schemaname = 'pgstac'::name) AND (i.tablename ~ '_items_'::text) AND (i.indexdef !~* ' only '::text));


create or replace view "pgstac"."pgstac_indexes_stats" as  SELECT i.schemaname,
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


CREATE OR REPLACE FUNCTION pgstac.queryable_signature(n text, c text[])
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT concat(n, c);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.queryables_constraint_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    allcollections text[];
BEGIN
    RAISE NOTICE 'Making sure that name/collection is unique for queryables';
    IF NEW.collection_ids IS NOT NULL THEN
        IF EXISTS (
            SELECT 1 FROM
                collections
                LEFT JOIN
                unnest(NEW.collection_ids) c
                ON (collections.id = c)
                WHERE c IS NULL
        ) THEN
            RAISE foreign_key_violation;
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
            RAISE unique_violation;
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

CREATE OR REPLACE FUNCTION pgstac.queue_timeout()
 RETURNS interval
 LANGUAGE sql
AS $function$
    SELECT t2s(coalesce(
            get_setting('queue_timeout'),
            '1h'
        ))::interval;
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
    timeout_ts timestamptz;
    error text;
    cnt int := 0;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        DELETE FROM query_queue WHERE query = (SELECT query FROM query_queue ORDER BY added DESC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING * INTO qitem;
        IF NOT FOUND THEN
            EXIT;
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
    timeout_ts timestamptz;
    error text;
    cnt int := 0;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        DELETE FROM query_queue WHERE query = (SELECT query FROM query_queue ORDER BY added DESC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING * INTO qitem;
        IF NOT FOUND THEN
            RETURN cnt;
        END IF;
        cnt := cnt + 1;
        BEGIN
            qitem.query := regexp_replace(qitem.query, 'CONCURRENTLY', '');
            RAISE NOTICE 'RUNNING QUERY: %', qitem.query;

            EXECUTE qitem.query;
            EXCEPTION WHEN others THEN
                error := format('%s | %s', SQLERRM, SQLSTATE);
                RAISE WARNING '%', error;
        END;
        INSERT INTO query_queue_history (query, added, finished, error)
            VALUES (qitem.query, qitem.added, clock_timestamp(), error);
    END LOOP;
    RETURN cnt;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.t2s(text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
AS $function$
    SELECT extract(epoch FROM $1::interval)::text || ' s';
$function$
;

CREATE OR REPLACE FUNCTION pgstac.unnest_collection(collection_ids text[] DEFAULT NULL::text[])
 RETURNS SETOF text
 LANGUAGE plpgsql
 STABLE
AS $function$
    DECLARE
    BEGIN
        IF collection_ids IS NULL THEN
            RETURN QUERY SELECT id FROM collections;
        END IF;
        RETURN QUERY SELECT unnest(collection_ids);
    END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.update_partition_stats(_partition text, istrigger boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 STRICT
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
        constraint_dtrange, constraint_edtrange, partitions.collection
        INTO cdtrange, cedtrange, collection
    FROM partitions WHERE partition = _partition;

    RAISE NOTICE 'Checking if we need to modify constraints.';
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
                collection_extent(%L),
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

CREATE OR REPLACE FUNCTION pgstac.update_partition_stats_q(_partition text, istrigger boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    PERFORM run_or_queue(
        format('SELECT update_partition_stats(%L, %L);', _partition, istrigger)
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

CREATE OR REPLACE PROCEDURE pgstac.analyze_items()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    q text;
    timeout_ts timestamptz;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        SELECT format('ANALYZE (VERBOSE, SKIP_LOCKED) %I;', relname) INTO q
        FROM pg_stat_user_tables
        WHERE relname like '_item%' AND (n_mod_since_analyze>0 OR last_analyze IS NULL) LIMIT 1;
        IF NOT FOUND THEN
            EXIT;
        END IF;
        RAISE NOTICE '%', q;
        EXECUTE q;
        COMMIT;
    END LOOP;
END;
$procedure$
;

CREATE OR REPLACE FUNCTION pgstac.check_pgstac_settings(_sysmem text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
 SET client_min_messages TO 'notice'
AS $function$
DECLARE
    settingval text;
    sysmem bigint := pg_size_bytes(_sysmem);
    effective_cache_size bigint := pg_size_bytes(current_setting('effective_cache_size', TRUE));
    shared_buffers bigint := pg_size_bytes(current_setting('shared_buffers', TRUE));
    work_mem bigint := pg_size_bytes(current_setting('work_mem', TRUE));
    max_connections int := current_setting('max_connections', TRUE);
    maintenance_work_mem bigint := pg_size_bytes(current_setting('maintenance_work_mem', TRUE));
    seq_page_cost float := current_setting('seq_page_cost', TRUE);
    random_page_cost float := current_setting('random_page_cost', TRUE);
    temp_buffers bigint := pg_size_bytes(current_setting('temp_buffers', TRUE));
    r record;
BEGIN
    IF _sysmem IS NULL THEN
      RAISE NOTICE 'Call function with the size of your system memory `SELECT check_pgstac_settings(''4GB'')` to get pg system setting recommendations.';
    ELSE
        IF effective_cache_size < (sysmem * 0.5) THEN
            RAISE WARNING 'effective_cache_size of % is set low for a system with %. Recomended value between % and %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.5), pg_size_pretty(sysmem * 0.75);
        ELSIF effective_cache_size > (sysmem * 0.75) THEN
            RAISE WARNING 'effective_cache_size of % is set high for a system with %. Recomended value between % and %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.5), pg_size_pretty(sysmem * 0.75);
        ELSE
            RAISE NOTICE 'effective_cache_size of % is set appropriately for a system with %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem);
        END IF;

        IF shared_buffers < (sysmem * 0.2) THEN
            RAISE WARNING 'shared_buffers of % is set low for a system with %. Recomended value between % and %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.2), pg_size_pretty(sysmem * 0.3);
        ELSIF shared_buffers > (sysmem * 0.3) THEN
            RAISE WARNING 'shared_buffers of % is set high for a system with %. Recomended value between % and %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.2), pg_size_pretty(sysmem * 0.3);
        ELSE
            RAISE NOTICE 'shared_buffers of % is set appropriately for a system with %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem);
        END IF;
        shared_buffers = sysmem * 0.3;
        IF maintenance_work_mem < (sysmem * 0.2) THEN
            RAISE WARNING 'maintenance_work_mem of % is set low for shared_buffers of %. Recomended value between % and %', pg_size_pretty(maintenance_work_mem), pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers * 0.2), pg_size_pretty(shared_buffers * 0.3);
        ELSIF maintenance_work_mem > (shared_buffers * 0.3) THEN
            RAISE WARNING 'maintenance_work_mem of % is set high for shared_buffers of %. Recomended value between % and %', pg_size_pretty(maintenance_work_mem), pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers * 0.2), pg_size_pretty(shared_buffers * 0.3);
        ELSE
            RAISE NOTICE 'maintenance_work_mem of % is set appropriately for shared_buffers of %', pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers);
        END IF;

        IF work_mem * max_connections > shared_buffers THEN
            RAISE WARNING 'work_mem setting of % is set high for % max_connections please reduce work_mem to % or decrease max_connections to %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers/max_connections), floor(shared_buffers/work_mem);
        ELSIF work_mem * max_connections < (shared_buffers * 0.75) THEN
            RAISE WARNING 'work_mem setting of % is set low for % max_connections you may consider raising work_mem to % or increasing max_connections to %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers/max_connections), floor(shared_buffers/work_mem);
        ELSE
            RAISE NOTICE 'work_mem setting of % and max_connections of % are adequate for shared_buffers of %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers);
        END IF;

        IF random_page_cost / seq_page_cost != 1.1 THEN
            RAISE WARNING 'random_page_cost (%) /seq_page_cost (%) should be set to 1.1 for SSD. Change random_page_cost to %', random_page_cost, seq_page_cost, 1.1 * seq_page_cost;
        ELSE
            RAISE NOTICE 'random_page_cost and seq_page_cost set appropriately for SSD';
        END IF;

        IF temp_buffers < greatest(pg_size_bytes('128MB'),(maintenance_work_mem / 2)) THEN
            RAISE WARNING 'pgstac makes heavy use of temp tables, consider raising temp_buffers from % to %', pg_size_pretty(temp_buffers), greatest('128MB', pg_size_pretty((shared_buffers / 16)));
        END IF;
    END IF;

    RAISE NOTICE 'VALUES FOR PGSTAC VARIABLES';
    RAISE NOTICE 'These can be set either as GUC system variables or by setting in the pgstac_settings table.';

    FOR r IN SELECT name, get_setting(name) as setting, CASE WHEN current_setting(concat('pgstac.',name), TRUE) IS NOT NULL THEN concat('pgstac.',name, ' GUC') WHEN value IS NOT NULL THEN 'pgstac_settings table' ELSE 'Not Set' END as loc FROM pgstac_settings LOOP
      RAISE NOTICE '% is set to % from the %', r.name, r.setting, r.loc;
    END LOOP;

    SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pg_cron';
    IF NOT FOUND OR settingval IS NULL THEN
        RAISE NOTICE 'Consider intalling pg_cron which can be used to automate tasks';
    ELSE
        RAISE NOTICE 'pg_cron % is installed', settingval;
    END IF;

    SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pgstattuple';
    IF NOT FOUND OR settingval IS NULL THEN
        RAISE NOTICE 'Consider installing the pgstattuple extension which can be used to help maintain tables and indexes.';
    ELSE
        RAISE NOTICE 'pgstattuple % is installed', settingval;
    END IF;

    SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pg_stat_statements';
    IF NOT FOUND OR settingval IS NULL THEN
        RAISE NOTICE 'Consider installing the pg_stat_statements extension which is very helpful for tracking the types of queries on the system';
    ELSE
        RAISE NOTICE 'pg_stat_statements % is installed', settingval;
        IF current_setting('pg_stat_statements.track_statements', TRUE) IS DISTINCT FROM 'all' THEN
            RAISE WARNING 'SET pg_stat_statements.track_statements TO ''all''; --In order to track statements within functions.';
        END IF;
    END IF;

END;
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

CREATE OR REPLACE FUNCTION pgstac.get_item(_id text, _collection text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE sql
 STABLE
 SET search_path TO 'pgstac', 'public'
AS $function$
    SELECT content_hydrate(items) FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_setting(_setting text, conf jsonb DEFAULT NULL::jsonb)
 RETURNS text
 LANGUAGE sql
AS $function$
SELECT COALESCE(
  nullif(conf->>_setting, ''),
  nullif(current_setting(concat('pgstac.',_setting), TRUE),''),
  nullif((SELECT value FROM pgstac.pgstac_settings WHERE name=_setting),'')
);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_token_filter(_search jsonb DEFAULT '{}'::jsonb, token_rec jsonb DEFAULT NULL::jsonb)
 RETURNS text
 LANGUAGE plpgsql
 SET transform_null_equals TO 'true'
AS $function$
DECLARE
    token_id text;
    filters text[] := '{}'::text[];
    prev boolean := TRUE;
    field text;
    dir text;
    sort record;
    orfilter text := '';
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
        IF token_id IS NULL OR token_id = '' THEN
            RAISE WARNING 'next or prev set, but no token id found';
            RETURN NULL;
        END IF;
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
            orfilter := NULL;
            RAISE NOTICE 'SORT: %', sort;
            IF sort._val IS NOT NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
                orfilter := format($f$(
                    (%s < %s) OR (%s IS NULL)
                )$f$,
                sort._field,
                sort._val,
                sort._val
                );
            ELSIF sort._val IS NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
                RAISE NOTICE '< but null';
                orfilter := format('%s IS NOT NULL', sort._field);
            ELSIF sort._val IS NULL THEN
                RAISE NOTICE '> but null';
                --orfilter := format('%s IS NULL', sort._field);
            ELSE
                orfilter := format($f$(
                    (%s > %s) OR (%s IS NULL)
                )$f$,
                sort._field,
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

    DROP TABLE IF EXISTS sorts;
    token_where := concat('(',coalesce(output,'true'),')');
    IF trim(token_where) = '' THEN
        token_where := NULL;
    END IF;
    RAISE NOTICE 'TOKEN_WHERE: %',token_where;
    RETURN token_where;
    END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.item_by_id(_id text, _collection text DEFAULT NULL::text)
 RETURNS pgstac.items
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    i items%ROWTYPE;
BEGIN
    SELECT * INTO i FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection) LIMIT 1;
    RETURN i;
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

CREATE OR REPLACE FUNCTION pgstac.parse_dtrange(_indate jsonb, relative_base timestamp with time zone DEFAULT date_trunc('hour'::text, CURRENT_TIMESTAMP))
 RETURNS tstzrange
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE STRICT
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE
    timestrs text[];
    s timestamptz;
    e timestamptz;
BEGIN
    timestrs :=
    CASE
        WHEN _indate ? 'timestamp' THEN
            ARRAY[_indate->>'timestamp']
        WHEN _indate ? 'interval' THEN
            to_text_array(_indate->'interval')
        WHEN jsonb_typeof(_indate) = 'array' THEN
            to_text_array(_indate)
        ELSE
            regexp_split_to_array(
                _indate->>0,
                '/'
            )
    END;
    RAISE NOTICE 'TIMESTRS %', timestrs;
    IF cardinality(timestrs) = 1 THEN
        IF timestrs[1] ILIKE 'P%' THEN
            RETURN tstzrange(relative_base - upper(timestrs[1])::interval, relative_base, '[)');
        END IF;
        s := timestrs[1]::timestamptz;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) != 2 THEN
        RAISE EXCEPTION 'Timestamp cannot have more than 2 values';
    END IF;

    IF timestrs[1] = '..' OR timestrs[1] = '' THEN
        s := '-infinity'::timestamptz;
        e := timestrs[2]::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] = '..' OR timestrs[2] = '' THEN
        s := timestrs[1]::timestamptz;
        e := 'infinity'::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[1] ILIKE 'P%' AND timestrs[2] NOT ILIKE 'P%' THEN
        e := timestrs[2]::timestamptz;
        s := e - upper(timestrs[1])::interval;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] ILIKE 'P%' AND timestrs[1] NOT ILIKE 'P%' THEN
        s := timestrs[1]::timestamptz;
        e := s + upper(timestrs[2])::interval;
        RETURN tstzrange(s,e,'[)');
    END IF;

    s := timestrs[1]::timestamptz;
    e := timestrs[2]::timestamptz;

    RETURN tstzrange(s,e,'[)');

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


CREATE OR REPLACE FUNCTION pgstac.queryables_trigger_func()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
    PERFORM maintain_partitions();
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
 SET cursor_tuple_fraction TO '1'
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
CREATE TEMP TABLE results (i int GENERATED ALWAYS AS IDENTITY, content jsonb) ON COMMIT DROP;
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
        RAISE NOTICE 'cursor_tuple_fraction: %', current_setting('cursor_tuple_fraction');
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

WITH ordered AS (SELECT * FROM results WHERE content IS NOT NULL ORDER BY i)
SELECT jsonb_agg(content) INTO out_records FROM ordered;

DROP TABLE results;


-- Flip things around if this was the result of a prev token query
IF token_type='prev' THEN
    out_records := flip_jsonb_array(out_records);
    first_item := last_item;
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

CREATE OR REPLACE FUNCTION pgstac.search_query(_search jsonb DEFAULT '{}'::jsonb, updatestats boolean DEFAULT false, _metadata jsonb DEFAULT '{}'::jsonb)
 RETURNS pgstac.searches
 LANGUAGE plpgsql
 SECURITY DEFINER
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
        search._where := stac_search_to_where(_search);
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

CREATE OR REPLACE PROCEDURE pgstac.validate_constraints()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    q text;
BEGIN
    FOR q IN
    SELECT
        FORMAT(
            'ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;',
            nsp.nspname,
            cls.relname,
            con.conname
        )

    FROM pg_constraint AS con
        JOIN pg_class AS cls
        ON con.conrelid = cls.oid
        JOIN pg_namespace AS nsp
        ON cls.relnamespace = nsp.oid
    WHERE convalidated = FALSE AND contype in ('c','f')
    AND nsp.nspname = 'pgstac'
    LOOP
        RAISE NOTICE '%', q;
        PERFORM run_or_queue(q);
        COMMIT;
    END LOOP;
END;
$procedure$
;

CREATE OR REPLACE FUNCTION pgstac.where_stats(inwhere text, updatestats boolean DEFAULT false, conf jsonb DEFAULT NULL::jsonb)
 RETURNS pgstac.search_wheres
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    partitions text[];
    sw search_wheres%ROWTYPE;
    inwhere_hash text := md5(inwhere);
    _context text := lower(context(conf));
    _stats_ttl interval := context_stats_ttl(conf);
    _estimated_cost float := context_estimated_cost(conf);
    _estimated_count int := context_estimated_count(conf);
BEGIN
    IF _context = 'off' THEN
        sw._where := inwhere;
        return sw;
    END IF;

    SELECT * INTO sw FROM search_wheres WHERE md5(_where)=inwhere_hash FOR UPDATE;

    -- Update statistics if explicitly set, if statistics do not exist, or statistics ttl has expired
    IF NOT updatestats THEN
        RAISE NOTICE 'Checking if update is needed for: % .', inwhere;
        RAISE NOTICE 'Stats Last Updated: %', sw.statslastupdated;
        RAISE NOTICE 'TTL: %, Age: %', _stats_ttl, now() - sw.statslastupdated;
        RAISE NOTICE 'Context: %, Existing Total: %', _context, sw.total_count;
        IF
            sw.statslastupdated IS NULL
            OR (now() - sw.statslastupdated) > _stats_ttl
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
        WHERE md5(_where) = inwhere_hash
        RETURNING * INTO sw
        ;
        RETURN sw;
    END IF;

    -- Use explain to get estimated count/cost and a list of the partitions that would be hit by the query
    t := clock_timestamp();
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
    INTO explain_json;
    RAISE NOTICE 'Time for just the explain: %', clock_timestamp() - t;
    i := clock_timestamp() - t;

    sw.statslastupdated := now();
    sw.estimated_count := explain_json->0->'Plan'->'Plan Rows';
    sw.estimated_cost := explain_json->0->'Plan'->'Total Cost';
    sw.time_to_estimate := extract(epoch from i);

    RAISE NOTICE 'ESTIMATED_COUNT: % < %', sw.estimated_count, _estimated_count;
    RAISE NOTICE 'ESTIMATED_COST: % < %', sw.estimated_cost, _estimated_cost;

    -- Do a full count of rows if context is set to on or if auto is set and estimates are low enough
    IF
        _context = 'on'
        OR
        ( _context = 'auto' AND
            (
                sw.estimated_count < _estimated_count
                AND
                sw.estimated_cost < _estimated_cost
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
            total_count = sw.total_count,
            time_to_count = sw.time_to_count
    ;
    RETURN sw;
END;
$function$
;

CREATE TRIGGER items_after_delete_trigger AFTER UPDATE ON pgstac.items REFERENCING NEW TABLE AS newdata FOR EACH STATEMENT EXECUTE FUNCTION pgstac.partition_after_triggerfunc();

CREATE TRIGGER items_after_insert_trigger AFTER INSERT ON pgstac.items REFERENCING NEW TABLE AS newdata FOR EACH STATEMENT EXECUTE FUNCTION pgstac.partition_after_triggerfunc();

CREATE TRIGGER items_after_update_trigger AFTER DELETE ON pgstac.items REFERENCING OLD TABLE AS newdata FOR EACH STATEMENT EXECUTE FUNCTION pgstac.partition_after_triggerfunc();

CREATE TRIGGER queryables_constraint_insert_trigger BEFORE INSERT ON pgstac.queryables FOR EACH ROW EXECUTE FUNCTION pgstac.queryables_constraint_triggerfunc();

CREATE TRIGGER queryables_constraint_update_trigger BEFORE UPDATE ON pgstac.queryables FOR EACH ROW WHEN (((new.name = old.name) AND (new.collection_ids IS DISTINCT FROM old.collection_ids))) EXECUTE FUNCTION pgstac.queryables_constraint_triggerfunc();



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
  ('update_collection_extent', 'false')
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
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

SELECT update_partition_stats_q(partition) FROM partitions;
SELECT set_version('0.7.3');
