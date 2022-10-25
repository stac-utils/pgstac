CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

DO $$
  BEGIN
    CREATE ROLE pgstac_admin;
    CREATE ROLE pgstac_read;
    CREATE ROLE pgstac_ingest;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

GRANT pgstac_admin TO current_user;

CREATE SCHEMA IF NOT EXISTS pgstac AUTHORIZATION pgstac_admin;

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


CREATE TABLE IF NOT EXISTS migrations (
  version text PRIMARY KEY,
  datetime timestamptz DEFAULT clock_timestamp() NOT NULL
);

CREATE OR REPLACE FUNCTION get_version() RETURNS text AS $$
  SELECT version FROM pgstac.migrations ORDER BY datetime DESC, version DESC LIMIT 1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION set_version(text) RETURNS text AS $$
  INSERT INTO pgstac.migrations (version) VALUES ($1)
  ON CONFLICT DO NOTHING
  RETURNING version;
$$ LANGUAGE SQL;


CREATE TABLE IF NOT EXISTS pgstac_settings (
  name text PRIMARY KEY,
  value text NOT NULL
);

INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default-filter-lang', 'cql2-json'),
  ('additional_properties', 'true')
ON CONFLICT DO NOTHING
;


CREATE OR REPLACE FUNCTION get_setting(IN _setting text, IN conf jsonb DEFAULT NULL) RETURNS text AS $$
SELECT COALESCE(
  conf->>_setting,
  current_setting(concat('pgstac.',_setting), TRUE),
  (SELECT value FROM pgstac.pgstac_settings WHERE name=_setting)
);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context(conf jsonb DEFAULT NULL) RETURNS text AS $$
  SELECT pgstac.get_setting('context', conf);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context_estimated_count(conf jsonb DEFAULT NULL) RETURNS int AS $$
  SELECT pgstac.get_setting('context_estimated_count', conf)::int;
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_estimated_cost();
CREATE OR REPLACE FUNCTION context_estimated_cost(conf jsonb DEFAULT NULL) RETURNS float AS $$
  SELECT pgstac.get_setting('context_estimated_cost', conf)::float;
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS context_stats_ttl();
CREATE OR REPLACE FUNCTION context_stats_ttl(conf jsonb DEFAULT NULL) RETURNS interval AS $$
  SELECT pgstac.get_setting('context_stats_ttl', conf)::interval;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION notice(VARIADIC text[]) RETURNS boolean AS $$
DECLARE
debug boolean := current_setting('pgstac.debug', true);
BEGIN
    IF debug THEN
        RAISE NOTICE 'NOTICE FROM FUNC: %  >>>>> %', concat_ws(' | ', $1), clock_timestamp();
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION empty_arr(ANYARRAY) RETURNS BOOLEAN AS $$
SELECT CASE
  WHEN $1 IS NULL THEN TRUE
  WHEN cardinality($1)<1 THEN TRUE
ELSE FALSE
END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_intersection(_a ANYARRAY, _b ANYARRAY) RETURNS ANYARRAY AS $$
  SELECT ARRAY ( SELECT unnest(_a) INTERSECT SELECT UNNEST(_b) );
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION array_map_ident(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_ident(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_map_literal(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_literal(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE SQL STRICT IMMUTABLE;
CREATE OR REPLACE FUNCTION to_int(jsonb) RETURNS int AS $$
    SELECT floor(($1->>0)::float)::int;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION to_float(jsonb) RETURNS float AS $$
    SELECT ($1->>0)::float;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION to_tstz(jsonb) RETURNS timestamptz AS $$
    SELECT ($1->>0)::timestamptz;
$$ LANGUAGE SQL IMMUTABLE STRICT SET TIME ZONE 'UTC';


CREATE OR REPLACE FUNCTION to_text(jsonb) RETURNS text AS $$
    SELECT CASE WHEN jsonb_typeof($1) IN ('array','object') THEN $1::text ELSE $1->>0 END;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION to_text_array(jsonb) RETURNS text[] AS $$
    SELECT
        CASE jsonb_typeof($1)
            WHEN 'array' THEN ARRAY(SELECT jsonb_array_elements_text($1))
            ELSE ARRAY[$1->>0]
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS geometry AS $$
SELECT CASE jsonb_array_length(_bbox)
    WHEN 4 THEN
        ST_SetSRID(ST_MakeEnvelope(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float,
            (_bbox->>3)::float
        ),4326)
    WHEN 6 THEN
    ST_SetSRID(ST_3DMakeBox(
        ST_MakePoint(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float
        ),
        ST_MakePoint(
            (_bbox->>3)::float,
            (_bbox->>4)::float,
            (_bbox->>5)::float
        )
    ),4326)
    ELSE null END;
;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION geom_bbox(_geom geometry) RETURNS jsonb AS $$
    SELECT jsonb_build_array(
        st_xmin(_geom),
        st_ymin(_geom),
        st_xmax(_geom),
        st_ymax(_geom)
    );
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION flip_jsonb_array(j jsonb) RETURNS jsonb AS $$
    SELECT jsonb_agg(value) FROM (SELECT value FROM jsonb_array_elements(j) WITH ORDINALITY ORDER BY ordinality DESC) as t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION explode_dotpaths(j jsonb) RETURNS SETOF text[] AS $$
    SELECT string_to_array(p, '.') as e FROM jsonb_array_elements_text(j) p;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION explode_dotpaths_recurse(IN j jsonb) RETURNS SETOF text[] AS $$
    WITH RECURSIVE t AS (
        SELECT e FROM explode_dotpaths(j) e
        UNION ALL
        SELECT e[1:cardinality(e)-1]
        FROM t
        WHERE cardinality(e)>1
    ) SELECT e FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_set_nested(j jsonb, path text[], val jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE PLPGSQL IMMUTABLE;



CREATE OR REPLACE FUNCTION jsonb_include(j jsonb, f jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_exclude(j jsonb, f jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_fields(j jsonb, f jsonb DEFAULT '{"fields":[]}') RETURNS jsonb AS $$
    SELECT jsonb_exclude(jsonb_include(j, f), f);
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION merge_jsonb(_a jsonb, _b jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION strip_jsonb(_a jsonb, _b jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE SQL IMMUTABLE;
/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
    CASE
            WHEN value ? 'intersects' THEN
                ST_GeomFromGeoJSON(value->>'intersects')
            WHEN value ? 'geometry' THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value ? 'bbox' THEN
                pgstac.bbox_geom(value->'bbox')
            ELSE NULL
        END as geometry
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION stac_daterange(
    value jsonb
) RETURNS tstzrange AS $$
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
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_datetime(value jsonb) RETURNS timestamptz AS $$
    SELECT lower(stac_daterange(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_end_datetime(value jsonb) RETURNS timestamptz AS $$
    SELECT upper(stac_daterange(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';


CREATE TABLE IF NOT EXISTS stac_extensions(
    name text PRIMARY KEY,
    url text,
    enbabled_by_default boolean NOT NULL DEFAULT TRUE,
    enableable boolean NOT NULL DEFAULT TRUE
);

INSERT INTO stac_extensions (name, url) VALUES
    ('fields', 'https://api.stacspec.org/v1.0.0-beta.5/item-search#fields'),
    ('sort','https://api.stacspec.org/v1.0.0-beta.5/item-search#sort'),
    ('context','https://api.stacspec.org/v1.0.0-beta.5/item-search#context'),
    ('filter', 'https://api.stacspec.org/v1.0.0-beta.5/item-search#filter'),
    ('query', 'https://api.stacspec.org/v1.0.0-beta.5/item-search#query')
ON CONFLICT (name) DO UPDATE SET url=EXCLUDED.url;



CREATE OR REPLACE FUNCTION collection_base_item(content jsonb) RETURNS jsonb AS $$
    SELECT jsonb_build_object(
        'type', 'Feature',
        'stac_version', content->'stac_version',
        'assets', content->'item_assets',
        'collection', content->'id'
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TYPE partition_trunc_strategy AS ENUM ('year', 'month');

CREATE TABLE IF NOT EXISTS collections (
    key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id text GENERATED ALWAYS AS (content->>'id') STORED UNIQUE,
    content JSONB NOT NULL,
    base_item jsonb GENERATED ALWAYS AS (pgstac.collection_base_item(content)) STORED,
    partition_trunc partition_trunc_strategy
);


CREATE OR REPLACE FUNCTION collection_base_item(cid text) RETURNS jsonb AS $$
    SELECT pgstac.collection_base_item(content) FROM pgstac.collections WHERE id = cid LIMIT 1;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION table_empty(text) RETURNS boolean AS $$
DECLARE
    retval boolean;
BEGIN
    EXECUTE format($q$
        SELECT NOT EXISTS (SELECT 1 FROM %I LIMIT 1)
        $q$,
        $1
    ) INTO retval;
    RETURN retval;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION collections_trigger_func() RETURNS TRIGGER AS $$
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
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collections_trigger AFTER INSERT OR UPDATE ON collections FOR EACH ROW
EXECUTE FUNCTION collections_trigger_func();

CREATE OR REPLACE FUNCTION partition_collection(collection text, strategy partition_trunc_strategy) RETURNS text AS $$
    UPDATE collections SET partition_trunc=strategy WHERE id=collection RETURNING partition_trunc;
$$ LANGUAGE SQL;

CREATE TABLE IF NOT EXISTS partitions (
    collection text REFERENCES collections(id) ON DELETE CASCADE,
    name text PRIMARY KEY,
    partition_range tstzrange NOT NULL DEFAULT tstzrange('-infinity'::timestamptz,'infinity'::timestamptz, '[]'),
    datetime_range tstzrange,
    end_datetime_range tstzrange,
    CONSTRAINT prange EXCLUDE USING GIST (
        collection WITH =,
        partition_range WITH &&
    )
) WITH (FILLFACTOR=90);
CREATE INDEX partitions_range_idx ON partitions USING GIST(partition_range);

CREATE OR REPLACE FUNCTION partitions_delete_trigger_func() RETURNS TRIGGER AS $$
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
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER partitions_delete_trigger BEFORE DELETE ON partitions FOR EACH ROW
EXECUTE FUNCTION partitions_delete_trigger_func();

CREATE OR REPLACE FUNCTION partition_name(
    IN collection text,
    IN dt timestamptz,
    OUT partition_name text,
    OUT partition_range tstzrange
) AS $$
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
$$ LANGUAGE PLPGSQL STABLE;



CREATE OR REPLACE FUNCTION partitions_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    q text;
    cq text;
    parent_name text;
    partition_trunc text;
    partition_name text := NEW.name;
    partition_exists boolean := false;
    partition_empty boolean := true;
    partition_range tstzrange;
    datetime_range tstzrange;
    end_datetime_range tstzrange;
    err_context text;
    mindt timestamptz := lower(NEW.datetime_range);
    maxdt timestamptz := upper(NEW.datetime_range);
    minedt timestamptz := lower(NEW.end_datetime_range);
    maxedt timestamptz := upper(NEW.end_datetime_range);
    t_mindt timestamptz;
    t_maxdt timestamptz;
    t_minedt timestamptz;
    t_maxedt timestamptz;
BEGIN
    RAISE NOTICE 'Partitions Trigger. %', NEW;
    datetime_range := NEW.datetime_range;
    end_datetime_range := NEW.end_datetime_range;

    SELECT
        format('_items_%s', key),
        c.partition_trunc::text
    INTO
        parent_name,
        partition_trunc
    FROM pgstac.collections c
    WHERE c.id = NEW.collection;
    SELECT (pgstac.partition_name(NEW.collection, mindt)).* INTO partition_name, partition_range;
    NEW.name := partition_name;

    IF partition_range IS NULL OR partition_range = 'empty'::tstzrange THEN
        partition_range :=  tstzrange('-infinity'::timestamptz, 'infinity'::timestamptz, '[]');
    END IF;

    NEW.partition_range := partition_range;
    IF TG_OP = 'UPDATE' THEN
        mindt := least(mindt, lower(OLD.datetime_range));
        maxdt := greatest(maxdt, upper(OLD.datetime_range));
        minedt := least(minedt, lower(OLD.end_datetime_range));
        maxedt := greatest(maxedt, upper(OLD.end_datetime_range));
        NEW.datetime_range := tstzrange(mindt, maxdt, '[]');
        NEW.end_datetime_range := tstzrange(minedt, maxedt, '[]');
    END IF;
    IF TG_OP = 'INSERT' THEN

        IF partition_range != tstzrange('-infinity'::timestamptz, 'infinity'::timestamptz, '[]') THEN

            RAISE NOTICE '% % %', partition_name, parent_name, partition_range;
            q := format($q$
                CREATE TABLE IF NOT EXISTS %I partition OF %I FOR VALUES FROM (%L) TO (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                $q$,
                partition_name,
                parent_name,
                lower(partition_range),
                upper(partition_range),
                format('%s_pkey', partition_name),
                partition_name
            );
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
        END IF;

    END IF;

    -- Update constraints
    EXECUTE format($q$
        SELECT
            min(datetime),
            max(datetime),
            min(end_datetime),
            max(end_datetime)
        FROM %I;
        $q$, partition_name)
    INTO t_mindt, t_maxdt, t_minedt, t_maxedt;
    mindt := least(mindt, t_mindt);
    maxdt := greatest(maxdt, t_maxdt);
    minedt := least(mindt, minedt, t_minedt);
    maxedt := greatest(maxdt, maxedt, t_maxedt);

    mindt := date_trunc(coalesce(partition_trunc, 'year'), mindt);
    maxdt := date_trunc(coalesce(partition_trunc, 'year'), maxdt - '1 second'::interval) + concat('1 ',coalesce(partition_trunc, 'year'))::interval;
    minedt := date_trunc(coalesce(partition_trunc, 'year'), minedt);
    maxedt := date_trunc(coalesce(partition_trunc, 'year'), maxedt - '1 second'::interval) + concat('1 ',coalesce(partition_trunc, 'year'))::interval;


    IF mindt IS NOT NULL AND maxdt IS NOT NULL AND minedt IS NOT NULL AND maxedt IS NOT NULL THEN
        NEW.datetime_range := tstzrange(mindt, maxdt, '[]');
        NEW.end_datetime_range := tstzrange(minedt, maxedt, '[]');
        IF
            TG_OP='UPDATE'
            AND OLD.datetime_range @> NEW.datetime_range
            AND OLD.end_datetime_range @> NEW.end_datetime_range
        THEN
            RAISE NOTICE 'Range unchanged, not updating constraints.';
        ELSE

            RAISE NOTICE '
                SETTING CONSTRAINTS
                    mindt:  %, maxdt:  %
                    minedt: %, maxedt: %
                ', mindt, maxdt, minedt, maxedt;
            IF partition_trunc IS NULL THEN
                cq := format($q$
                    ALTER TABLE %7$I
                        DROP CONSTRAINT IF EXISTS %1$I,
                        DROP CONSTRAINT IF EXISTS %2$I,
                        ADD CONSTRAINT %1$I
                            CHECK (
                                (datetime >= %3$L)
                                AND (datetime <= %4$L)
                                AND (end_datetime >= %5$L)
                                AND (end_datetime <= %6$L)
                            ) NOT VALID
                    ;
                    ALTER TABLE %7$I
                        VALIDATE CONSTRAINT %1$I;
                    $q$,
                    format('%s_dt', partition_name),
                    format('%s_edt', partition_name),
                    mindt,
                    maxdt,
                    minedt,
                    maxedt,
                    partition_name
                );
            ELSE
                cq := format($q$
                    ALTER TABLE %5$I
                        DROP CONSTRAINT IF EXISTS %1$I,
                        DROP CONSTRAINT IF EXISTS %2$I,
                        ADD CONSTRAINT %2$I
                            CHECK ((end_datetime >= %3$L) AND (end_datetime <= %4$L)) NOT VALID
                    ;
                    ALTER TABLE %5$I
                        VALIDATE CONSTRAINT %2$I;
                    $q$,
                    format('%s_dt', partition_name),
                    format('%s_edt', partition_name),
                    minedt,
                    maxedt,
                    partition_name
                );

            END IF;
            RAISE NOTICE 'Altering Constraints. %', cq;
            EXECUTE cq;
        END IF;
    ELSE
        NEW.datetime_range = NULL;
        NEW.end_datetime_range = NULL;

        cq := format($q$
            ALTER TABLE %3$I
                DROP CONSTRAINT IF EXISTS %1$I,
                DROP CONSTRAINT IF EXISTS %2$I,
                ADD CONSTRAINT %1$I
                    CHECK ((datetime IS NULL AND end_datetime IS NULL)) NOT VALID
            ;
            ALTER TABLE %3$I
                VALIDATE CONSTRAINT %1$I;
            $q$,
            format('%s_dt', partition_name),
            format('%s_edt', partition_name),
            partition_name
        );
        EXECUTE cq;
    END IF;

    RETURN NEW;

END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER partitions_trigger BEFORE INSERT OR UPDATE ON partitions FOR EACH ROW
EXECUTE FUNCTION partitions_trigger_func();


CREATE OR REPLACE FUNCTION create_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION update_collection(data jsonb) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    UPDATE collections SET content=data WHERE id = data->>'id' RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ON CONFLICT (id) DO
    UPDATE
        SET content=EXCLUDED.content
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION delete_collection(_id text) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    DELETE FROM collections WHERE id = _id RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
    SELECT content FROM collections
    WHERE id=$1
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION all_collections() RETURNS jsonb AS $$
    SELECT jsonb_agg(content) FROM collections;
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;
CREATE TABLE queryables (
    id bigint GENERATED ALWAYS AS identity PRIMARY KEY,
    name text UNIQUE NOT NULL,
    collection_ids text[], -- used to determine what partitions to create indexes on
    definition jsonb,
    property_path text,
    property_wrapper text,
    property_index_type text
);
CREATE INDEX queryables_name_idx ON queryables (name);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);


INSERT INTO queryables (name, definition) VALUES
('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}'),
('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}'),
('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}')
ON CONFLICT DO NOTHING;

INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
('eo:cloud_cover','{"$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fieldsproperties/eo:cloud_cover"}','to_int','BTREE')
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION array_to_path(arr text[]) RETURNS text AS $$
    SELECT string_agg(
        quote_literal(v),
        '->'
    ) FROM unnest(arr) v;
$$ LANGUAGE SQL IMMUTABLE STRICT;




CREATE OR REPLACE FUNCTION queryable(
    IN dotpath text,
    OUT path text,
    OUT expression text,
    OUT wrapper text,
    OUT nulled_wrapper text
) AS $$
DECLARE
    q RECORD;
    path_elements text[];
BEGIN
    IF dotpath IN ('id', 'geometry', 'datetime', 'end_datetime', 'collection') THEN
        path := dotpath;
        expression := dotpath;
        wrapper := NULL;
        RETURN;
    END IF;
    SELECT * INTO q FROM queryables
        WHERE
            name=dotpath
            OR name = 'properties.' || dotpath
            OR name = replace(dotpath, 'properties.', '')
    ;
    IF q.property_wrapper IS NULL THEN
        IF q.definition->>'type' = 'number' THEN
            wrapper := 'to_float';
            nulled_wrapper := wrapper;
        ELSIF q.definition->>'format' = 'date-time' THEN
            wrapper := 'to_tstz';
            nulled_wrapper := wrapper;
        ELSE
            nulled_wrapper := NULL;
            wrapper := 'to_text';
        END IF;
    ELSE
        wrapper := q.property_wrapper;
        nulled_wrapper := wrapper;
    END IF;
    IF q.property_path IS NOT NULL THEN
        path := q.property_path;
    ELSE
        path_elements := string_to_array(dotpath, '.');
        IF path_elements[1] IN ('links', 'assets', 'stac_version', 'stac_extensions') THEN
            path := format('content->%s', array_to_path(path_elements));
        ELSIF path_elements[1] = 'properties' THEN
            path := format('content->%s', array_to_path(path_elements));
        ELSE
            path := format($F$content->'properties'->%s$F$, array_to_path(path_elements));
        END IF;
    END IF;
    expression := format('%I(%s)', wrapper, path);
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;

CREATE OR REPLACE FUNCTION create_queryable_indexes() RETURNS VOID AS $$
DECLARE
    queryable RECORD;
    q text;
BEGIN
    FOR queryable IN
        SELECT
            queryables.id as qid,
            CASE WHEN collections.key IS NULL THEN 'items' ELSE format('_items_%s',collections.key) END AS part,
            property_index_type,
            expression
            FROM
            queryables
            LEFT JOIN collections ON (collections.id = ANY (queryables.collection_ids))
            JOIN LATERAL queryable(queryables.name) ON (queryables.property_index_type IS NOT NULL)
        LOOP
        q := format(
            $q$
                CREATE INDEX IF NOT EXISTS %I ON %I USING %s ((%s));
            $q$,
            format('%s_%s_idx', queryable.part, queryable.qid),
            queryable.part,
            COALESCE(queryable.property_index_type, 'to_text'),
            queryable.expression
            );
        RAISE NOTICE '%',q;
        EXECUTE q;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION queryables_trigger_func() RETURNS TRIGGER AS $$
DECLARE
BEGIN
PERFORM create_queryable_indexes();
RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER queryables_trigger AFTER INSERT OR UPDATE ON queryables
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();

CREATE TRIGGER queryables_collection_trigger AFTER INSERT OR UPDATE ON collections
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();

CREATE OR REPLACE FUNCTION get_queryables(_collection_ids text[] DEFAULT NULL) RETURNS jsonb AS $$
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
            SELECT
                jsonb_build_object(
                    '$schema', 'http://json-schema.org/draft-07/schema#',
                    '$id', 'https://example.org/queryables',
                    'type', 'object',
                    'title', 'STAC Queryables.',
                    'properties', jsonb_object_agg(
                        name,
                        definition
                    )
                )
                FROM queryables
                WHERE
                    _collection_ids IS NULL OR
                    cardinality(_collection_ids) = 0 OR
                    collection_ids IS NULL OR
                    _collection_ids && collection_ids
        );
    ELSE
        RETURN NULL;
    END IF;
END;

$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables(_collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT
        CASE
            WHEN _collection IS NULL THEN get_queryables(NULL::text[])
            ELSE get_queryables(ARRAY[_collection])
        END
    ;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION missing_queryables(_collection text, _tablesample int DEFAULT 5) RETURNS TABLE(collection text, name text, definition jsonb, property_wrapper text) AS $$
DECLARE
    q text;
    _partition text;
    explain_json json;
    psize bigint;
BEGIN
    SELECT format('_items_%s', key) INTO _partition FROM collections WHERE id=_collection;

    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM %I;', _partition)
    INTO explain_json;
    psize := explain_json->0->'Plan'->'Plan Rows';
    IF _tablesample * .01 * psize < 10 THEN
        _tablesample := 100;
    END IF;
    RAISE NOTICE 'Using tablesample % to find missing queryables from % % that has ~% rows', _tablesample, _collection, _partition, psize;

    q := format(
        $q$
            WITH q AS (
                SELECT * FROM queryables
                WHERE
                    collection_ids IS NULL
                    OR %L = ANY(collection_ids)
            ), t AS (
                SELECT
                    content->'properties' AS properties
                FROM
                    %I
                TABLESAMPLE SYSTEM(%L)
            ), p AS (
                SELECT DISTINCT ON (key)
                    key,
                    value
                FROM t
                JOIN LATERAL jsonb_each(properties) ON TRUE
                LEFT JOIN q ON (q.name=key)
                WHERE q.definition IS NULL
            )
            SELECT
                %L,
                key,
                jsonb_build_object('type',jsonb_typeof(value)) as definition,
                CASE jsonb_typeof(value)
                    WHEN 'number' THEN 'to_float'
                    WHEN 'array' THEN 'to_text_array'
                    ELSE 'to_text'
                END
            FROM p;
        $q$,
        _collection,
        _partition,
        _tablesample,
        _collection
    );
    RETURN QUERY EXECUTE q;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION missing_queryables(_tablesample int DEFAULT 5) RETURNS TABLE(collection_ids text[], name text, definition jsonb, property_wrapper text) AS $$
    SELECT
        array_agg(collection),
        name,
        definition,
        property_wrapper
    FROM
        collections
        JOIN LATERAL
        missing_queryables(id, _tablesample) c
        ON TRUE
    GROUP BY
        2,3,4
    ORDER BY 2,1
    ;
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate jsonb,
    relative_base timestamptz DEFAULT date_trunc('hour', CURRENT_TIMESTAMP)
) RETURNS tstzrange AS $$
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

    IF timestrs[1] = '..' THEN
        s := '-infinity'::timestamptz;
        e := timestrs[2]::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] = '..' THEN
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
$$ LANGUAGE PLPGSQL STABLE STRICT PARALLEL SAFE SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate text,
    relative_base timestamptz DEFAULT CURRENT_TIMESTAMP
) RETURNS tstzrange AS $$
    SELECT parse_dtrange(to_jsonb(_indate), relative_base);
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION temporal_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    ll text := 'datetime';
    lh text := 'end_datetime';
    rrange tstzrange;
    rl text;
    rh text;
    outq text;
BEGIN
    rrange := parse_dtrange(args->1);
    RAISE NOTICE 'Constructing temporal query OP: %, ARGS: %, RRANGE: %', op, args, rrange;
    op := lower(op);
    rl := format('%L::timestamptz', lower(rrange));
    rh := format('%L::timestamptz', upper(rrange));
    outq := CASE op
        WHEN 't_before'       THEN 'lh < rl'
        WHEN 't_after'        THEN 'll > rh'
        WHEN 't_meets'        THEN 'lh = rl'
        WHEN 't_metby'        THEN 'll = rh'
        WHEN 't_overlaps'     THEN 'll < rl AND rl < lh < rh'
        WHEN 't_overlappedby' THEN 'rl < ll < rh AND lh > rh'
        WHEN 't_starts'       THEN 'll = rl AND lh < rh'
        WHEN 't_startedby'    THEN 'll = rl AND lh > rh'
        WHEN 't_during'       THEN 'll > rl AND lh < rh'
        WHEN 't_contains'     THEN 'll < rl AND lh > rh'
        WHEN 't_finishes'     THEN 'll > rl AND lh = rh'
        WHEN 't_finishedby'   THEN 'll < rl AND lh = rh'
        WHEN 't_equals'       THEN 'll = rl AND lh = rh'
        WHEN 't_disjoint'     THEN 'NOT (ll <= rh AND lh >= rl)'
        WHEN 't_intersects'   THEN 'll <= rh AND lh >= rl'
        WHEN 'anyinteracts'   THEN 'll <= rh AND lh >= rl'
    END;
    outq := regexp_replace(outq, '\mll\M', ll);
    outq := regexp_replace(outq, '\mlh\M', lh);
    outq := regexp_replace(outq, '\mrl\M', rl);
    outq := regexp_replace(outq, '\mrh\M', rh);
    outq := format('(%s)', outq);
    RETURN outq;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;



CREATE OR REPLACE FUNCTION spatial_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    geom text;
    j jsonb := args->1;
BEGIN
    op := lower(op);
    RAISE NOTICE 'Constructing spatial query OP: %, ARGS: %', op, args;
    IF op NOT IN ('s_equals','s_disjoint','s_touches','s_within','s_overlaps','s_crosses','s_intersects','intersects','s_contains') THEN
        RAISE EXCEPTION 'Spatial Operator % Not Supported', op;
    END IF;
    op := regexp_replace(op, '^s_', 'st_');
    IF op = 'intersects' THEN
        op := 'st_intersects';
    END IF;
    -- Convert geometry to WKB string
    IF j ? 'type' AND j ? 'coordinates' THEN
        geom := st_geomfromgeojson(j)::text;
    ELSIF jsonb_typeof(j) = 'array' THEN
        geom := bbox_geom(j)::text;
    END IF;

    RETURN format('%s(geometry, %L::geometry)', op, geom);
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION query_to_cql2(q jsonb) RETURNS jsonb AS $$
-- Translates anything passed in through the deprecated "query" into equivalent CQL2
WITH t AS (
    SELECT key as property, value as ops
        FROM jsonb_each(q)
), t2 AS (
    SELECT property, (jsonb_each(ops)).*
        FROM t WHERE jsonb_typeof(ops) = 'object'
    UNION ALL
    SELECT property, 'eq', ops
        FROM t WHERE jsonb_typeof(ops) != 'object'
)
SELECT
    jsonb_strip_nulls(jsonb_build_object(
        'op', 'and',
        'args', jsonb_agg(
            jsonb_build_object(
                'op', key,
                'args', jsonb_build_array(
                    jsonb_build_object('property',property),
                    value
                )
            )
        )
    )
) as qcql FROM t2
;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION cql1_to_cql2(j jsonb) RETURNS jsonb AS $$
DECLARE
    args jsonb;
    ret jsonb;
BEGIN
    RAISE NOTICE 'CQL1_TO_CQL2: %', j;
    IF j ? 'filter' THEN
        RETURN cql1_to_cql2(j->'filter');
    END IF;
    IF j ? 'property' THEN
        RETURN j;
    END IF;
    IF jsonb_typeof(j) = 'array' THEN
        SELECT jsonb_agg(cql1_to_cql2(el)) INTO args FROM jsonb_array_elements(j) el;
        RETURN args;
    END IF;
    IF jsonb_typeof(j) = 'number' THEN
        RETURN j;
    END IF;
    IF jsonb_typeof(j) = 'string' THEN
        RETURN j;
    END IF;

    IF jsonb_typeof(j) = 'object' THEN
        SELECT jsonb_build_object(
                'op', key,
                'args', cql1_to_cql2(value)
            ) INTO ret
        FROM jsonb_each(j)
        WHERE j IS NOT NULL;
        RETURN ret;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT;

CREATE TABLE cql2_ops (
    op text PRIMARY KEY,
    template text,
    types text[]
);
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
    ('lower', 'lower(%s)', NULL)
ON CONFLICT (op) DO UPDATE
    SET
        template = EXCLUDED.template
;


CREATE OR REPLACE FUNCTION cql2_query(j jsonb, wrapper text DEFAULT NULL) RETURNS text AS $$
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
BEGIN
    IF j IS NULL OR (op IS NOT NULL AND args IS NULL) THEN
        RETURN NULL;
    END IF;
    RAISE NOTICE 'CQL2_QUERY: %', j;
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
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION paging_dtrange(
    j jsonb
) RETURNS tstzrange AS $$
DECLARE
    op text;
    filter jsonb := j->'filter';
    dtrange tstzrange := tstzrange('-infinity'::timestamptz,'infinity'::timestamptz);
    sdate timestamptz := '-infinity'::timestamptz;
    edate timestamptz := 'infinity'::timestamptz;
    jpitem jsonb;
BEGIN

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        sdate := lower(dtrange);
        edate := upper(dtrange);
    END IF;
    IF NOT (filter  @? '$.**.op ? (@ == "or" || @ == "not")') THEN
        FOR jpitem IN SELECT j FROM jsonb_path_query(filter,'strict $.** ? (@.args[*].property == "datetime")'::jsonpath) j LOOP
            op := lower(jpitem->>'op');
            dtrange := parse_dtrange(jpitem->'args'->1);
            IF op IN ('<=', 'lt', 'lte', '<', 'le', 't_before') THEN
                sdate := greatest(sdate,'-infinity');
                edate := least(edate, upper(dtrange));
            ELSIF op IN ('>=', '>', 'gt', 'gte', 'ge', 't_after') THEN
                edate := least(edate, 'infinity');
                sdate := greatest(sdate, lower(dtrange));
            ELSIF op IN ('=', 'eq') THEN
                edate := least(edate, upper(dtrange));
                sdate := greatest(sdate, lower(dtrange));
            END IF;
            RAISE NOTICE '2 OP: %, ARGS: %, DTRANGE: %, SDATE: %, EDATE: %', op, jpitem->'args'->1, dtrange, sdate, edate;
        END LOOP;
    END IF;
    IF sdate > edate THEN
        RETURN 'empty'::tstzrange;
    END IF;
    RETURN tstzrange(sdate,edate, '[]');
END;
$$ LANGUAGE PLPGSQL STABLE STRICT SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION paging_collections(
    IN j jsonb
) RETURNS text[] AS $$
DECLARE
    filter jsonb := j->'filter';
    jpitem jsonb;
    op text;
    args jsonb;
    arg jsonb;
    collections text[];
BEGIN
    IF j ? 'collections' THEN
        collections := to_text_array(j->'collections');
    END IF;
    IF NOT (filter  @? '$.**.op ? (@ == "or" || @ == "not")') THEN
        FOR jpitem IN SELECT j FROM jsonb_path_query(filter,'strict $.** ? (@.args[*].property == "collection")'::jsonpath) j LOOP
            RAISE NOTICE 'JPITEM: %', jpitem;
            op := jpitem->>'op';
            args := jpitem->'args';
            IF op IN ('=', 'eq', 'in') THEN
                FOR arg IN SELECT a FROM jsonb_array_elements(args) a LOOP
                    IF jsonb_typeof(arg) IN ('string', 'array') THEN
                        RAISE NOTICE 'arg: %, collections: %', arg, collections;
                        IF collections IS NULL OR collections = '{}'::text[] THEN
                            collections := to_text_array(arg);
                        ELSE
                            collections := array_intersection(collections, to_text_array(arg));
                        END IF;
                    END IF;
                END LOOP;
            END IF;
        END LOOP;
    END IF;
    IF collections = '{}'::text[] THEN
        RETURN NULL;
    END IF;
    RETURN collections;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;
CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    content JSONB NOT NULL
)
PARTITION BY LIST (collection)
;

CREATE INDEX "datetime_idx" ON items USING BTREE (datetime DESC, end_datetime ASC);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;


ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;


CREATE OR REPLACE FUNCTION content_slim(_item jsonb) RETURNS jsonb AS $$
    SELECT strip_jsonb(_item - '{id,geometry,collection,type}'::text[], collection_base_item(_item->>'collection')) - '{id,geometry,collection,type}'::text[];
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
    SELECT
            content->>'id' as id,
            stac_geom(content) as geometry,
            content->>'collection' as collection,
            stac_datetime(content) as datetime,
            stac_end_datetime(content) as end_datetime,
            content_slim(content) as content
    ;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION include_field(f text, fields jsonb DEFAULT '{}'::jsonb) RETURNS boolean AS $$
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
$$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS content_hydrate(jsonb, jsonb, jsonb);
CREATE OR REPLACE FUNCTION content_hydrate(
    _item jsonb,
    _base_item jsonb,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
    SELECT merge_jsonb(
            jsonb_fields(_item, fields),
            jsonb_fields(_base_item, fields)
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION content_hydrate(_item items, _collection collections, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
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
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
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
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_hydrate(_item items, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
    SELECT content_hydrate(
        _item,
        (SELECT c FROM collections c WHERE id=_item.collection LIMIT 1),
        fields
    );
$$ LANGUAGE SQL STABLE;


CREATE UNLOGGED TABLE items_staging (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_ignore (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_upsert (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_triggerfunc() RETURNS TRIGGER AS $$
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
            JOIN deletes d
            USING (id, collection);
        DELETE FROM items_staging_upsert;
    END IF;
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER items_staging_insert_trigger AFTER INSERT ON items_staging REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_ignore_trigger AFTER INSERT ON items_staging_ignore REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_upsert_trigger AFTER INSERT ON items_staging_upsert REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();




CREATE OR REPLACE FUNCTION item_by_id(_id text, _collection text DEFAULT NULL) RETURNS items AS
$$
DECLARE
    i items%ROWTYPE;
BEGIN
    SELECT * INTO i FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection) LIMIT 1;
    RETURN i;
END;
$$ LANGUAGE PLPGSQL STABLE SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION get_item(_id text, _collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT content_hydrate(items) FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection);
$$ LANGUAGE SQL STABLE SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION delete_item(_id text, _collection text DEFAULT NULL) RETURNS VOID AS $$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id AND (_collection IS NULL OR collection=_collection) RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL;

--/*
CREATE OR REPLACE FUNCTION create_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION update_item(content jsonb) RETURNS VOID AS $$
DECLARE
    old items %ROWTYPE;
    out items%ROWTYPE;
BEGIN
    PERFORM delete_item(content->>'id', content->>'collection');
    PERFORM create_item(content);
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION collection_bbox(id text) RETURNS jsonb AS $$
    SELECT (replace(replace(replace(st_extent(geometry)::text,'BOX(','[['),')',']]'),' ',','))::jsonb
    FROM items WHERE collection=$1;
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_temporal_extent(id text) RETURNS jsonb AS $$
    SELECT to_jsonb(array[array[min(datetime)::text, max(datetime)::text]])
    FROM items WHERE collection=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections SET
    content = content ||
    jsonb_build_object(
        'extent', jsonb_build_object(
            'spatial', jsonb_build_object(
                'bbox', collection_bbox(collections.id)
            ),
            'temporal', jsonb_build_object(
                'interval', collection_temporal_extent(collections.id)
            )
        )
    )
;
$$ LANGUAGE SQL;
CREATE VIEW partition_steps AS
SELECT
    name,
    date_trunc('month',lower(datetime_range)) as sdate,
    date_trunc('month', upper(datetime_range)) + '1 month'::interval as edate
    FROM partitions WHERE datetime_range IS NOT NULL AND datetime_range != 'empty'::tstzrange
    ORDER BY datetime_range ASC
;

CREATE OR REPLACE FUNCTION chunker(
    IN _where text,
    OUT s timestamptz,
    OUT e timestamptz
) RETURNS SETOF RECORD AS $$
DECLARE
    explain jsonb;
BEGIN
    IF _where IS NULL THEN
        _where := ' TRUE ';
    END IF;
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s;', _where)
    INTO explain;

    RETURN QUERY
    WITH t AS (
        SELECT j->>0 as p FROM
            jsonb_path_query(
                explain,
                'strict $.**."Relation Name" ? (@ != null)'
            ) j
    ),
    parts AS (
        SELECT sdate, edate FROM t JOIN partition_steps ON (t.p = name)
    ),
    times AS (
        SELECT sdate FROM parts
        UNION
        SELECT edate FROM parts
    ),
    uniq AS (
        SELECT DISTINCT sdate FROM times ORDER BY sdate
    ),
    last AS (
    SELECT sdate, lead(sdate, 1) over () as edate FROM uniq
    )
    SELECT sdate, edate FROM last WHERE edate IS NOT NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN partitions text[] DEFAULT NULL
) RETURNS SETOF text AS $$
DECLARE
    query text;
    sdate timestamptz;
    edate timestamptz;
BEGIN
IF _where IS NULL OR trim(_where) = '' THEN
    _where = ' TRUE ';
END IF;
RAISE NOTICE 'Getting chunks for % %', _where, _orderby;
IF _orderby ILIKE 'datetime d%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 DESC LOOP
        RETURN NEXT format($q$
            SELECT * FROM items
            WHERE
            datetime >= %L AND datetime < %L
            AND (%s)
            ORDER BY %s
            $q$,
            sdate,
            edate,
            _where,
            _orderby
        );
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RETURN NEXT format($q$
            SELECT * FROM items
            WHERE
            datetime >= %L AND datetime < %L
            AND (%s)
            ORDER BY %s
            $q$,
            sdate,
            edate,
            _where,
            _orderby
        );
    END LOOP;
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
    $q$, _where, _orderby
    );

    RETURN NEXT query;
    RETURN;
END IF;

RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION partition_query_view(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 10
) RETURNS text AS $$
    WITH p AS (
        SELECT * FROM partition_queries(_where, _orderby) p
    )
    SELECT
        CASE WHEN EXISTS (SELECT 1 FROM p) THEN
            (SELECT format($q$
                SELECT * FROM (
                    %s
                ) total LIMIT %s
                $q$,
                string_agg(
                    format($q$ SELECT * FROM ( %s ) AS sub $q$, p),
                    '
                    UNION ALL
                    '
                ),
                _limit
            ))
        ELSE NULL
        END FROM p;
$$ LANGUAGE SQL IMMUTABLE;




CREATE OR REPLACE FUNCTION stac_search_to_where(j jsonb) RETURNS text AS $$
DECLARE
    where_segments text[];
    _where text;
    dtrange tstzrange;
    collections text[];
    geom geometry;
    sdate timestamptz;
    edate timestamptz;
    filterlang text;
    filter jsonb := j->'filter';
BEGIN
    IF j ? 'ids' THEN
        where_segments := where_segments || format('id = ANY (%L) ', to_text_array(j->'ids'));
    END IF;

    IF j ? 'collections' THEN
        collections := to_text_array(j->'collections');
        where_segments := where_segments || format('collection = ANY (%L) ', collections);
    END IF;

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        sdate := lower(dtrange);
        edate := upper(dtrange);

        where_segments := where_segments || format(' datetime <= %L::timestamptz AND end_datetime >= %L::timestamptz ',
            edate,
            sdate
        );
    END IF;

    geom := stac_geom(j);
    IF geom IS NOT NULL THEN
        where_segments := where_segments || format('st_intersects(geometry, %L)',geom);
    END IF;

    filterlang := COALESCE(
        j->>'filter-lang',
        get_setting('default-filter-lang', j->'conf')
    );
    IF NOT filter @? '$.**.op' THEN
        filterlang := 'cql-json';
    END IF;

    IF filterlang NOT IN ('cql-json','cql2-json') AND j ? 'filter' THEN
        RAISE EXCEPTION '% is not a supported filter-lang. Please use cql-json or cql2-json.', filterlang;
    END IF;

    IF j ? 'query' AND j ? 'filter' THEN
        RAISE EXCEPTION 'Can only use either query or filter at one time.';
    END IF;

    IF j ? 'query' THEN
        filter := query_to_cql2(j->'query');
    ELSIF filterlang = 'cql-json' THEN
        filter := cql1_to_cql2(filter);
    END IF;
    RAISE NOTICE 'FILTER: %', filter;
    where_segments := where_segments || cql2_query(filter);
    IF cardinality(where_segments) < 1 THEN
        RETURN ' TRUE ';
    END IF;

    _where := array_to_string(array_remove(where_segments, NULL), ' AND ');

    IF _where IS NULL OR BTRIM(_where) = '' THEN
        RETURN ' TRUE ';
    END IF;
    RETURN _where;

END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION parse_sort_dir(_dir text, reverse boolean default false) RETURNS text AS $$
    WITH t AS (
        SELECT COALESCE(upper(_dir), 'ASC') as d
    ) SELECT
        CASE
            WHEN NOT reverse THEN d
            WHEN d = 'ASC' THEN 'DESC'
            WHEN d = 'DESC' THEN 'ASC'
        END
    FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sort_dir_to_op(_dir text, prev boolean default false) RETURNS text AS $$
    WITH t AS (
        SELECT COALESCE(upper(_dir), 'ASC') as d
    ) SELECT
        CASE
            WHEN d = 'ASC' AND prev THEN '<='
            WHEN d = 'DESC' AND prev THEN '>='
            WHEN d = 'ASC' THEN '>='
            WHEN d = 'DESC' THEN '<='
        END
    FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE
) RETURNS text AS $$
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
                -- field_orderby((items_path(value->>'field')).path_txt),
                (queryable(value->>'field')).expression
            ) as key,
            parse_sort_dir(value->>'direction', reverse) as dir
        FROM withid_rows
    )
    SELECT array_to_string(
        array_agg(concat(key, ' ', dir)),
        ', '
    ) FROM sorts;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_sort_dir(sort_item jsonb) RETURNS text AS $$
    SELECT CASE WHEN sort_item->>'direction' ILIKE 'desc%' THEN 'DESC' ELSE 'ASC' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION get_token_filter(_search jsonb = '{}'::jsonb, token_rec jsonb DEFAULT NULL) RETURNS text AS $$
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
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search_tohash(jsonb) RETURNS jsonb AS $$
    SELECT $1 - '{token,limit,context,includes,excludes}'::text[];
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search_hash(jsonb, jsonb) RETURNS text AS $$
    SELECT md5(concat(search_tohash($1)::text,$2::text));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TABLE IF NOT EXISTS searches(
    hash text GENERATED ALWAYS AS (search_hash(search, metadata)) STORED PRIMARY KEY,
    search jsonb NOT NULL,
    _where text,
    orderby text,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS search_wheres(
    id bigint generated always as identity primary key,
    _where text NOT NULL,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    statslastupdated timestamptz,
    estimated_count bigint,
    estimated_cost float,
    time_to_estimate float,
    total_count bigint,
    time_to_count float,
    partitions text[]
);

CREATE INDEX IF NOT EXISTS search_wheres_partitions ON search_wheres USING GIN (partitions);
CREATE UNIQUE INDEX IF NOT EXISTS search_wheres_where ON search_wheres ((md5(_where)));

CREATE OR REPLACE FUNCTION where_stats(inwhere text, updatestats boolean default false, conf jsonb default null) RETURNS search_wheres AS $$
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
$$ LANGUAGE PLPGSQL ;



DROP FUNCTION IF EXISTS search_query;
CREATE OR REPLACE FUNCTION search_query(
    _search jsonb = '{}'::jsonb,
    updatestats boolean = false,
    _metadata jsonb = '{}'::jsonb
) RETURNS searches AS $$
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
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS jsonb AS $$
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
END IF;

SELECT jsonb_agg(content) INTO out_records FROM results WHERE content is not NULL;

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
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public SET cursor_tuple_fraction TO 1;


CREATE OR REPLACE FUNCTION search_cursor(_search jsonb = '{}'::jsonb) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
    searches searches%ROWTYPE;
    _where text;
    _orderby text;
    q text;

BEGIN
    searches := search_query(_search);
    _where := searches._where;
    _orderby := searches.orderby;

    OPEN curs FOR
        WITH p AS (
            SELECT * FROM partition_queries(_where, _orderby) p
        )
        SELECT
            CASE WHEN EXISTS (SELECT 1 FROM p) THEN
                (SELECT format($q$
                    SELECT * FROM (
                        %s
                    ) total
                    $q$,
                    string_agg(
                        format($q$ SELECT * FROM ( %s ) AS sub $q$, p),
                        '
                        UNION ALL
                        '
                    )
                ))
            ELSE NULL
            END FROM p;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;
SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION tileenvelope(zoom int, x int, y int) RETURNS geometry AS $$
WITH t AS (
    SELECT
        20037508.3427892 as merc_max,
        -20037508.3427892 as merc_min,
        (2 * 20037508.3427892) / (2 ^ zoom) as tile_size
)
SELECT st_makeenvelope(
    merc_min + (tile_size * x),
    merc_max - (tile_size * (y + 1)),
    merc_min + (tile_size * (x + 1)),
    merc_max - (tile_size * y),
    3857
) FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;DROP FUNCTION IF EXISTS mercgrid;


CREATE OR REPLACE FUNCTION ftime() RETURNS interval as $$
SELECT age(clock_timestamp(), transaction_timestamp());
$$ LANGUAGE SQL;
SET SEARCH_PATH to pgstac, public;

DROP FUNCTION IF EXISTS geometrysearch;
CREATE OR REPLACE FUNCTION geometrysearch(
    IN geom geometry,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE, -- Return as soon as the passed in geometry is full covered
    IN skipcovered boolean DEFAULT TRUE -- Skip any items that would show up completely under the previous items
) RETURNS jsonb AS $$
DECLARE
    search searches%ROWTYPE;
    curs refcursor;
    _where text;
    query text;
    iter_record items%ROWTYPE;
    out_records jsonb := '{}'::jsonb[];
    exit_flag boolean := FALSE;
    counter int := 1;
    scancounter int := 1;
    remaining_limit int := _scanlimit;
    tilearea float;
    unionedgeom geometry;
    clippedgeom geometry;
    unionedgeom_area float := 0;
    prev_area float := 0;
    excludes text[];
    includes text[];

BEGIN
    DROP TABLE IF EXISTS pgstac_results;
    CREATE TEMP TABLE pgstac_results (content jsonb) ON COMMIT DROP;

    -- If skipcovered is true then you will always want to exit when the passed in geometry is full
    IF skipcovered THEN
        exitwhenfull := TRUE;
    END IF;

    SELECT * INTO search FROM searches WHERE hash=queryhash;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Search with Query Hash % Not Found', queryhash;
    END IF;

    tilearea := st_area(geom);
    _where := format('%s AND st_intersects(geometry, %L::geometry)', search._where, geom);


    FOR query IN SELECT * FROM partition_queries(_where, search.orderby) LOOP
        query := format('%s LIMIT %L', query, remaining_limit);
        RAISE NOTICE '%', query;
        OPEN curs FOR EXECUTE query;
        LOOP
            FETCH curs INTO iter_record;
            EXIT WHEN NOT FOUND;
            IF exitwhenfull OR skipcovered THEN -- If we are not using exitwhenfull or skipcovered, we do not need to do expensive geometry operations
                clippedgeom := st_intersection(geom, iter_record.geometry);

                IF unionedgeom IS NULL THEN
                    unionedgeom := clippedgeom;
                ELSE
                    unionedgeom := st_union(unionedgeom, clippedgeom);
                END IF;

                unionedgeom_area := st_area(unionedgeom);

                IF skipcovered AND prev_area = unionedgeom_area THEN
                    scancounter := scancounter + 1;
                    CONTINUE;
                END IF;

                prev_area := unionedgeom_area;

                RAISE NOTICE '% % % %', unionedgeom_area/tilearea, counter, scancounter, ftime();
            END IF;
            RAISE NOTICE '% %', iter_record, content_hydrate(iter_record, fields);
            INSERT INTO pgstac_results (content) VALUES (content_hydrate(iter_record, fields));

            IF counter >= _limit
                OR scancounter > _scanlimit
                OR ftime() > _timelimit
                OR (exitwhenfull AND unionedgeom_area >= tilearea)
            THEN
                exit_flag := TRUE;
                EXIT;
            END IF;
            counter := counter + 1;
            scancounter := scancounter + 1;

        END LOOP;
        CLOSE curs;
        EXIT WHEN exit_flag;
        remaining_limit := _scanlimit - scancounter;
    END LOOP;

    SELECT jsonb_agg(content) INTO out_records FROM pgstac_results WHERE content IS NOT NULL;

    RETURN jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb)
    );
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS geojsonsearch;
CREATE OR REPLACE FUNCTION geojsonsearch(
    IN geojson jsonb,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
    SELECT * FROM geometrysearch(
        st_geomfromgeojson(geojson),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        exitwhenfull,
        skipcovered
    );
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS xyzsearch;
CREATE OR REPLACE FUNCTION xyzsearch(
    IN _x int,
    IN _y int,
    IN _z int,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
    SELECT * FROM geometrysearch(
        st_transform(tileenvelope(_z, _x, _y), 4326),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        exitwhenfull,
        skipcovered
    );
$$ LANGUAGE SQL;
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
SELECT set_version('0.6.10');
