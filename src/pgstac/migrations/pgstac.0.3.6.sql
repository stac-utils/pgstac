CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS pgstac;
SET SEARCH_PATH TO pgstac, public;

CREATE TABLE migrations (
  version text PRIMARY KEY,
  datetime timestamptz DEFAULT clock_timestamp() NOT NULL
);

CREATE OR REPLACE FUNCTION get_version() RETURNS text AS $$
  SELECT version FROM migrations ORDER BY datetime DESC, version DESC LIMIT 1;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION set_version(text) RETURNS text AS $$
  INSERT INTO migrations (version) VALUES ($1)
  ON CONFLICT DO NOTHING
  RETURNING version;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;


CREATE OR REPLACE FUNCTION get_setting(IN setting text, INOUT _default anynonarray = null::text ) AS $$
DECLARE
_type text;
BEGIN
  SELECT pg_typeof(_default) INTO _type;
  IF _type = 'unknown' THEN _type='text'; END IF;
  EXECUTE format($q$
    SELECT COALESCE(
      CAST(current_setting($1,TRUE) AS %s),
      $2
    )
    $q$, _type)
    INTO _default
    USING setting, _default
  ;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION context() RETURNS text AS $$
  SELECT get_setting('pgstac.context','off'::text);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context_estimated_count() RETURNS int AS $$
  SELECT get_setting('pgstac.context_estimated_count', 100000::int);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context_estimated_cost() RETURNS float AS $$
  SELECT get_setting('pgstac.context_estimated_cost', 1000000::float);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION context_stats_ttl() RETURNS interval AS $$
  SELECT get_setting('pgstac.context_stats_ttl', '1 day'::interval);
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

CREATE OR REPLACE FUNCTION array_map_ident(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_ident(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION array_map_literal(_a text[])
  RETURNS text[] AS $$
  SELECT array_agg(quote_literal(v)) FROM unnest(_a) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION estimated_count(_where text) RETURNS bigint AS $$
DECLARE
rec record;
rows bigint;
BEGIN
    FOR rec in EXECUTE format(
        $q$
            EXPLAIN SELECT 1 FROM items WHERE %s
        $q$,
        _where)
    LOOP
        rows := substring(rec."QUERY PLAN" FROM ' rows=([[:digit:]]+)');
        EXIT WHEN rows IS NOT NULL;
    END LOOP;

    RETURN rows;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE 'sql' STRICT IMMUTABLE;
/* converts a jsonb text array to a pg text[] array */
CREATE OR REPLACE FUNCTION textarr(_js jsonb)
  RETURNS text[] AS $$
  SELECT
    CASE jsonb_typeof(_js)
        WHEN 'array' THEN ARRAY(SELECT jsonb_array_elements_text(_js))
        ELSE ARRAY[_js->>0]
    END
;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_paths (IN jdata jsonb, OUT path text[], OUT value jsonb) RETURNS
SETOF RECORD AS $$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        value
    FROM jsonb_each(jdata)
union all
    select
        path || coalesce(obj_key, (arr_key- 1)::text),
        coalesce(obj_value, arr_value)
    from extract_all
    left join lateral
        jsonb_each(case jsonb_typeof(value) when 'object' then value end)
        as o(obj_key, obj_value)
        on jsonb_typeof(value) = 'object'
    left join lateral
        jsonb_array_elements(case jsonb_typeof(value) when 'array' then value end)
        with ordinality as a(arr_value, arr_key)
        on jsonb_typeof(value) = 'array'
    where obj_key is not null or arr_key is not null
)
select *
from extract_all;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION jsonb_obj_paths (IN jdata jsonb, OUT path text[], OUT value jsonb) RETURNS
SETOF RECORD AS $$
with recursive extract_all as
(
    select
        ARRAY[key]::text[] as path,
        value
    FROM jsonb_each(jdata)
union all
    select
        path || obj_key,
        obj_value
    from extract_all
    left join lateral
        jsonb_each(case jsonb_typeof(value) when 'object' then value end)
        as o(obj_key, obj_value)
        on jsonb_typeof(value) = 'object'
    where obj_key is not null
)
select *
from extract_all;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION jsonb_val_paths (IN jdata jsonb, OUT path text[], OUT value jsonb) RETURNS
SETOF RECORD AS $$
SELECT * FROM jsonb_obj_paths(jdata) WHERE jsonb_typeof(value) not in  ('object','array');
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION path_includes(IN path text[], IN includes text[]) RETURNS BOOLEAN AS $$
WITH t AS (SELECT unnest(includes) i)
SELECT EXISTS (
    SELECT 1 FROM t WHERE path @> string_to_array(trim(i), '.')
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION path_excludes(IN path text[], IN excludes text[]) RETURNS BOOLEAN AS $$
WITH t AS (SELECT unnest(excludes) e)
SELECT NOT EXISTS (
    SELECT 1 FROM t WHERE path @> string_to_array(trim(e), '.')
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_obj_paths_filtered (
    IN jdata jsonb,
    IN includes text[] DEFAULT ARRAY[]::text[],
    IN excludes text[] DEFAULT ARRAY[]::text[],
    OUT path text[],
    OUT value jsonb
) RETURNS
SETOF RECORD AS $$
SELECT path, value
FROM jsonb_obj_paths(jdata)
WHERE
    CASE WHEN cardinality(includes) > 0 THEN path_includes(path, includes) ELSE TRUE END
    AND
    path_excludes(path, excludes)

;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION filter_jsonb(
    IN jdata jsonb,
    IN includes text[] DEFAULT ARRAY[]::text[],
    IN excludes text[] DEFAULT ARRAY[]::text[]
) RETURNS jsonb AS $$
DECLARE
rec RECORD;
outj jsonb := '{}'::jsonb;
created_paths text[] := '{}'::text[];
BEGIN

IF empty_arr(includes) AND empty_arr(excludes) THEN
RAISE NOTICE 'no filter';
  RETURN jdata;
END IF;
FOR rec in
SELECT * FROM jsonb_obj_paths_filtered(jdata, includes, excludes)
WHERE jsonb_typeof(value) != 'object'
LOOP
    IF array_length(rec.path,1)>1 THEN
        FOR i IN 1..(array_length(rec.path,1)-1) LOOP
          IF NOT array_to_string(rec.path[1:i],'.') = ANY (created_paths) THEN
            outj := jsonb_set(outj, rec.path[1:i],'{}', true);
            created_paths := created_paths || array_to_string(rec.path[1:i],'.');
          END IF;
        END LOOP;
    END IF;
    outj := jsonb_set(outj, rec.path, rec.value, true);
    created_paths := created_paths || array_to_string(rec.path,'.');
END LOOP;
RETURN outj;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION flip_jsonb_array(j jsonb) RETURNS jsonb AS $$
SELECT jsonb_agg(value) FROM (SELECT value FROM jsonb_array_elements(j) WITH ORDINALITY ORDER BY ordinality DESC) as t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
/* Functions to create an iterable of cursors over partitions. */
CREATE OR REPLACE FUNCTION create_cursor(q text) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR EXECUTE q;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS partition_queries;
CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN partitions text[] DEFAULT '{items}'
) RETURNS SETOF text AS $$
DECLARE
    partition_query text;
    query text;
    p text;
    cursors refcursor;
    dstart timestamptz;
    dend timestamptz;
    step interval := '10 weeks'::interval;
BEGIN

IF _orderby ILIKE 'datetime d%' THEN
    partitions := partitions;
ELSIF _orderby ILIKE 'datetime a%' THEN
    partitions := array_reverse(partitions);
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
RAISE NOTICE 'PARTITIONS ---> %',partitions;
IF cardinality(partitions) > 0 THEN
    FOREACH p IN ARRAY partitions
        --EXECUTE partition_query
    LOOP
        query := format($q$
            SELECT * FROM %I
            WHERE %s
            ORDER BY %s
            $q$,
            p,
            _where,
            _orderby
        );
        RETURN NEXT query;
    END LOOP;
END IF;
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION partition_cursor(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC'
) RETURNS SETOF refcursor AS $$
DECLARE
    partition_query text;
    query text;
    p record;
    cursors refcursor;
BEGIN
FOR query IN SELECT * FROM partition_queries(_where, _orderby) LOOP
    RETURN NEXT create_cursor(query);
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION partition_count(
    IN _where text DEFAULT 'TRUE'
) RETURNS bigint AS $$
DECLARE
    partition_query text;
    query text;
    p record;
    subtotal bigint;
    total bigint := 0;
BEGIN
partition_query := format($q$
    SELECT partition, tstzrange
    FROM items_partitions
    ORDER BY tstzrange DESC;
$q$);
RAISE NOTICE 'Partition Query: %', partition_query;
FOR p IN
    EXECUTE partition_query
LOOP
    query := format($q$
        SELECT count(*) FROM items
        WHERE datetime BETWEEN %L AND %L AND %s
    $q$, lower(p.tstzrange), upper(p.tstzrange), _where
    );
    RAISE NOTICE 'Query %', query;
    RAISE NOTICE 'Partition %, Count %, Total %',p.partition, subtotal, total;
    EXECUTE query INTO subtotal;
    total := subtotal + total;
END LOOP;
RETURN total;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION drop_partition_constraints(IN partition text) RETURNS VOID AS $$
DECLARE
    q text;
    end_datetime_constraint text := concat(partition, '_end_datetime_constraint');
    collections_constraint text := concat(partition, '_collections_constraint');
BEGIN
    q := format($q$
            ALTER TABLE %I
                DROP CONSTRAINT IF EXISTS %I,
                DROP CONSTRAINT IF EXISTS %I;
        $q$,
        partition,
        end_datetime_constraint,
        collections_constraint
    );

    EXECUTE q;
    RETURN;

END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS partition_checks;
CREATE OR REPLACE FUNCTION partition_checks(
    IN partition text,
    OUT min_datetime timestamptz,
    OUT max_datetime timestamptz,
    OUT min_end_datetime timestamptz,
    OUT max_end_datetime timestamptz,
    OUT collections text[],
    OUT cnt bigint
) RETURNS RECORD AS $$
DECLARE
q text;
end_datetime_constraint text := concat(partition, '_end_datetime_constraint');
collections_constraint text := concat(partition, '_collections_constraint');
BEGIN
RAISE NOTICE 'CREATING CONSTRAINTS FOR %', partition;
q := format($q$
        SELECT
            min(datetime),
            max(datetime),
            min(end_datetime),
            max(end_datetime),
            array_agg(DISTINCT collection_id),
            count(*)
        FROM %I;
    $q$,
    partition
);
EXECUTE q INTO min_datetime, max_datetime, min_end_datetime, max_end_datetime, collections, cnt;
RAISE NOTICE '% % % % % % %', min_datetime, max_datetime, min_end_datetime, max_end_datetime, collections, cnt, ftime();
IF cnt IS NULL or cnt = 0 THEN
    RAISE NOTICE 'Partition % is empty, removing...', partition;
    q := format($q$
        DROP TABLE IF EXISTS %I;
        $q$, partition
    );
    EXECUTE q;
    RETURN;
END IF;
RAISE NOTICE 'Running Constraint DDL %', ftime();
q := format($q$
        ALTER TABLE %I
        DROP CONSTRAINT IF EXISTS %I,
        ADD CONSTRAINT %I
            check((end_datetime >= %L) AND (end_datetime <= %L)) NOT VALID,
        DROP CONSTRAINT IF EXISTS %I,
        ADD CONSTRAINT %I
            check((collection_id = ANY(%L))) NOT VALID;
    $q$,
    partition,
    end_datetime_constraint,
    end_datetime_constraint,
    min_end_datetime,
    max_end_datetime,
    collections_constraint,
    collections_constraint,
    collections,
    partition
);
RAISE NOTICE 'q: %', q;

EXECUTE q;
RAISE NOTICE 'Returning %', ftime();
RETURN;

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION validate_constraints() RETURNS VOID AS $$
DECLARE
q text;
BEGIN
FOR q IN
    SELECT FORMAT(
        'ALTER TABLE %I.%I.%I VALIDATE CONSTRAINT %I;',
        current_database(),
        nsp.nspname,
        cls.relname,
        con.conname
    )
    FROM pg_constraint AS con
    JOIN pg_class AS cls
    ON con.conrelid = cls.oid
    JOIN pg_namespace AS nsp
    ON cls.relnamespace = nsp.oid
    WHERE convalidated IS FALSE
    AND nsp.nspname = 'pgstac'
LOOP
    EXECUTE q;
END LOOP;
END;
$$ LANGUAGE PLPGSQL;
/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
    CASE
            WHEN value->>'geometry' IS NOT NULL THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value->>'bbox' IS NOT NULL THEN
                ST_MakeEnvelope(
                    (value->'bbox'->>0)::float,
                    (value->'bbox'->>1)::float,
                    (value->'bbox'->>2)::float,
                    (value->'bbox'->>3)::float,
                    4326
                )
            ELSE NULL
        END as geometry
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION stac_datetime(value jsonb) RETURNS timestamptz AS $$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'start_datetime')::timestamptz
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_end_datetime(value jsonb) RETURNS timestamptz AS $$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'end_datetime')::timestamptz
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';


CREATE OR REPLACE FUNCTION stac_daterange(value jsonb) RETURNS tstzrange AS $$
SELECT tstzrange(stac_datetime(value),stac_end_datetime(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';
SET SEARCH_PATH TO pgstac, public;

CREATE TABLE IF NOT EXISTS collections (
    id VARCHAR GENERATED ALWAYS AS (content->>'id') STORED PRIMARY KEY,
    content JSONB
);

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
SET SEARCH_PATH TO pgstac, public;

CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection_id text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    properties jsonb NOT NULL,
    content JSONB NOT NULL
)
PARTITION BY RANGE (datetime)
;

CREATE OR REPLACE FUNCTION properties_idx (IN content jsonb) RETURNS jsonb AS $$
    with recursive extract_all as
    (
        select
            ARRAY[key]::text[] as path,
            ARRAY[key]::text[] as fullpath,
            value
        FROM jsonb_each(content->'properties')
    union all
        select
            CASE WHEN obj_key IS NOT NULL THEN path || obj_key ELSE path END,
            path || coalesce(obj_key, (arr_key- 1)::text),
            coalesce(obj_value, arr_value)
        from extract_all
        left join lateral
            jsonb_each(case jsonb_typeof(value) when 'object' then value end)
            as o(obj_key, obj_value)
            on jsonb_typeof(value) = 'object'
        left join lateral
            jsonb_array_elements(case jsonb_typeof(value) when 'array' then value end)
            with ordinality as a(arr_value, arr_key)
            on jsonb_typeof(value) = 'array'
        where obj_key is not null or arr_key is not null
    )
    , paths AS (
    select
        array_to_string(path, '.') as path,
        value
    FROM extract_all
    WHERE
        jsonb_typeof(value) NOT IN ('array','object')
    ), grouped AS (
    SELECT path, jsonb_agg(distinct value) vals FROM paths group by path
    ) SELECT coalesce(jsonb_object_agg(path, CASE WHEN jsonb_array_length(vals)=1 THEN vals->0 ELSE vals END) - '{datetime}'::text[], '{}'::jsonb) FROM grouped
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET JIT TO OFF;

CREATE INDEX "datetime_idx" ON items (datetime);
CREATE INDEX "end_datetime_idx" ON items (end_datetime);
CREATE INDEX "properties_idx" ON items USING GIN (properties jsonb_path_ops);
CREATE INDEX "collection_idx" ON items (collection_id);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);
CREATE UNIQUE INDEX "items_id_datetime_idx" ON items (datetime, id);

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION analyze_empty_partitions() RETURNS VOID AS $$
DECLARE
    p text;
BEGIN
    FOR p IN SELECT partition FROM all_items_partitions WHERE est_cnt = 0 LOOP
        EXECUTE format('ANALYZE %I;', p);
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION items_partition_name(timestamptz) RETURNS text AS $$
    SELECT to_char($1, '"items_p"IYYY"w"IW');
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION items_partition_exists(text) RETURNS boolean AS $$
    SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname=$1);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION items_partition_exists(timestamptz) RETURNS boolean AS $$
    SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname=items_partition_name($1));
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION items_partition_create_worker(partition text, partition_start timestamptz, partition_end timestamptz) RETURNS VOID AS $$
DECLARE
    err_context text;
BEGIN
    EXECUTE format(
        $f$
            CREATE TABLE IF NOT EXISTS %1$I PARTITION OF items
                FOR VALUES FROM (%2$L)  TO (%3$L);
            CREATE UNIQUE INDEX IF NOT EXISTS %4$I ON %1$I (id);
        $f$,
        partition,
        partition_start,
        partition_end,
        concat(partition, '_id_pk')
    );
EXCEPTION
        WHEN duplicate_table THEN
            RAISE NOTICE 'Partition % already exists.', partition;
        WHEN others THEN
            GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;
            RAISE INFO 'Error Name:%',SQLERRM;
            RAISE INFO 'Error State:%', SQLSTATE;
            RAISE INFO 'Error Context:%', err_context;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH to pgstac, public;

CREATE OR REPLACE FUNCTION items_partition_create(ts timestamptz) RETURNS text AS $$
DECLARE
    partition text := items_partition_name(ts);
    partition_start timestamptz;
    partition_end timestamptz;
BEGIN
    IF items_partition_exists(partition) THEN
        RETURN partition;
    END IF;
    partition_start := date_trunc('week', ts);
    partition_end := partition_start + '1 week'::interval;
    PERFORM items_partition_create_worker(partition, partition_start, partition_end);
    RAISE NOTICE 'partition: %', partition;
    RETURN partition;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION items_partition_create(st timestamptz, et timestamptz) RETURNS SETOF text AS $$
WITH t AS (
    SELECT
        generate_series(
            date_trunc('week',st),
            date_trunc('week', et),
            '1 week'::interval
        ) w
)
SELECT items_partition_create(w) FROM t;
$$ LANGUAGE SQL;


CREATE UNLOGGED TABLE items_staging (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_insert_triggerfunc() RETURNS TRIGGER AS $$
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
        WHERE partitions && _partitions
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
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER items_staging_insert_trigger AFTER INSERT ON items_staging REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_insert_triggerfunc();


CREATE UNLOGGED TABLE items_staging_ignore (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_ignore_insert_triggerfunc() RETURNS TRIGGER AS $$
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
        WHERE partitions && _partitions
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
        ON CONFLICT (datetime, id) DO NOTHING
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
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER items_staging_ignore_insert_trigger AFTER INSERT ON items_staging_ignore REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_ignore_insert_triggerfunc();

CREATE UNLOGGED TABLE items_staging_upsert (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_upsert_insert_triggerfunc() RETURNS TRIGGER AS $$
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
        WHERE partitions && _partitions
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
        ON CONFLICT (datetime, id) DO UPDATE SET
            content = EXCLUDED.content
            WHERE items.content IS DISTINCT FROM EXCLUDED.content
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
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER items_staging_upsert_insert_trigger AFTER INSERT ON items_staging_upsert REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_upsert_insert_triggerfunc();

CREATE OR REPLACE FUNCTION items_update_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
BEGIN
    NEW.id := NEW.content->>'id';
    NEW.datetime := stac_datetime(NEW.content);
    NEW.end_datetime := stac_end_datetime(NEW.content);
    NEW.collection_id := NEW.content->>'collection';
    NEW.geometry := stac_geom(NEW.content);
    NEW.properties := properties_idx(NEW.content);
    IF TG_OP = 'UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER items_update_trigger BEFORE UPDATE ON items
    FOR EACH ROW EXECUTE PROCEDURE items_update_triggerfunc();

/*
View to get a table of available items partitions
with date ranges
*/
DROP VIEW IF EXISTS all_items_partitions CASCADE;
CREATE VIEW all_items_partitions AS
WITH base AS
(SELECT
    c.oid::pg_catalog.regclass::text as partition,
    pg_catalog.pg_get_expr(c.relpartbound, c.oid) as _constraint,
    regexp_matches(
        pg_catalog.pg_get_expr(c.relpartbound, c.oid),
        E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
    ) as t,
    reltuples::bigint as est_cnt
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhrelid AND i.inhparent = 'items'::regclass)
SELECT partition, tstzrange(
    t[1]::timestamptz,
    t[2]::timestamptz
), t[1]::timestamptz as pstart,
    t[2]::timestamptz as pend, est_cnt
FROM base
ORDER BY 2 desc;

CREATE OR REPLACE VIEW items_partitions AS
SELECT * FROM all_items_partitions WHERE est_cnt>0;

CREATE OR REPLACE FUNCTION get_item(_id text) RETURNS jsonb AS $$
    SELECT content FROM items WHERE id=_id;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION delete_item(_id text) RETURNS VOID AS $$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION update_item(data jsonb) RETURNS VOID AS $$
DECLARE
    out items%ROWTYPE;
BEGIN
    UPDATE items SET content=data WHERE id = data->>'id' RETURNING * INTO STRICT out;
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
FROM items WHERE collection_id=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_temporal_extent(id text) RETURNS jsonb AS $$
SELECT to_jsonb(array[array[min(datetime)::text, max(datetime)::text]])
FROM items WHERE collection_id=$1;
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
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION items_path(
    IN dotpath text,
    OUT field text,
    OUT path text,
    OUT path_txt text,
    OUT jsonpath text,
    OUT eq text
) RETURNS RECORD AS $$
DECLARE
path_elements text[];
last_element text;
BEGIN
dotpath := replace(trim(dotpath), 'properties.', '');

IF dotpath = '' THEN
    RETURN;
END IF;

path_elements := string_to_array(dotpath, '.');
jsonpath := NULL;

IF path_elements[1] IN ('id','geometry','datetime') THEN
    field := path_elements[1];
    path_elements := path_elements[2:];
ELSIF path_elements[1] = 'collection' THEN
    field := 'collection_id';
    path_elements := path_elements[2:];
ELSIF path_elements[1] IN ('links', 'assets', 'stac_version', 'stac_extensions') THEN
    field := 'content';
ELSE
    field := 'content';
    path_elements := '{properties}'::text[] || path_elements;
END IF;
IF cardinality(path_elements)<1 THEN
    path := field;
    path_txt := field;
    jsonpath := '$';
    eq := NULL;
    RETURN;
END IF;


last_element := path_elements[cardinality(path_elements)];
path_elements := path_elements[1:cardinality(path_elements)-1];
jsonpath := concat(array_to_string('{$}'::text[] || array_map_ident(path_elements), '.'), '.', quote_ident(last_element));
path_elements := array_map_literal(path_elements);
path     := format($F$ properties->%s $F$, quote_literal(dotpath));
path_txt := format($F$ properties->>%s $F$, quote_literal(dotpath));
eq := format($F$ properties @? '$.%s[*] ? (@ == %%s) '$F$, quote_ident(dotpath));

RAISE NOTICE 'ITEMS PATH -- % % % % %', field, path, path_txt, jsonpath, eq;
RETURN;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION parse_dtrange(IN _indate jsonb, OUT _tstzrange tstzrange) AS $$
WITH t AS (
    SELECT CASE
        WHEN jsonb_typeof(_indate) = 'array' THEN
            textarr(_indate)
        ELSE
            regexp_split_to_array(
                btrim(_indate::text,'"'),
                '/'
            )
        END AS arr
)
, t1 AS (
    SELECT
        CASE
            WHEN array_upper(arr,1) = 1 OR arr[1] = '..' OR arr[1] IS NULL THEN '-infinity'::timestamptz
            ELSE arr[1]::timestamptz
        END AS st,
        CASE
            WHEN array_upper(arr,1) = 1 THEN arr[1]::timestamptz
            WHEN arr[2] = '..' OR arr[2] IS NULL THEN 'infinity'::timestamptz
            ELSE arr[2]::timestamptz
        END AS et
    FROM t
)
SELECT
    tstzrange(st,et)
FROM t1;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


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
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION cql_and_append(existing jsonb, newfilters jsonb) RETURNS jsonb AS $$
SELECT CASE WHEN existing ? 'filter' AND newfilters IS NOT NULL THEN
    jsonb_build_object(
        'and',
        jsonb_build_array(
            existing->'filter',
            newfilters
        )
    )
ELSE
    newfilters
END;
$$ LANGUAGE SQL;


-- ADDs base filters (ids, collections, datetime, bbox, intersects) that are
-- added outside of the filter/query in the stac request
CREATE OR REPLACE FUNCTION add_filters_to_cql(j jsonb) RETURNS jsonb AS $$
DECLARE
newprop jsonb;
newprops jsonb := '[]'::jsonb;
BEGIN
IF j ? 'ids' THEN
    newprop := jsonb_build_object(
        'in',
        jsonb_build_array(
            '{"property":"id"}'::jsonb,
            j->'ids'
        )
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;
IF j ? 'collections' THEN
    newprop := jsonb_build_object(
        'in',
        jsonb_build_array(
            '{"property":"collection"}'::jsonb,
            j->'collections'
        )
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

IF j ? 'datetime' THEN
    newprop := format(
        '{"anyinteracts":[{"property":"datetime"}, %s]}',
        j->'datetime'
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

IF j ? 'bbox' THEN
    newprop := format(
        '{"intersects":[{"property":"geometry"}, %s]}',
        j->'bbox'
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

IF j ? 'intersects' THEN
    newprop := format(
        '{"intersects":[{"property":"geometry"}, %s]}',
        j->'intersects'
    );
    newprops := jsonb_insert(newprops, '{1}', newprop);
END IF;

RAISE NOTICE 'newprops: %', newprops;

IF newprops IS NOT NULL AND jsonb_array_length(newprops) > 0 THEN
    return jsonb_set(
        j,
        '{filter}',
        cql_and_append(j, jsonb_build_object('and', newprops))
    ) - '{ids,collections,datetime,bbox,intersects}'::text[];
END IF;

return j;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION query_to_cqlfilter(j jsonb) RETURNS jsonb AS $$
-- Translates anything passed in through the deprecated "query" into equivalent CQL
WITH t AS (
    SELECT key as property, value as ops
        FROM jsonb_each(j->'query')
), t2 AS (
    SELECT property, (jsonb_each(ops)).*
        FROM t WHERE jsonb_typeof(ops) = 'object'
    UNION ALL
    SELECT property, 'eq', ops
        FROM t WHERE jsonb_typeof(ops) != 'object'
), t3 AS (
SELECT
    jsonb_strip_nulls(jsonb_build_object(
        'and',
        jsonb_agg(
            jsonb_build_object(
                key,
                jsonb_build_array(
                    jsonb_build_object('property',property),
                    value
                )
            )
        )
    )) as qcql FROM t2
)
SELECT
    CASE WHEN qcql IS NOT NULL THEN
        jsonb_set(j, '{filter}', cql_and_append(j, qcql)) - 'query'
    ELSE j
    END
FROM t3
;
$$ LANGUAGE SQL;



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
$$ LANGUAGE PLPGSQL;

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


/* cql_query_op -- Parses a CQL query operation, recursing when necessary
     IN jsonb -- a subelement from a valid stac query
     IN text -- the operator being used on elements passed in
     RETURNS a SQL fragment to be used in a WHERE clause
*/
CREATE OR REPLACE FUNCTION cql_query_op(j jsonb, _op text DEFAULT NULL) RETURNS text AS $$
DECLARE
jtype text := jsonb_typeof(j);
op text := lower(_op);
ops jsonb :=
    '{
        "eq": "%s = %s",
        "lt": "%s < %s",
        "lte": "%s <= %s",
        "gt": "%s > %s",
        "gte": "%s >= %s",
        "like": "%s LIKE %s",
        "+": "%s + %s",
        "-": "%s - %s",
        "*": "%s * %s",
        "/": "%s / %s",
        "in": "%s = ANY (%s)",
        "not": "NOT (%s)",
        "between": "%s BETWEEN %s AND %s",
        "lower":"lower(%s)"
    }'::jsonb;
ret text;
args text[] := NULL;

BEGIN
RAISE NOTICE 'j: %, op: %, jtype: %', j, op, jtype;

-- Set Lower Case on Both Arguments When Case Insensitive Flag Set
IF op in ('eq','lt','lte','gt','gte','like') AND jsonb_typeof(j->2) = 'boolean' THEN
    IF (j->>2)::boolean THEN
        RETURN format(concat('(',ops->>op,')'), cql_query_op(jsonb_build_array(j->0), 'lower'), cql_query_op(jsonb_build_array(j->1), 'lower'));
    END IF;
END IF;

-- Special Case when comparing a property in a jsonb field to a string or number using eq
-- Allows to leverage GIN index on jsonb fields
IF op = 'eq' THEN
    IF j->0 ? 'property'
        AND jsonb_typeof(j->1) IN ('number','string')
        AND (items_path(j->0->>'property')).eq IS NOT NULL
    THEN
        RETURN format((items_path(j->0->>'property')).eq, j->1);
    END IF;
END IF;

IF op ilike 't_%' or op = 'anyinteracts' THEN
    RETURN temporal_op_query(op, j);
END IF;

IF op ilike 's_%' or op = 'intersects' THEN
    RETURN spatial_op_query(op, j);
END IF;


IF jtype = 'object' THEN
    RAISE NOTICE 'parsing object';
    IF j ? 'property' THEN
        -- Convert the property to be used as an identifier
        return (items_path(j->>'property')).path_txt;
    ELSIF _op IS NULL THEN
        -- Iterate to convert elements in an object where the operator has not been set
        -- Combining with AND
        SELECT
            array_to_string(array_agg(cql_query_op(e.value, e.key)), ' AND ')
        INTO ret
        FROM jsonb_each(j) e;
        RETURN ret;
    END IF;
END IF;

IF jtype = 'string' THEN
    RETURN quote_literal(j->>0);
END IF;

IF jtype ='number' THEN
    RETURN (j->>0)::numeric;
END IF;

IF jtype = 'array' AND op IS NULL THEN
    RAISE NOTICE 'Parsing array into array arg. j: %', j;
    SELECT format($f$ '{%s}'::text[] $f$, string_agg(e,',')) INTO ret FROM jsonb_array_elements_text(j) e;
    RETURN ret;
END IF;


-- If the type of the passed json is an array
-- Calculate the arguments that will be passed to functions/operators
IF jtype = 'array' THEN
    RAISE NOTICE 'Parsing array into args. j: %', j;
    -- If any argument is numeric, cast any text arguments to numeric
    IF j @? '$[*] ? (@.type() == "number")' THEN
        SELECT INTO args
            array_agg(concat('(',cql_query_op(e),')::numeric'))
        FROM jsonb_array_elements(j) e;
    ELSE
        SELECT INTO args
            array_agg(cql_query_op(e))
        FROM jsonb_array_elements(j) e;
    END IF;
    --RETURN args;
END IF;
RAISE NOTICE 'ARGS after array cleaning: %', args;

IF op IS NULL THEN
    RETURN args::text[];
END IF;

IF args IS NULL OR cardinality(args) < 1 THEN
    RAISE NOTICE 'No Args';
    RETURN '';
END IF;

IF op IN ('and','or') THEN
    SELECT
        CONCAT(
            '(',
            array_to_string(args, UPPER(CONCAT(' ',op,' '))),
            ')'
        ) INTO ret
        FROM jsonb_array_elements(j) e;
        RETURN ret;
END IF;

-- If the op is in the ops json then run using the template in the json
IF ops ? op THEN
    RAISE NOTICE 'ARGS: % MAPPED: %',args, array_map_literal(args);

    RETURN format(concat('(',ops->>op,')'), VARIADIC args);
END IF;

RETURN j->>0;

END;
$$ LANGUAGE PLPGSQL;




CREATE OR REPLACE FUNCTION cql_to_where(_search jsonb = '{}'::jsonb) RETURNS text AS $$
DECLARE
search jsonb := _search;
_where text;
BEGIN
RAISE NOTICE 'SEARCH CQL 1: %', search;

-- Convert any old style stac query to cql
search := query_to_cqlfilter(search);

RAISE NOTICE 'SEARCH CQL 2: %', search;

-- Convert item,collection,datetime,bbox,intersects to cql
search := add_filters_to_cql(search);

RAISE NOTICE 'SEARCH CQL Final: %', search;
_where := cql_query_op(search->'filter');

IF trim(_where) = '' THEN
    _where := NULL;
END IF;
_where := coalesce(_where, ' TRUE ');
RETURN _where;
END;
$$ LANGUAGE PLPGSQL;


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

CREATE OR REPLACE FUNCTION field_orderby(p text) RETURNS text AS $$
WITH t AS (
    SELECT
        replace(trim(substring(indexdef from 'btree \((.*)\)')),' ','')as s
    FROM pg_indexes WHERE schemaname='pgstac' AND tablename='items' AND indexdef ~* 'btree' AND indexdef ~* 'properties'
) SELECT s FROM t WHERE strpos(s, lower(trim(p)))>0;
$$ LANGUAGE SQL;

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
        coalesce(field_orderby((items_path(value->>'field')).path_txt), (items_path(value->>'field')).path) as key,
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
        SELECT to_jsonb(items) INTO token_rec FROM items WHERE id=token_id;
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
    _where text PRIMARY KEY,
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

CREATE OR REPLACE FUNCTION where_stats(inwhere text, updatestats boolean default false) RETURNS search_wheres AS $$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    partitions text[];
    sw search_wheres%ROWTYPE;
BEGIN
    SELECT * INTO sw FROM search_wheres WHERE _where=inwhere FOR UPDATE;

    -- Update statistics if explicitly set, if statistics do not exist, or statistics ttl has expired
    IF NOT updatestats THEN
        RAISE NOTICE 'Checking if update is needed.';
        RAISE NOTICE 'Stats Last Updated: %', sw.statslastupdated;
        RAISE NOTICE 'TTL: %, Age: %', context_stats_ttl(), now() - sw.statslastupdated;
        RAISE NOTICE 'Context: %, Existing Total: %', context(), sw.total_count;
        IF
            sw.statslastupdated IS NULL
            OR (now() - sw.statslastupdated) > context_stats_ttl()
            OR (context() != 'off' AND sw.total_count IS NULL)
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
        WHERE _where = inwhere
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
        context() = 'on'
        OR
        ( context() = 'auto' AND
            (
                sw.estimated_count < context_estimated_count()
                OR
                sw.estimated_cost < context_estimated_cost()
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


    INSERT INTO search_wheres SELECT sw.*
    ON CONFLICT (_where)
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
$$ LANGUAGE PLPGSQL ;



CREATE OR REPLACE FUNCTION items_count(_where text) RETURNS bigint AS $$
DECLARE
cnt bigint;
BEGIN
EXECUTE format('SELECT count(*) FROM items WHERE %s', _where) INTO cnt;
RETURN cnt;
END;
$$ LANGUAGE PLPGSQL;





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
    search._where := cql_to_where(_search);
END IF;

-- Calculate the order by clause if not already calculated
IF search.orderby IS NULL THEN
    search.orderby := sort_sqlorderby(_search);
END IF;

PERFORM where_stats(search._where, updatestats);

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
BEGIN
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

CREATE TEMP TABLE results (content jsonb) ON COMMIT DROP;


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

IF context() != 'off' THEN
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
$$ LANGUAGE PLPGSQL
SET jit TO off
;
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
    out_records jsonb[] := '{}'::jsonb[];
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

    IF fields IS NOT NULL THEN
        IF fields ? 'fields' THEN
            fields := fields->'fields';
        END IF;
        IF fields ? 'exclude' THEN
            excludes=textarr(fields->'exclude');
        END IF;
        IF fields ? 'include' THEN
            includes=textarr(fields->'include');
            IF array_length(includes, 1)>0 AND NOT 'id' = ANY (includes) THEN
                includes = includes || '{id}';
            END IF;
        END IF;
    END IF;
    RAISE NOTICE 'fields: %, includes: %, excludes: %', fields, includes, excludes;

    FOR query IN SELECT * FROM partition_queries(_where, search.orderby) LOOP
        query := format('%s LIMIT %L', query, remaining_limit);
        RAISE NOTICE '%', query;
        curs = create_cursor(query);
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

            IF fields IS NOT NULL THEN
                out_records := out_records || filter_jsonb(iter_record.content, includes, excludes);
            ELSE
                out_records := out_records || iter_record.content;
            END IF;
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
        EXIT WHEN exit_flag;
        remaining_limit := _scanlimit - scancounter;
    END LOOP;

    RETURN jsonb_build_object(
        'type', 'FeatureCollection',
        'features', array_to_json(out_records)::jsonb
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
SELECT set_version('0.3.6');
