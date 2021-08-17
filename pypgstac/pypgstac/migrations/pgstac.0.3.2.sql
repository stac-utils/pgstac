CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS pgstac;
SET SEARCH_PATH TO pgstac, public;

CREATE TABLE migrations (
  version text,
  datetime timestamptz DEFAULT now() NOT NULL
);

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

CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC'
) RETURNS SETOF text AS $$
DECLARE
    partition_query text;
    query text;
    p record;
    cursors refcursor;
BEGIN
IF _orderby ILIKE 'datetime d%' THEN
    partition_query := format($q$
        SELECT partition, tstzrange
        FROM items_partitions
        ORDER BY tstzrange DESC;
    $q$);
ELSIF _orderby ILIKE 'datetime a%' THEN
    partition_query := format($q$
        SELECT partition, tstzrange
        FROM items_partitions
        ORDER BY tstzrange ASC
        ;
    $q$);
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
FOR p IN
    EXECUTE partition_query
LOOP
    query := format($q$
        SELECT * FROM items
        WHERE datetime >= %L AND datetime < %L AND %s
        ORDER BY %s
    $q$, lower(p.tstzrange), upper(p.tstzrange), _where, _orderby
    );
    RETURN NEXT query;
END LOOP;
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
FOR query IN SELECT * FROM partion_queries(_where, _orderby) LOOP
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
$$ LANGUAGE SQL;

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
    mindate timestamptz;
    maxdate timestamptz;
    partition text;
BEGIN
    SELECT min(stac_datetime(content)), max(stac_datetime(content)) INTO mindate, maxdate FROM newdata;
    PERFORM items_partition_create(mindate, maxdate);
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
    PERFORM analyze_empty_partitions();
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
    mindate timestamptz;
    maxdate timestamptz;
    partition text;
BEGIN
    SELECT min(stac_datetime(content)), max(stac_datetime(content)) INTO mindate, maxdate FROM newdata;
    PERFORM items_partition_create(mindate, maxdate);
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
    DELETE FROM items_staging_ignore;
    PERFORM analyze_empty_partitions();
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
    mindate timestamptz;
    maxdate timestamptz;
    partition text;
BEGIN
    SELECT min(stac_datetime(content)), max(stac_datetime(content)) INTO mindate, maxdate FROM newdata;
    PERFORM items_partition_create(mindate, maxdate);
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
    DELETE FROM items_staging_upsert;
    PERFORM analyze_empty_partitions();
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
), est_cnt
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
    eq := NULL; -- format($F$ %s = %%s $F$, field);
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
IF j ? 'id' THEN
    newprop := jsonb_build_object(
        'in',
        jsonb_build_array(
            '{"property":"id"}'::jsonb,
            j->'id'
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
    ) - '{id,collections,datetime,bbox,intersects}'::text[];
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

CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE
) RETURNS text AS $$
WITH sorts AS (
    SELECT
        (items_path(value->>'field')).path as key,
        parse_sort_dir(value->>'direction', reverse) as dir
    FROM jsonb_array_elements(
        '[]'::jsonb
        ||
        coalesce(_search->'sortby','[{"field":"datetime", "direction":"desc"}]')
        ||
        '[{"field":"id","direction":"desc"}]'::jsonb
    )
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
        jsonb_array_elements(coalesce(_search->'sort','[{"field":"datetime","direction":"desc"}]'))
ON CONFLICT DO NOTHING
;

-- Get the first sort direction provided. As the id is a primary key, if there are any
-- sorts after id they won't do anything, so make sure that id is the last sort item.
SELECT _dir INTO dir FROM sorts ORDER BY _row ASC LIMIT 1;
IF EXISTS (SELECT 1 FROM sorts WHERE _field = 'id') THEN
    DELETE FROM sorts WHERE _row > (SELECT _row FROM sorts WHERE _field = 'id');
ELSE
    INSERT INTO sorts (_field, _dir) VALUES ('id', dir);
END IF;

-- Add value from looked up item to the sorts table
UPDATE sorts SET _val=quote_literal(token_rec->>_field);

-- Check if all sorts are the same direction and use row comparison
-- to filter
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

CREATE OR REPLACE FUNCTION search_hash(jsonb) RETURNS text AS $$
    SELECT md5(search_tohash($1)::text);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE TABLE IF NOT EXISTS searches(
    hash text GENERATED ALWAYS AS (search_hash(search)) STORED PRIMARY KEY,
    search jsonb NOT NULL,
    _where text,
    orderby text,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    statslastupdated timestamptz,
    estimated_count bigint,
    total_count bigint
);

CREATE OR REPLACE FUNCTION search_query(_search jsonb = '{}'::jsonb, updatestats boolean DEFAULT false) RETURNS searches AS $$
DECLARE
    search searches%ROWTYPE;
BEGIN
INSERT INTO searches (search)
    VALUES (search_tohash(_search))
    ON CONFLICT DO NOTHING
    RETURNING * INTO search;
IF search.hash IS NULL THEN
    SELECT * INTO search FROM searches WHERE hash=search_hash(_search);
END IF;
IF search._where IS NULL THEN
    search._where := cql_to_where(_search);
END IF;
IF search.orderby IS NULL THEN
    search.orderby := sort_sqlorderby(_search);
END IF;

IF search.statslastupdated IS NULL OR age(search.statslastupdated) > '1 day'::interval OR (_search ? 'context' AND search.total_count IS NULL) THEN
    updatestats := TRUE;
END IF;

IF updatestats THEN
    -- Get Estimated Stats
    RAISE NOTICE 'Getting stats for %', search._where;
    search.estimated_count := estimated_count(search._where);
    RAISE NOTICE 'Estimated Count: %', search.estimated_count;

    IF _search ? 'context' OR search.estimated_count < 10000 THEN
        --search.total_count := partition_count(search._where);
        EXECUTE format(
            'SELECT count(*) FROM items WHERE %s',
            search._where
        ) INTO search.total_count;
        RAISE NOTICE 'Actual Count: %', search.total_count;
    ELSE
        search.total_count := NULL;
    END IF;
    search.statslastupdated := now();
END IF;

search.lastused := now();
search.usecount := coalesce(search.usecount,0) + 1;
RAISE NOTICE 'SEARCH: %', search;
UPDATE searches SET
    _where = search._where,
    orderby = search.orderby,
    lastused = search.lastused,
    usecount = search.usecount,
    statslastupdated = search.statslastupdated,
    estimated_count = search.estimated_count,
    total_count = search.total_count
WHERE hash = search.hash
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
BEGIN
searches := search_query(_search);
_where := searches._where;
orderby := searches.orderby;
total_count := coalesce(searches.total_count, searches.estimated_count);


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

FOR query IN SELECT partition_queries(full_where, orderby) LOOP
    timer := clock_timestamp();
    query := format('%s LIMIT %L', query, _limit + 1);
    RAISE NOTICE 'Partition Query: %', query;
    batches := batches + 1;
    curs = create_cursor(query);
    LOOP
        FETCH curs into iter_record;
        EXIT WHEN NOT FOUND;
        cntr := cntr + 1;
        last_record := iter_record;
        IF cntr = 1 THEN
            first_record := last_record;
        END IF;
        IF cntr <= _limit THEN
            out_records := out_records || last_record.content;
        ELSIF cntr > _limit THEN
            has_next := true;
            exit_flag := true;
            EXIT;
        END IF;
    END LOOP;
    RAISE NOTICE 'Query took %', clock_timestamp()-timer;
    timer := clock_timestamp();
    EXIT WHEN exit_flag;
END LOOP;
RAISE NOTICE 'Scanned through % partitions.', batches;


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


context := jsonb_strip_nulls(jsonb_build_object(
    'limit', _limit,
    'matched', total_count,
    'returned', coalesce(jsonb_array_length(out_records), 0)
));

collection := jsonb_build_object(
    'type', 'FeatureCollection',
    'features', out_records,
    'next', next,
    'prev', prev,
    'context', context
);

RETURN collection;
END;
$$ LANGUAGE PLPGSQL SET jit TO off;
INSERT INTO pgstac.migrations (version) VALUES ('0.3.2');
