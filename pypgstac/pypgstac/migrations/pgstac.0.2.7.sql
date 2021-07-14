CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE SCHEMA IF NOT EXISTS pgstac;


SET SEARCH_PATH TO pgstac, public;

CREATE TABLE migrations (
  version text,
  datetime timestamptz DEFAULT now() NOT NULL
);

/* converts a jsonb text array to a pg text[] array */
CREATE OR REPLACE FUNCTION textarr(_js jsonb)
  RETURNS text[] AS $$
  SELECT ARRAY(SELECT jsonb_array_elements_text(_js));
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;

/*
converts a jsonb text array to comma delimited list of identifer quoted
useful for constructing column lists for selects
*/
CREATE OR REPLACE FUNCTION array_idents(_js jsonb)
  RETURNS text AS $$
  SELECT string_agg(quote_ident(v),',') FROM jsonb_array_elements_text(_js) v;
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE;


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
SELECT (value->'properties'->>'datetime')::timestamptz;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

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

CREATE OR REPLACE FUNCTION empty_arr(ANYARRAY) RETURNS BOOLEAN AS $$
SELECT CASE
  WHEN $1 IS NULL THEN TRUE
  WHEN cardinality($1)<1 THEN TRUE
ELSE FALSE
END;
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

CREATE OR REPLACE FUNCTION properties_idx(_in jsonb) RETURNS jsonb AS $$
WITH t AS (
  select array_to_string(path,'.') as path, lower(value::text)::jsonb as lowerval
  FROM  jsonb_val_paths(_in)
  WHERE array_to_string(path,'.') not in ('datetime')
)
SELECT jsonb_object_agg(path, lowerval) FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
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






/* CREATE OR REPLACE FUNCTION collections_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() = 1 THEN
        PERFORM create_collection(NEW.content);
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collections_trigger
BEFORE INSERT ON collections
FOR EACH ROW EXECUTE PROCEDURE collections_trigger_func();
 */
SET SEARCH_PATH TO pgstac, public;

CREATE TABLE IF NOT EXISTS items (
    id VARCHAR GENERATED ALWAYS AS (content->>'id') STORED NOT NULL,
    geometry geometry GENERATED ALWAYS AS (stac_geom(content)) STORED NOT NULL,
    properties jsonb GENERATED ALWAYS as (properties_idx(content->'properties')) STORED,
    collection_id text GENERATED ALWAYS AS (content->>'collection') STORED NOT NULL,
    datetime timestamptz GENERATED ALWAYS AS (stac_datetime(content)) STORED NOT NULL,
    content JSONB NOT NULL
)
PARTITION BY RANGE (stac_datetime(content))
;

ALTER TABLE items ADD constraint items_collections_fk FOREIGN KEY (collection_id) REFERENCES collections(id) DEFERRABLE;

CREATE TABLE items_template (
    LIKE items
);

ALTER TABLE items_template ADD PRIMARY KEY (id);

/*
CREATE TABLE IF NOT EXISTS items_search (
    id text NOT NULL,
    geometry geometry NOT NULL,
    properties jsonb,
    collection_id text NOT NULL,
    datetime timestamptz NOT NULL
)
PARTITION BY RANGE (datetime)
;

CREATE TABLE IF NOT EXISTS items_search_template (
    LIKE items_search
)
;
ALTER TABLE items_search_template ADD PRIMARY KEY (id);
*/

DELETE from partman.part_config WHERE parent_table = 'pgstac.items';
SELECT partman.create_parent(
    'pgstac.items',
    'datetime',
    'native',
    'weekly',
    p_template_table := 'pgstac.items_template',
    p_premake := 4
);

CREATE OR REPLACE FUNCTION make_partitions(st timestamptz, et timestamptz DEFAULT NULL) RETURNS BOOL AS $$
WITH t AS (
    SELECT
        generate_series(
            date_trunc('week',st),
            date_trunc('week', coalesce(et, st)),
            '1 week'::interval
        ) w
),
w AS (SELECT array_agg(w) as w FROM t)
SELECT CASE WHEN w IS NULL THEN NULL ELSE partman.create_partition_time('pgstac.items', w, true) END FROM w;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_partition(timestamptz) RETURNS text AS $$
SELECT to_char($1, '"items_p"IYYY"w"IW');
$$ LANGUAGE SQL;

CREATE INDEX "datetime_id_idx" ON items (datetime, id);
CREATE INDEX "properties_idx" ON items USING GIN (properties);
CREATE INDEX "collection_idx" ON items (collection_id);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);


CREATE TYPE item AS (
    id text,
    geometry geometry,
    properties JSONB,
    collection_id text,
    datetime timestamptz
);


/*
Converts single feature into an items row
*/

/*
CREATE OR REPLACE FUNCTION feature_to_item(value jsonb) RETURNS item AS $$
    SELECT
        value->>'id' as id,
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
        END as geometry,
        properties_idx(value ->'properties') as properties,
        value->>'collection' as collection_id,
        (value->'properties'->>'datetime')::timestamptz as datetime
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;
*/
/*
Takes a single feature, an array of features, or a feature collection
and returns a set up individual items rows
*/
/*
CREATE OR REPLACE FUNCTION features_to_items(value jsonb) RETURNS SETOF item AS $$
    WITH features AS (
        SELECT
        jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(value) = 'array' THEN value
                WHEN value->>'type' = 'Feature' THEN '[]'::jsonb || value
                WHEN value->>'type' = 'FeatureCollection' THEN value->'features'
                ELSE NULL
            END
        ) as value
    )
    SELECT feature_to_item(value) FROM features
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;
*/

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
    SELECT make_partitions(stac_datetime(data));
    INSERT INTO items (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION update_item(data jsonb) RETURNS VOID AS $$
DECLARE
out items%ROWTYPE;
BEGIN
    UPDATE items SET content=data WHERE id = data->>'id' RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_item(data jsonb) RETURNS VOID AS $$
DECLARE
partition text;
q text;
newcontent jsonb;
BEGIN
    PERFORM make_partitions(stac_datetime(data));
    partition := get_partition(stac_datetime(data));
    q := format($q$
        INSERT INTO %I (content) VALUES ($1)
        ON CONFLICT (id) DO
        UPDATE SET content = EXCLUDED.content
        WHERE %I.content IS DISTINCT FROM EXCLUDED.content RETURNING content;
        $q$, partition, partition);
    EXECUTE q INTO newcontent USING (data);
    RAISE NOTICE 'newcontent: %', newcontent;
    RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION analyze_empty_partitions() RETURNS VOID AS $$
DECLARE
p text;
BEGIN
FOR p IN SELECT partition FROM all_items_partitions WHERE est_cnt = 0 LOOP
    RAISE NOTICE 'Analyzing %', p;
    EXECUTE format('ANALYZE %I;', p);
END LOOP;
END;
$$ LANGUAGE PLPGSQL;

/* Trigger Function to cascade inserts/updates/deletes
from items table to items_search table */
/*
ALTER TABLE items_search ADD CONSTRAINT items_search_fk
FOREIGN KEY (id) REFERENCES items(id)
ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION items_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
BEGIN
    IF TG_OP = 'UPDATE' THEN
    RAISE NOTICE 'DELETING % BEFORE UPDATE', OLD;
        DELETE FROM items_search WHERE id = OLD.id AND datetime = (OLD.content->'properties'->>'datetime')::timestamptz;
    END IF;

    INSERT INTO items_search SELECT * FROM feature_to_item(NEW.content);
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

DROP TRIGGER IF EXISTS items_insert_trigger ON items;
CREATE TRIGGER items_insert_trigger
AFTER INSERT ON items
FOR EACH ROW EXECUTE PROCEDURE items_trigger_func();

DROP TRIGGER IF EXISTS items_update_trigger ON items;
CREATE TRIGGER items_update_trigger
AFTER UPDATE ON items
FOR EACH ROW
WHEN (NEW.content IS DISTINCT FROM OLD.content)
EXECUTE PROCEDURE items_trigger_func();
*/

/* Trigger Function to cascade inserts/updates/deletes
from items table to items_search table */
/*
CREATE OR REPLACE FUNCTION items_search_trigger_delete_func()
RETURNS TRIGGER AS $$
DECLARE
BEGIN
    RAISE NOTICE 'Deleting from items_search: % Depth: %', OLD, pg_trigger_depth();
    IF pg_trigger_depth()<3 THEN
        RAISE NOTICE 'DELETING WITH datetime';
        DELETE FROM items_search WHERE id=OLD.id AND datetime=OLD.datetime;
        RETURN NULL;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

DROP TRIGGER IF EXISTS items_search_delete_trigger ON items_search;
CREATE TRIGGER items_search_delete_trigger
BEFORE DELETE ON items_search
FOR EACH ROW EXECUTE PROCEDURE items_search_trigger_delete_func();
*/
CREATE OR REPLACE FUNCTION backfill_partitions()
RETURNS VOID AS $$
DECLARE
BEGIN
    IF EXISTS (SELECT 1 FROM items_default LIMIT 1) THEN
        RAISE NOTICE 'Creating new partitions and moving data from default';
        CREATE TEMP TABLE items_default_tmp ON COMMIT DROP AS SELECT datetime, content FROM items_default;
        TRUNCATE items_default;
        PERFORM make_partitions(min(datetime), max(datetime)) FROM items_default_tmp;
        INSERT INTO items (content) SELECT content FROM items_default_tmp;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION items_trigger_stmt_func()
RETURNS TRIGGER AS $$
DECLARE
BEGIN
    PERFORM analyze_empty_partitions();
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

DROP TRIGGER IF EXISTS items_stmt_trigger ON items;
CREATE TRIGGER items_stmt_trigger
AFTER INSERT OR UPDATE OR DELETE ON items
FOR EACH STATEMENT EXECUTE PROCEDURE items_trigger_stmt_func();


/*
View to get a table of available items partitions
with date ranges
*/
--DROP VIEW IF EXISTS all_items_partitions CASCADE;
CREATE OR REPLACE VIEW all_items_partitions AS
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

--DROP VIEW IF EXISTS items_partitions;
CREATE OR REPLACE VIEW items_partitions AS
SELECT * FROM all_items_partitions WHERE est_cnt>0;

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

/*
View to get a table of available items partitions
with date ranges
*/
DROP VIEW IF EXISTS items_partitions;
CREATE VIEW items_partitions AS
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
WHERE est_cnt >0
ORDER BY 2 desc;


CREATE OR REPLACE FUNCTION items_by_partition(
    IN _where text DEFAULT 'TRUE',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity'),
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 10
) RETURNS SETOF items AS $$
DECLARE
partition_query text;
main_query text;
batchcount int;
counter int := 0;
p record;
BEGIN
IF _orderby ILIKE 'datetime d%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange DESC;
    $q$);
ELSIF _orderby ILIKE 'datetime a%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange ASC
        ;
    $q$);
ELSE
    partition_query := format($q$
        SELECT 'items' as partition WHERE $1 IS NOT NULL;
    $q$);
END IF;
RAISE NOTICE 'Partition Query: %', partition_query;
FOR p IN
    EXECUTE partition_query USING (_dtrange)
LOOP
    IF lower(_dtrange)::timestamptz > '-infinity' THEN
        _where := concat(_where,format(' AND datetime >= %L',lower(_dtrange)::timestamptz::text));
    END IF;
    IF upper(_dtrange)::timestamptz < 'infinity' THEN
        _where := concat(_where,format(' AND datetime <= %L',upper(_dtrange)::timestamptz::text));
    END IF;

    main_query := format($q$
        SELECT * FROM %I
        WHERE %s
        ORDER BY %s
        LIMIT %s - $1
    $q$, p.partition::text, _where, _orderby, _limit
    );
    RAISE NOTICE 'Partition Query %', main_query;
    RAISE NOTICE '%', counter;
    RETURN QUERY EXECUTE main_query USING counter;

    GET DIAGNOSTICS batchcount = ROW_COUNT;
    counter := counter + batchcount;
    RAISE NOTICE 'FOUND %', batchcount;
    IF counter >= _limit THEN
        EXIT;
    END IF;
    RAISE NOTICE 'ADDED % FOR A TOTAL OF %', batchcount, counter;
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION split_stac_path(IN path text, OUT col text, OUT dotpath text, OUT jspath text, OUT jspathtext text) AS $$
WITH col AS (
    SELECT
        CASE WHEN
            split_part(path, '.', 1) IN ('id', 'stac_version', 'stac_extensions','geometry','properties','assets','collection_id','datetime','links', 'extra_fields') THEN split_part(path, '.', 1)
        ELSE 'properties'
        END AS col
),
dp AS (
    SELECT
        col, ltrim(replace(path, col , ''),'.') as dotpath
    FROM col
),
paths AS (
SELECT
    col, dotpath,
    regexp_split_to_table(dotpath,E'\\.') as path FROM dp
) SELECT
    col,
    btrim(concat(col,'.',dotpath),'.'),
    CASE WHEN btrim(concat(col,'.',dotpath),'.') != col THEN concat(col,'->',string_agg(concat('''',path,''''),'->')) ELSE col END,
    regexp_replace(
        CASE WHEN btrim(concat(col,'.',dotpath),'.') != col THEN concat(col,'->',string_agg(concat('''',path,''''),'->')) ELSE col END,
        E'>([^>]*)$','>>\1'
    )
FROM paths group by col, dotpath;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


/* Functions for searching items */
CREATE OR REPLACE FUNCTION sort_base(
    IN _sort jsonb DEFAULT '[{"field":"datetime","direction":"desc"}]',
    OUT key text,
    OUT col text,
    OUT dir text,
    OUT rdir text,
    OUT sort text,
    OUT rsort text
) RETURNS SETOF RECORD AS $$
WITH sorts AS (
    SELECT
        value->>'field' as key,
        (split_stac_path(value->>'field')).jspathtext as col,
        coalesce(upper(value->>'direction'),'ASC') as dir
    FROM jsonb_array_elements('[]'::jsonb || coalesce(_sort,'[{"field":"datetime","direction":"desc"}]') )
)
SELECT
    key,
    col,
    dir,
    CASE dir WHEN 'DESC' THEN 'ASC' ELSE 'ASC' END as rdir,
    concat(col, ' ', dir, ' NULLS LAST ') AS sort,
    concat(col,' ', CASE dir WHEN 'DESC' THEN 'ASC' ELSE 'ASC' END, ' NULLS LAST ') AS rsort
FROM sorts
UNION ALL
SELECT 'id', 'id', 'DESC', 'ASC', 'id DESC', 'id ASC'
;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION sort(_sort jsonb) RETURNS text AS $$
SELECT string_agg(sort,', ') FROM sort_base(_sort);
$$ LANGUAGE SQL PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION rsort(_sort jsonb) RETURNS text AS $$
SELECT string_agg(rsort,', ') FROM sort_base(_sort);
$$ LANGUAGE SQL PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;


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

CREATE OR REPLACE FUNCTION in_array_q(col text, arr jsonb) RETURNS text AS $$
SELECT CASE jsonb_typeof(arr) WHEN 'array' THEN format('%I = ANY(textarr(%L))', col, arr) ELSE format('%I = %L', col, arr) END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION count_by_delim(text, text) RETURNS int AS $$
SELECT count(*) FROM regexp_split_to_table($1,$2);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION stac_query_op(att text, _op text, val jsonb) RETURNS text AS $$
DECLARE
ret text := '';
op text;
jp text;
att_parts RECORD;
val_str text;
prop_path text;
BEGIN
val_str := lower(jsonb_build_object('a',val)->>'a');
RAISE NOTICE 'val_str %', val_str;

att_parts := split_stac_path(att);
prop_path := replace(att_parts.dotpath, 'properties.', '');

op := CASE _op
    WHEN 'eq' THEN '='
    WHEN 'gte' THEN '>='
    WHEN 'gt' THEN '>'
    WHEN 'lte' THEN '<='
    WHEN 'lt' THEN '<'
    WHEN 'ne' THEN '!='
    WHEN 'neq' THEN '!='
    WHEN 'startsWith' THEN 'LIKE'
    WHEN 'endsWith' THEN 'LIKE'
    WHEN 'contains' THEN 'LIKE'
    ELSE _op
END;

val_str := CASE _op
    WHEN 'startsWith' THEN concat(val_str, '%')
    WHEN 'endsWith' THEN concat('%', val_str)
    WHEN 'contains' THEN concat('%',val_str,'%')
    ELSE val_str
END;


RAISE NOTICE 'att_parts: % %', att_parts, count_by_delim(att_parts.dotpath,'\.');
IF
    op = '='
    AND att_parts.col = 'properties'
    --AND count_by_delim(att_parts.dotpath,'\.') = 2
THEN
    -- use jsonpath query to leverage index for eqaulity tests on single level deep properties
    jp := btrim(format($jp$ $.%I[*] ? ( @ == %s ) $jp$, replace(att_parts.dotpath, 'properties.',''), lower(val::text)::jsonb));
    raise notice 'jp: %', jp;
    ret := format($q$ properties @? %L $q$, jp);
ELSIF jsonb_typeof(val) = 'number' THEN
    ret := format('properties ? %L AND (%s)::numeric %s %s', prop_path, att_parts.jspathtext, op, val);
ELSE
    ret := format('properties ? %L AND %s %s %L', prop_path ,att_parts.jspathtext, op, val_str);
END IF;
RAISE NOTICE 'Op Query: %', ret;

return ret;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION stac_query(_query jsonb) RETURNS TEXT[] AS $$
DECLARE
qa text[];
att text;
ops jsonb;
op text;
val jsonb;
BEGIN
FOR att, ops IN SELECT key, value FROM jsonb_each(_query)
LOOP
    FOR op, val IN SELECT key, value FROM jsonb_each(ops)
    LOOP
        qa := array_append(qa, stac_query_op(att,op, val));
        RAISE NOTICE '% % %', att, op, val;
    END LOOP;
END LOOP;
RETURN qa;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION filter_by_order(item_id text, _sort jsonb, _type text) RETURNS text AS $$
DECLARE
item item;
BEGIN
SELECT * INTO item FROM items WHERE id=item_id;
RETURN filter_by_order(item, _sort, _type);
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- Used to create filters used for paging using the items id from the token
CREATE OR REPLACE FUNCTION filter_by_order(_item item, _sort jsonb, _type text) RETURNS text AS $$
DECLARE
sorts RECORD;
filts text[];
itemval text;
op text;
idop text;
ret text;
eq_flag text;
_item_j jsonb := to_jsonb(_item);
BEGIN
FOR sorts IN SELECT * FROM sort_base(_sort) LOOP
    IF sorts.col = 'datetime' THEN
        CONTINUE;
    END IF;
    IF sorts.col='id' AND _type IN ('prev','next') THEN
        eq_flag := '';
    ELSE
        eq_flag := '=';
    END IF;

    op := concat(
        CASE
            WHEN _type in ('prev','first') AND sorts.dir = 'ASC' THEN '<'
            WHEN _type in ('last','next') AND sorts.dir = 'ASC' THEN '>'
            WHEN _type in ('prev','first') AND sorts.dir = 'DESC' THEN '>'
            WHEN _type in ('last','next') AND sorts.dir = 'DESC' THEN '<'
        END,
        eq_flag
    );

    IF _item_j ? sorts.col THEN
        filts = array_append(filts, format('%s %s %L', sorts.col, op, _item_j->>sorts.col));
    END IF;
END LOOP;
ret := coalesce(array_to_string(filts,' AND '), 'TRUE');
RAISE NOTICE 'Order Filter %', ret;
RETURN ret;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION search_dtrange(IN _indate jsonb, OUT _tstzrange tstzrange) AS
$$
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

CREATE OR REPLACE FUNCTION flip_jsonb_array(j jsonb) RETURNS jsonb AS $$
WITH t AS (
    SELECT i, row_number() over () as r FROM jsonb_array_elements(j) i
), o AS (
    SELECT i FROM t ORDER BY r DESC
)
SELECT jsonb_agg(i) from o
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS SETOF jsonb AS $$
DECLARE
qstart timestamptz := clock_timestamp();
_sort text := '';
_rsort text := '';
_limit int := 10;
_geom geometry;
qa text[];
pq text[];
query text;
pq_prop record;
pq_op record;
prev_id text := NULL;
next_id text := NULL;
whereq text := 'TRUE';
links jsonb := '[]'::jsonb;
token text;
tok_val text;
tok_q text := 'TRUE';
tok_sort text;
first_id text;
first_dt timestamptz;
last_id text;
sort text;
rsort text;
dt text[];
dqa text[];
dq text;
mq_where text;
startdt timestamptz;
enddt timestamptz;
item items%ROWTYPE;
counter int := 0;
batchcount int;
month timestamptz;
m record;
_dtrange tstzrange := tstzrange('-infinity','infinity');
_dtsort text;
_token_dtrange tstzrange := tstzrange('-infinity','infinity');
_token_record items%ROWTYPE;
is_prev boolean := false;
includes text[];
excludes text[];
BEGIN
-- Create table from sort query of items to sort
CREATE TEMP TABLE pgstac_tmp_sorts ON COMMIT DROP AS SELECT * FROM sort_base(_search->'sortby');

-- Get the datetime sort direction, necessary for efficient cycling through partitions
SELECT INTO _dtsort dir FROM pgstac_tmp_sorts WHERE key='datetime';
RAISE NOTICE '_dtsort: %',_dtsort;

SELECT INTO _sort string_agg(s.sort,', ') FROM pgstac_tmp_sorts s;
SELECT INTO _rsort string_agg(s.rsort,', ') FROM pgstac_tmp_sorts s;
tok_sort := _sort;


-- Get datetime from query as a tstzrange
IF _search ? 'datetime' THEN
    _dtrange := search_dtrange(_search->'datetime');
    _token_dtrange := _dtrange;
END IF;

-- Get the paging token
IF _search ? 'token' THEN
    token := _search->>'token';
    tok_val := substr(token,6);
    IF starts_with(token, 'prev:') THEN
        is_prev := true;
    END IF;
    SELECT INTO _token_record * FROM items WHERE id=tok_val;
    IF
        (is_prev AND _dtsort = 'DESC')
        OR
        (not is_prev AND _dtsort = 'ASC')
    THEN
        _token_dtrange := _dtrange * tstzrange(_token_record.datetime, 'infinity');
    ELSIF
        _dtsort IS NOT NULL
    THEN
        _token_dtrange := _dtrange * tstzrange('-infinity',_token_record.datetime);
    END IF;
    IF is_prev THEN
        tok_q := filter_by_order(tok_val,  _search->'sortby', 'first');
        _sort := _rsort;
    ELSIF starts_with(token, 'next:') THEN
       tok_q := filter_by_order(tok_val,  _search->'sortby', 'last');
    END IF;
END IF;
RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);
RAISE NOTICE 'tok_q: % _token_dtrange: %', tok_q, _token_dtrange;

IF _search ? 'ids' THEN
    RAISE NOTICE 'searching solely based on ids... %',_search;
    qa := array_append(qa, in_array_q('id', _search->'ids'));
ELSE
    IF _search ? 'intersects' THEN
        _geom := ST_SetSRID(ST_GeomFromGeoJSON(_search->>'intersects'), 4326);
    ELSIF _search ? 'bbox' THEN
        _geom := bbox_geom(_search->'bbox');
    END IF;

    IF _geom IS NOT NULL THEN
        qa := array_append(qa, format('st_intersects(geometry, %L::geometry)',_geom));
    END IF;

    IF _search ? 'collections' THEN
        qa := array_append(qa, in_array_q('collection_id', _search->'collections'));
    END IF;

    IF _search ? 'query' THEN
        qa := array_cat(qa,
            stac_query(_search->'query')
        );
    END IF;
END IF;

IF _search ? 'limit' THEN
    _limit := (_search->>'limit')::int;
END IF;

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
    RAISE NOTICE 'Includes: %, Excludes: %', includes, excludes;
END IF;

whereq := COALESCE(array_to_string(qa,' AND '),' TRUE ');
dq := COALESCE(array_to_string(dqa,' AND '),' TRUE ');
RAISE NOTICE 'timing before temp table: %', age(clock_timestamp(), qstart);

CREATE TEMP TABLE results_page ON COMMIT DROP AS
SELECT * FROM items_by_partition(
    concat(whereq, ' AND ', tok_q),
    _token_dtrange,
    _sort,
    _limit + 1
);
RAISE NOTICE 'timing after temp table: %', age(clock_timestamp(), qstart);

RAISE NOTICE 'timing before min/max: %', age(clock_timestamp(), qstart);

IF is_prev THEN
    SELECT INTO last_id, first_id, counter
        first_value(id) OVER (),
        last_value(id) OVER (),
        count(*) OVER ()
    FROM results_page;
ELSE
    SELECT INTO first_id, last_id, counter
        first_value(id) OVER (),
        last_value(id) OVER (),
        count(*) OVER ()
    FROM results_page;
END IF;
RAISE NOTICE 'firstid: %, lastid %', first_id, last_id;
RAISE NOTICE 'timing after min/max: %', age(clock_timestamp(), qstart);




IF counter > _limit THEN
    next_id := last_id;
    RAISE NOTICE 'next_id: %', next_id;
ELSE
    RAISE NOTICE 'No more next';
END IF;

IF tok_q = 'TRUE' THEN
    RAISE NOTICE 'Not a paging query, no previous item';
ELSE
    RAISE NOTICE 'Getting previous item id';
    RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);
    SELECT INTO _token_record * FROM items WHERE id=first_id;
    IF
        _dtsort = 'DESC'
    THEN
        _token_dtrange := _dtrange * tstzrange(_token_record.datetime, 'infinity');
    ELSE
        _token_dtrange := _dtrange * tstzrange('-infinity',_token_record.datetime);
    END IF;
    RAISE NOTICE '% %', _token_dtrange, _dtrange;
    SELECT id INTO prev_id FROM items_by_partition(
        concat(whereq, ' AND ', filter_by_order(first_id, _search->'sortby', 'prev')),
        _token_dtrange,
        _rsort,
        1
    );
    RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);

    RAISE NOTICE 'prev_id: %', prev_id;
END IF;


RETURN QUERY
WITH features AS (
    SELECT filter_jsonb(content, includes, excludes) as content
    FROM results_page LIMIT _limit
),
j AS (SELECT jsonb_agg(content) as feature_arr FROM features)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce (
        CASE WHEN is_prev THEN flip_jsonb_array(feature_arr) ELSE feature_arr END
        ,'[]'::jsonb),
    'links', links,
    'timeStamp', now(),
    'next', next_id,
    'prev', prev_id
)
FROM j
;


END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;
INSERT INTO migrations (version) VALUES ('0.2.7');
