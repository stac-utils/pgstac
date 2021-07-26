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

-- Bulk insert function which takes a sequence of items as input
CREATE OR REPLACE FUNCTION create_items(datas jsonb) RETURNS VOID AS $$
    INSERT INTO items (content) SELECT * FROM jsonb_array_elements(datas);
    SELECT backfill_partitions();
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
