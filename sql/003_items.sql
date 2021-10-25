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
        WHERE _where IN (
            SELECT _where FROM search_wheres sw WHERE sw.partitions && _partitions
            FOR UPDATE SKIP LOCKED
        )
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
        WHERE _where IN (
            SELECT _where FROM search_wheres sw WHERE sw.partitions && _partitions
            FOR UPDATE SKIP LOCKED
        )
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
        WHERE _where IN (
            SELECT _where FROM search_wheres sw WHERE sw.partitions && _partitions
            FOR UPDATE SKIP LOCKED
        )
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
