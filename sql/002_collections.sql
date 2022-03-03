


CREATE OR REPLACE FUNCTION collection_base_item(content jsonb) RETURNS jsonb AS $$
    SELECT jsonb_build_object(
        'type', 'Feature',
        'stac_version', content->'stac_version',
        'stac_extensions', content->'stac_extensions',
        'links', content->'links',
        'assets', content->'item_assets',
        'collection', content->'id'
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TYPE partition_trunc_strategy AS ENUM ('year', 'month');

CREATE TABLE collections (
    key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id text GENERATED ALWAYS AS (content->>'id') STORED UNIQUE,
    content JSONB NOT NULL,
    base_item jsonb GENERATED ALWAYS AS (collection_base_item(content)) STORED,
    partition_trunc partition_trunc_strategy
);


CREATE OR REPLACE FUNCTION collection_base_item(cid text) RETURNS jsonb AS $$
    SELECT collection_base_item(content) FROM collections WHERE id = cid LIMIT 1;
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
BEGIN
    RAISE NOTICE 'Collection Trigger. % % %', NEW.id, NEW.key, NEW.content;
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
            DROP TABLE IF EXISTS %I;
            $q$,
            partition_name
        );
        EXECUTE q;
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
        INSERT INTO partitions (collection, name) VALUES (NEW.id, partition_name);
        RETURN NEW;
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
        INSERT INTO partitions (collection, name) VALUES (NEW.id, partition_name);
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'Cannot modify partition % unless empty', partition_name;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER collections_trigger AFTER INSERT OR UPDATE ON collections FOR EACH ROW
EXECUTE FUNCTION collections_trigger_func();



CREATE TABLE partitions (
    collection text REFERENCES collections(id),
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
    SELECT * INTO c FROM collections WHERE id=collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', collection;
    END IF;
    parent_name := format('_items_%s', c.key);


    IF c.partition_trunc = 'year' THEN
        partition_name := format('%s_%s', parent_name, to_char(dt,'YYYY'));
    ELSIF c.partition_trunc = 'month' THEN
        partition_name := format('%s_%s', parent_name, to_char(dt,'YYYYMM'));
    ELSE
        partition_name := parent_name;
        partition_range := tstzrange('-infinity'::timestamptz, 'infinity'::timestamptz, '[]');
        RETURN;
    END IF;
    partition_range := tstzrange(
        date_trunc(c.partition_trunc::text, dt),
        date_trunc(c.partition_trunc::text, dt) + concat('1 ', c.partition_trunc)::interval
    );
    RETURN;

END;
$$ LANGUAGE PLPGSQL STABLE;



CREATE OR REPLACE FUNCTION partitions_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    q text;
    cq text;
    parent_name text;
    partition_name text;
    partition_exists boolean := false;
    partition_empty boolean := true;
    partition_range tstzrange;
    datetime_range tstzrange;
    end_datetime_range tstzrange;
    err_context text;
BEGIN
    datetime_range := NEW.datetime_range;
    end_datetime_range := NEW.end_datetime_range;
    SELECT format('_items_%s', key) INTO parent_name FROM collections WHERE collections.id = NEW.collection;

    SELECT (partition_name(NEW.collection, lower(datetime_range))).* INTO partition_name, partition_range;
    NEW.name := partition_name;
    NEW.partition_range := partition_range;

    IF TG_OP = 'UPDATE' AND upper(NEW.end_datetime_range) <= upper(OLD.end_datetime_range) THEN
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN

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

    -- Update constraints if needed
    IF partition_range != tstzrange('-infinity'::timestamptz, 'infinity'::timestamptz, '[]') THEN
        cq := format($q$
            ALTER TABLE %I
                DROP CONSTRAINT IF EXISTS %I,
                ADD CONSTRAINT %I
                CHECK ((end_datetime >= %L) AND (end_datetime <= %L)) NOT VALID;
            ALTER TABLE %I
                VALIDATE CONSTRAINT %I;
            $q$,
            partition_name,
            format('%s_edt', partition_name),
            format('%s_edt', partition_name),
            lower(partition_range),
            greatest(upper(partition_range), upper(end_datetime_range)),
            partition_name,
            format('%s_edt', partition_name)
        );
        EXECUTE cq;
    END IF;

    RETURN NEW;

END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER partitions_trigger BEFORE INSERT OR UPDATE ON partitions FOR EACH ROW
EXECUTE FUNCTION partitions_trigger_func();

/*
---------------------------
TRUNCATE pgstac.collections CASCADE;
INSERT INTO pgstac.collections (content) SELECT content FROM pgstac_test.collections;
UPDATE pgstac.collections SET partition_trunc='year' WHERE id IN ('aster-l1t', 'landsat-8-c2-l2', 'naip');
UPDATE pgstac.collections SET partition_trunc='month' WHERE id IN ('goes-cmi','sentinel-2-l2a');



-----------------------------

*/



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
