


CREATE OR REPLACE FUNCTION collection_base_item(content jsonb) RETURNS jsonb AS $$
    SELECT jsonb_build_object(
        'type', 'Feature',
        'stac_version', content->'stac_version',
        'assets', content->'item_assets',
        'collection', content->'id',
        'links', '[]'::jsonb
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
        DELETE FROM partitions WHERE collection=NEW.id AND name=partition_name;
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
        DELETE FROM partitions WHERE collection=NEW.id AND name=partition_name;
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'Cannot modify partition % unless empty', partition_name;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collections_trigger AFTER INSERT OR UPDATE ON collections FOR EACH ROW
EXECUTE FUNCTION collections_trigger_func();



CREATE TABLE IF NOT EXISTS partitions (
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
    SELECT * INTO c FROM pgstac.collections WHERE id=collection;
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
    RAISE NOTICE 'I % % % % % % % %', mindt, t_mindt, maxdt, t_maxdt, minedt, t_minedt, maxedt, t_maxedt;
    datetime_range := NEW.datetime_range;
    end_datetime_range := NEW.end_datetime_range;

    SELECT format('_items_%s', key) INTO parent_name FROM pgstac.collections WHERE collections.id = NEW.collection;
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
    RAISE NOTICE 'U % % % % % % % %', mindt, t_mindt, maxdt, t_maxdt, minedt, t_minedt, maxedt, t_maxedt;
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
    minedt := least(minedt, t_minedt);
    maxedt := greatest(maxedt, t_maxedt);
    RAISE NOTICE 'F % % % % % % % %', mindt, t_mindt, maxdt, t_maxdt, minedt, t_minedt, maxedt, t_maxedt;

    IF mindt IS NOT NULL AND maxdt IS NOT NULL AND minedt IS NOT NULL AND maxedt IS NOT NULL THEN
        NEW.datetime_range := tstzrange(mindt, maxdt, '[]');
        NEW.end_datetime_range := tstzrange(minedt, maxedt, '[]');
        IF
            TG_OP='UPDATE'
            AND OLD.datetime_range @> NEW.datetime_range
            AND OLD.end_datetime_range @> NEW.end_datetime_range THEN
            RAISE NOTICE 'Range unchanged, not updating constraints.';
        ELSE
            cq := format($q$
                ALTER TABLE %7$I
                    DROP CONSTRAINT IF EXISTS %1$I,
                    DROP CONSTRAINT IF EXISTS %2$I,
                    ADD CONSTRAINT %1$I
                        CHECK ((datetime >= %3$L) AND (datetime <= %4$L)) NOT VALID,
                    ADD CONSTRAINT %2$I
                        CHECK ((end_datetime >= %5$L) AND (end_datetime <= %6$L)) NOT VALID
                ;
                ALTER TABLE %7$I
                    VALIDATE CONSTRAINT %1$I;
                ALTER TABLE %7$I
                    VALIDATE CONSTRAINT %2$I;
                $q$,
                format('%s_dt', partition_name),
                format('%s_edt', partition_name),
                date_trunc('month', mindt),
                date_trunc('month', maxdt) + '1 month'::interval,
                date_trunc('month', minedt),
                date_trunc('month', maxedt) + '1 month'::interval,
                partition_name
            );
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
                    CHECK ((datetime IS NULL)) NOT VALID,
                ADD CONSTRAINT %2$I
                    CHECK ((end_datetime IS NULL)) NOT VALID
            ;
            ALTER TABLE %3$I
                VALIDATE CONSTRAINT %1$I;
            ALTER TABLE %3$I
                VALIDATE CONSTRAINT %2$I;
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
