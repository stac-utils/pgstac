CREATE TABLE partition_stats (
    partition text PRIMARY KEY,
    dtrange tstzrange,
    edtrange tstzrange,
    spatial geometry,
    last_updated timestamptz,
    keys text[]
) WITH (FILLFACTOR=90);

CREATE INDEX partitions_range_idx ON partition_stats USING GIST(dtrange);


CREATE OR REPLACE FUNCTION constraint_tstzrange(expr text) RETURNS tstzrange AS $$
    WITH t AS (
        SELECT regexp_matches(
            expr,
            E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
        ) AS m
    ) SELECT tstzrange(m[1]::timestamptz, m[2]::timestamptz) FROM t
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION dt_constraint(coid oid, OUT dt tstzrange, OUT edt tstzrange) RETURNS RECORD AS $$
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
$$ LANGUAGE PLPGSQL STABLE STRICT;

CREATE OR REPLACE VIEW partition_sys_meta AS
SELECT
    relid::text as partition,
    replace(replace(CASE WHEN level = 1 THEN pg_get_expr(c.relpartbound, c.oid)
        ELSE pg_get_expr(parent.relpartbound, parent.oid)
    END, 'FOR VALUES IN (''',''), ''')','') AS collection,
    level,
    c.reltuples,
    c.relhastriggers,
    COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity', 'infinity','[]')) as partition_dtrange,
    COALESCE((dt_constraint(edt.oid)).dt, constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), tstzrange('-infinity', 'infinity','[]')) as constraint_dtrange,
    COALESCE((dt_constraint(edt.oid)).edt, tstzrange('-infinity', 'infinity','[]')) as constraint_edtrange
FROM
    pg_partition_tree('items')
    JOIN pg_class c ON (relid::regclass = c.oid)
    JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    LEFT JOIN pg_constraint edt ON (conrelid=c.oid AND contype='c')
WHERE isleaf
;

CREATE VIEW partitions AS
SELECT * FROM partition_sys_meta LEFT JOIN partition_stats USING (partition);

CREATE OR REPLACE FUNCTION update_partition_stats(_partition text) RETURNS VOID AS $$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    extent geometry;
BEGIN
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
$$ LANGUAGE PLPGSQL STRICT;



CREATE OR REPLACE FUNCTION partition_name( IN collection text, IN dt timestamptz, OUT partition_name text, OUT partition_range tstzrange) AS $$
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


CREATE OR REPLACE FUNCTION drop_table_constraints(t text) RETURNS text AS $$
DECLARE
    q text;
BEGIN
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
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION create_table_constraints(t text, _dtrange tstzrange, _edtrange tstzrange) RETURNS text AS $$
DECLARE
    q text;
BEGIN
    RAISE NOTICE 'Creating Table Constraints for % % %', t, _dtrange, _edtrange;
    q :=format(
        $q$
            ALTER TABLE %I
                ADD CONSTRAINT %I
                    CHECK (
                        (datetime >= %L)
                        AND (datetime <= $L)
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
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION check_partition(
    _collection text,
    _dtrange tstzrange,
    _edtrange tstzrange
) RETURNS text AS $$
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
        _partition_dtrange := tstzrange(lower(_dtrange), lower(_dtrange) + ('1' + c.partition_trunc)::interval, '[)');
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
        _constraint_edtrange := _edtrange + pm.edtrange;
        _constraint_dtrange := _dtrange + pm.dtrange;
        IF pm.edtrange @> _edtrange AND pm.dtrange @> _dtrange THEN
            RETURN pm.partition;
        ELSE
            PERFORM drop_table_constraints(_partition_name);
        END IF;
    ELSE
        _constraint_edtrange := _edtrange;
        _constraint_dtrange := _dtrange;
    END IF;
    RAISE NOTICE 'Creating partition %', _partition_name;
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
            format('_items_%s', _collection),
            _collection,
            _partition_name,
            format('_items_%s', _collection),
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
    RETURN _collection;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION repartition(_collection text, _partition_trunc text) RETURNS text AS $$
DECLARE
    c RECORD;
    q text;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;

    IF c.partition_trunc = _partition_trunc THEN
        RAISE NOTICE 'Collection % already set to use partition by %', _collection, _partition_trunc;
    END IF;

    PERFORM format(
        $q$
            CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
            DROP TABLE IF EXISTS %I CASCADE;
            WITH p AS (
                SELECT
                    collection,
                    date_trunc(c.partition_trunc, datetime) as d,
                    tstzrange(min(datetime),max(datetime),'[]') as dtrange,
                    tstzrange(min(datetime),max(datetime),'[]') as edtrange,
                FROM changepartitionstaging
                GROUP BY 1,2
            ) SELECT check_partition(collection, dtrange, edtrange) FROM p;
        $q$
    );
    RETURN _collection;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

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
    IF TG_OP = 'UPDATE' AND NEW.partition_trunc IS DISTINCT FROM OLD.partition_trunc THEN
        PERFORM repartition(NEW.id, NEW.partition_trunc);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER collections_trigger AFTER
INSERT
OR
UPDATE ON collections
FOR EACH ROW EXECUTE FUNCTION collections_trigger_func();
