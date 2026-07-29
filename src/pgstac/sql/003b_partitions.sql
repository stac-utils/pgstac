-- collection and partition_dtrange describe the partition itself and are
-- written synchronously by check_partition. dtrange, edtrange and spatial
-- describe the data in the partition and may be updated asynchronously
-- through the query queue.
CREATE TABLE partition_stats (
    partition text PRIMARY KEY,
    collection text,
    partition_dtrange tstzrange,
    dtrange tstzrange,
    edtrange tstzrange,
    spatial geometry,
    last_updated timestamptz,
    keys text[]
) WITH (FILLFACTOR=90);

CREATE INDEX partition_stats_collection_idx ON partition_stats (collection);


CREATE OR REPLACE FUNCTION constraint_tstzrange(expr text) RETURNS tstzrange AS $$
    WITH t AS (
        SELECT regexp_matches(
            expr,
            E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
        ) AS m
    ) SELECT tstzrange(m[1]::timestamptz, m[2]::timestamptz) FROM t
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION get_tstz_constraint(reloid oid, colname text) RETURNS tstzrange AS $$
DECLARE
    expr text := NULL;
    m text[];
    ts_lower timestamptz := NULL;
    ts_upper timestamptz := NULL;
    lower_inclusive text := '[';
    upper_inclusive text := ']';
    ts timestamptz;
BEGIN
    SELECT INTO expr
        string_agg(def, ' AND ')
    FROM pg_constraint JOIN LATERAL pg_get_constraintdef(oid) AS def ON TRUE
    WHERE
        conrelid = reloid
        AND contype = 'c'
        AND def LIKE '%' || colname || '%'
    ;

    IF expr IS NULL THEN
        RETURN NULL;
    END IF;

    RAISE DEBUG 'Constraint expression for % on %: %', colname, reloid::regclass, expr;
    -- collect all constraints for the specified column
    FOR m IN SELECT regexp_matches(expr, '[ (]' || colname || $expr$\s*([<>=]{1,2})\s*'([0-9 :.+\-]+)'$expr$, 'g') LOOP
        ts := m[2]::timestamptz;
        IF m[1] IN ('>', '>=')
        THEN
            IF ts_lower IS NULL OR ts > ts_lower OR (ts = ts_lower AND m[1] = '>') THEN
                ts_lower := ts;
                lower_inclusive := CASE WHEN m[1] = '>' THEN '(' ELSE '[' END;
            END IF;
        ELSIF m[1] IN ('<', '<=')
        THEN
            IF ts_upper IS NULL OR ts < ts_upper OR (ts = ts_upper AND m[1] = '<') THEN
                ts_upper := ts;
                upper_inclusive := CASE WHEN m[1] = '<' THEN ')' ELSE ']' END;
            END IF;
        END IF;
    END LOOP;
    RAISE DEBUG 'Constraint % for %: % %', colname, reloid::regclass, ts_lower, ts_upper;
    RETURN tstzrange(ts_lower, ts_upper, lower_inclusive || upper_inclusive);
END;
$$ LANGUAGE plpgsql STRICT STABLE;

CREATE OR REPLACE FUNCTION get_partition_name(relid regclass) RETURNS text AS $$
    SELECT (parse_ident(relid::text))[cardinality(parse_ident(relid::text))];
$$ LANGUAGE SQL STABLE STRICT;

-- Resolve a partition name, bare or pgstac qualified, against pgstac rather
-- than the search_path. NULL if there is no such table, or if the name is
-- qualified with a different schema.
CREATE OR REPLACE FUNCTION partition_oid(_partition text) RETURNS oid AS $$
    SELECT CASE
        WHEN cardinality(parts) > 1 AND parts[cardinality(parts) - 1] <> 'pgstac' THEN NULL
        ELSE to_regclass(format('pgstac.%I', parts[cardinality(parts)]))
    END
    FROM parse_ident(_partition) AS parts;
$$ LANGUAGE SQL STABLE STRICT;

-- Catalog metadata for one partition, touching one relation. pg_partition_tree
-- would lock every partition in the tree. No rows unless _partition is a leaf
-- partition of items.
CREATE OR REPLACE FUNCTION partition_catalog_meta(_partition text)
RETURNS TABLE (
    collection text,
    partition_dtrange tstzrange,
    constraint_dtrange tstzrange,
    constraint_edtrange tstzrange
) AS $$
DECLARE
    _oid oid;
    _parent oid;
    _expr text;
    _inf tstzrange := tstzrange('-infinity', 'infinity', '[]');
    _dtrange tstzrange;
BEGIN
    _oid := partition_oid(_partition);
    -- Leaves only, matching partitions_view: an intermediate partition has no
    -- meaningful range of its own.
    IF _oid IS NULL OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = _oid) THEN
        RETURN;
    END IF;

    SELECT inhparent INTO _parent FROM pg_inherits WHERE inhrelid = _oid;

    -- Partitions of items only. The SECURITY DEFINER functions below use this
    -- to decide what they may alter, so any other relation must return nothing.
    IF _parent IS NULL
        OR (
            _parent <> 'pgstac.items'::regclass
            AND NOT EXISTS (
                SELECT 1 FROM pg_inherits
                WHERE inhrelid = _parent AND inhparent = 'pgstac.items'::regclass
            )
        )
    THEN
        RETURN;
    END IF;

    -- A partition of a sub-partitioned collection carries a datetime range
    -- bound; its collection is on the parent. A direct partition of items
    -- carries the collection itself.
    IF _parent = 'pgstac.items'::regclass THEN
        SELECT pg_get_expr(relpartbound, oid) INTO _expr FROM pg_class WHERE oid = _oid;
    ELSE
        SELECT pg_get_expr(relpartbound, oid) INTO _expr FROM pg_class WHERE oid = _parent;
    END IF;

    SELECT COALESCE(
        constraint_tstzrange(pg_get_expr(relpartbound, oid)),
        _inf
    ) INTO _dtrange FROM pg_class WHERE oid = _oid;

    RETURN QUERY SELECT
        replace(replace(_expr, 'FOR VALUES IN (''', ''), ''')', ''),
        _dtrange,
        COALESCE(get_tstz_constraint(_oid, 'datetime'), _dtrange, _inf),
        COALESCE(get_tstz_constraint(_oid, 'end_datetime'), _inf);
END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE VIEW partition_sys_meta AS
SELECT
    partition,
    replace(
        replace(
            CASE WHEN level = 1 THEN partition_expr ELSE parent_partition_expr END,
            'FOR VALUES IN (''',
            ''
        ),
        ''')',
        ''
    ) AS collection,
    level,
    c.reltuples,
    c.relhastriggers,
    partition_dtrange,
    COALESCE(
        get_tstz_constraint(c.oid, 'datetime'),
        partition_dtrange,
        inf_range
    ) as constraint_dtrange,
    COALESCE(
        get_tstz_constraint(c.oid, 'end_datetime'),
        inf_range
    ) as constraint_edtrange
FROM
    pg_partition_tree('items')
    JOIN pg_class c ON (relid::regclass = c.oid)
    JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    JOIN LATERAL get_partition_name(relid) AS partition ON TRUE
    JOIN LATERAL pg_get_expr(c.relpartbound, c.oid) as partition_expr ON TRUE
    JOIN LATERAL pg_get_expr(parent.relpartbound, parent.oid) as parent_partition_expr ON TRUE
    JOIN LATERAL tstzrange('-infinity', 'infinity','[]') as inf_range ON TRUE
    JOIN LATERAL COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), inf_range) as partition_dtrange ON TRUE
WHERE isleaf
;

-- partition_sys_meta plus the statistics tracked alongside each partition.
CREATE OR REPLACE VIEW partitions_view AS
SELECT
    sm.*,
    ps.dtrange,
    ps.edtrange,
    ps.spatial,
    ps.last_updated
FROM partition_sys_meta sm
    LEFT JOIN pgstac.partition_stats ps USING (partition)
;

CREATE VIEW partitions AS
SELECT * FROM partitions_view;


-- Returns TRUE if the stats update ran, FALSE if it was queued.
CREATE OR REPLACE FUNCTION update_partition_stats_q(_partition text, istrigger boolean default false) RETURNS boolean AS $$
DECLARE
BEGIN
    RETURN run_or_queue(
        format('SELECT update_partition_stats(%L, %L);', _partition, istrigger)
    );
END;
$$ LANGUAGE PLPGSQL;

-- _extent NULL follows pgstac.update_collection_extent; TRUE computes the
-- spatial extent regardless, for callers that are about to read it.
CREATE OR REPLACE FUNCTION update_partition_stats(
    _partition text,
    istrigger boolean default false,
    _extent boolean default NULL
) RETURNS VOID AS $$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    cdtrange tstzrange;
    cedtrange tstzrange;
    extent geometry;
    collection text;
    pdtrange tstzrange;
    auto_extent boolean := get_setting_bool('update_collection_extent');
    do_extent boolean := COALESCE(_extent, auto_extent);
BEGIN
    -- Cannot be STRICT: _extent is three valued, and STRICT would skip the
    -- body whenever it is NULL.
    IF _partition IS NULL OR istrigger IS NULL THEN
        RETURN;
    END IF;
    RAISE NOTICE 'Updating stats for %.', _partition;

    SELECT m.collection, m.partition_dtrange, m.constraint_dtrange, m.constraint_edtrange
        INTO collection, pdtrange, cdtrange, cedtrange
    FROM partition_catalog_meta(_partition) m;

    -- A queued update can outlive the partition it names.
    IF NOT FOUND THEN
        RAISE NOTICE 'Partition % no longer exists, skipping stats update.', _partition;
        RETURN;
    END IF;

    -- Taken before the partition is read. The constraint rebuild below needs
    -- ACCESS EXCLUSIVE, and escalating to that mid-transaction deadlocks
    -- against another session doing the same. SHARE UPDATE EXCLUSIVE conflicts
    -- with itself but not with readers.
    IF NOT istrigger THEN
        EXECUTE format('LOCK TABLE %I IN SHARE UPDATE EXCLUSIVE MODE', _partition);
    END IF;

    -- The observed ranges feed the constraint tightening below and collection
    -- extents. When neither will read them, only the partition's identity is
    -- written, which is what keeps it visible to search.
    IF NOT istrigger OR do_extent THEN
        -- st_extent visits every geometry, so it is only run when wanted.
        IF do_extent THEN
            EXECUTE format(
                $q$
                    SELECT
                        tstzrange(min(datetime), max(datetime),'[]'),
                        tstzrange(min(end_datetime), max(end_datetime), '[]'),
                        st_extent(geometry)::geometry
                    FROM %I
                $q$,
                _partition
            ) INTO dtrange, edtrange, extent;
            RAISE DEBUG 'Extent: %', extent;
        ELSE
            EXECUTE format(
                $q$
                    SELECT
                        tstzrange(min(datetime), max(datetime),'[]'),
                        tstzrange(min(end_datetime), max(end_datetime), '[]')
                    FROM %I
                $q$,
                _partition
            ) INTO dtrange, edtrange;
        END IF;

        INSERT INTO partition_stats
            (partition, collection, partition_dtrange, dtrange, edtrange, spatial, last_updated)
            VALUES (_partition, collection, pdtrange, dtrange, edtrange, extent, now())
            ON CONFLICT (partition) DO
                UPDATE SET
                    collection=EXCLUDED.collection,
                    partition_dtrange=EXCLUDED.partition_dtrange,
                    dtrange=EXCLUDED.dtrange,
                    edtrange=EXCLUDED.edtrange,
                    spatial=COALESCE(EXCLUDED.spatial, partition_stats.spatial),
                    last_updated=EXCLUDED.last_updated
        ;
    ELSE
        INSERT INTO partition_stats (partition, collection, partition_dtrange)
            VALUES (_partition, collection, pdtrange)
            ON CONFLICT (partition) DO UPDATE
                SET collection = EXCLUDED.collection,
                    partition_dtrange = EXCLUDED.partition_dtrange
                WHERE
                    partition_stats.collection IS DISTINCT FROM EXCLUDED.collection
                    OR partition_stats.partition_dtrange IS DISTINCT FROM EXCLUDED.partition_dtrange
        ;
    END IF;

    RAISE NOTICE 'Checking if we need to modify constraints...';
    RAISE NOTICE 'cdtrange: % dtrange: % cedtrange: % edtrange: %',cdtrange, dtrange, cedtrange, edtrange;
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
    -- auto_extent, not do_extent: a caller that passed _extent aggregates the
    -- extent itself, and update_collection_extents would then be updating
    -- collections from inside its own UPDATE of collections.
    RAISE NOTICE 'Checking if we need to update collection extents.';
    IF auto_extent THEN
        RAISE NOTICE 'updating collection extent for %', collection;
        PERFORM run_or_queue(format($q$
            UPDATE collections
            SET content = jsonb_set_lax(
                content,
                '{extent}'::text[],
                collection_extent(%L, FALSE),
                true,
                'use_json_null'
            ) WHERE id=%L
            ;
        $q$, collection, collection));
    ELSE
        RAISE NOTICE 'Not updating collection extent for %', collection;
    END IF;

END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;



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
    _oid oid := partition_oid(t);
BEGIN
    IF _oid IS NULL THEN
        RETURN NULL;
    END IF;
    -- Only partitions of items. This runs elevated, so without the check it
    -- would alter any table pgstac_admin owns that the caller names.
    IF NOT EXISTS (SELECT 1 FROM partition_catalog_meta(t)) THEN
        RETURN NULL;
    END IF;
    -- Reduce to the bare name so the ALTER statements below quote it correctly
    -- even when the caller passed a schema qualified name.
    t := get_partition_name(_oid);
    FOR q IN SELECT FORMAT(
        $q$
            ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        $q$,
        t,
        conname
    ) FROM pg_constraint
        WHERE conrelid=_oid AND contype='c'
    LOOP
        EXECUTE q;
    END LOOP;
    RETURN t;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION create_table_constraints(t text, _dtrange tstzrange, _edtrange tstzrange) RETURNS text AS $$
DECLARE
    q text;
    _oid oid := partition_oid(t);
BEGIN
    IF _oid IS NULL THEN
        RETURN NULL;
    END IF;
    -- Only partitions of items. This runs elevated, so without the check it
    -- would alter any table pgstac_admin owns that the caller names.
    IF NOT EXISTS (SELECT 1 FROM partition_catalog_meta(t)) THEN
        RETURN NULL;
    END IF;
    -- Reduce to the bare name so the ALTER statements below quote it correctly
    -- even when the caller passed a schema qualified name.
    t := get_partition_name(_oid);
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
    -- Run, not queued: the queue runner is not a definer, so queued DDL
    -- executes as whoever drains it. Defer by queueing a call to this function.
    EXECUTE q;
    RETURN t;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


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

    -- Constraint ranges are maintained asynchronously, so they come from the
    -- catalog rather than partition_stats.
    SELECT ps.partition, m.constraint_dtrange, m.constraint_edtrange
        INTO pm
    FROM partition_stats ps
        JOIN LATERAL partition_catalog_meta(ps.partition) m ON TRUE
    WHERE ps.collection = _collection AND ps.partition_dtrange @> _dtrange
    LIMIT 1;
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
    RAISE NOTICE 'EXISTING CONSTRAINTS % %, NEW % %', pm.constraint_dtrange, pm.constraint_edtrange, _constraint_dtrange, _constraint_edtrange;
    RAISE NOTICE 'Creating partition % %', _partition_name, _partition_dtrange;
    IF c.partition_trunc IS NULL THEN
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                GRANT ALL ON %I to pgstac_ingest;
            $q$,
            _partition_name,
            _collection,
            concat(_partition_name,'_pk'),
            _partition_name,
            _partition_name
        );
    ELSE
        q := format(
            $q$
                CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L) PARTITION BY RANGE (datetime);
                CREATE TABLE IF NOT EXISTS %I partition OF %I FOR VALUES FROM (%L) TO (%L);
                CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                GRANT ALL ON %I TO pgstac_ingest;
            $q$,
            format('_items_%s', c.key),
            _collection,
            _partition_name,
            format('_items_%s', c.key),
            lower(_partition_dtrange),
            upper(_partition_dtrange),
            format('%s_pk', _partition_name),
            _partition_name,
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
    -- The _constraint_ ranges are the union of the existing constraint and the
    -- incoming batch, so they hold for rows already present as well as the ones
    -- about to be added. Queueable: rebuilding validates the partition under an
    -- ACCESS EXCLUSIVE lock.
    PERFORM run_or_queue(format(
        'SELECT create_table_constraints(%L, %L, %L);',
        _partition_name,
        _constraint_dtrange,
        _constraint_edtrange
    ));
    PERFORM maintain_partitions(_partition_name);
    -- Search finds partitions through partition_stats, so the row has to exist
    -- before this transaction commits. A queued stats update has not written it.
    IF NOT update_partition_stats_q(_partition_name, true) THEN
        INSERT INTO partition_stats (partition, collection, partition_dtrange)
            VALUES (_partition_name, _collection, _partition_dtrange)
            ON CONFLICT (partition) DO UPDATE
                SET collection = EXCLUDED.collection,
                    partition_dtrange = EXCLUDED.partition_dtrange
                WHERE
                    partition_stats.collection IS DISTINCT FROM EXCLUDED.collection
                    OR partition_stats.partition_dtrange IS DISTINCT FROM EXCLUDED.partition_dtrange
        ;
    END IF;
    RETURN _partition_name;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


CREATE OR REPLACE FUNCTION repartition(_collection text, _partition_trunc text, triggered boolean DEFAULT FALSE) RETURNS text AS $$
DECLARE
    c RECORD;
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

    IF EXISTS (SELECT 1 FROM partitions_view WHERE collection=_collection LIMIT 1) THEN
        EXECUTE format(
            $q$
                CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
                DROP TABLE IF EXISTS %I CASCADE;
                DELETE FROM partition_stats WHERE collection = %L;
                WITH p AS (
                    SELECT
                        collection,
                        CASE
                            WHEN %L IS NULL THEN '-infinity'::timestamptz
                            ELSE date_trunc(%L, datetime)
                        END as d,
                        tstzrange(min(datetime),max(datetime),'[]') as dtrange,
                        tstzrange(min(end_datetime),max(end_datetime),'[]') as edtrange
                    FROM changepartitionstaging
                    GROUP BY 1,2
                ) SELECT check_partition(collection, dtrange, edtrange) FROM p;
                INSERT INTO items SELECT * FROM changepartitionstaging;
                DROP TABLE changepartitionstaging;
            $q$,
            concat('_items_', c.key),
            concat('_items_', c.key),
            _collection,
            c.partition_trunc,
            c.partition_trunc
        );
    END IF;
    RETURN _collection;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

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
        PERFORM repartition(NEW.id, NEW.partition_trunc, TRUE);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER collections_trigger AFTER
INSERT
OR
UPDATE ON collections
FOR EACH ROW EXECUTE FUNCTION collections_trigger_func();
