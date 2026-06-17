-- partition_stats: the single source of per-partition metadata, used both for
-- maintenance and as the search-discovery index (chunker() reads it directly). Holds
-- the partition boundary (partition_dtrange) plus the actual data extents (dtrange,
-- edtrange, spatial) and a row estimate (n). NULL data extents mean a freshly created,
-- not-yet-analyzed partition (treated as unbounded by discovery).
CREATE TABLE partition_stats (
    partition text PRIMARY KEY,
    collection text,
    partition_dtrange tstzrange,
    dtrange tstzrange,
    edtrange tstzrange,
    spatial geometry,
    last_updated timestamptz,
    n bigint,
    keys text[]
) WITH (FILLFACTOR=90);

CREATE INDEX partitions_range_idx ON partition_stats USING GIST(dtrange);
CREATE INDEX partition_stats_collection_idx ON partition_stats (collection);
CREATE INDEX partition_stats_spatial_idx ON partition_stats USING GIST(spatial) WHERE spatial IS NOT NULL;


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
    LEFT JOIN pg_constraint edt ON (conrelid=c.oid AND contype='c')
    JOIN LATERAL get_partition_name(relid) AS partition ON TRUE
    JOIN LATERAL pg_get_expr(c.relpartbound, c.oid) as partition_expr ON TRUE
    JOIN LATERAL pg_get_expr(parent.relpartbound, parent.oid) as parent_partition_expr ON TRUE
    JOIN LATERAL tstzrange('-infinity', 'infinity','[]') as inf_range ON TRUE
    JOIN LATERAL COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), inf_range) as partition_dtrange ON TRUE
    JOIN LATERAL get_tstz_constraint(c.oid, 'datetime') as datetime_constraint ON TRUE
    JOIN LATERAL get_tstz_constraint(c.oid, 'end_datetime') as end_datetime_constraint ON TRUE
WHERE isleaf
;

CREATE OR REPLACE VIEW partitions_view AS
SELECT
    (parse_ident(relid::text))[cardinality(parse_ident(relid::text))] as partition,
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
    partboundary AS partition_dtrange,
    COALESCE(
        get_tstz_constraint(c.oid, 'datetime'),
        partboundary,
        inf_range
    ) as constraint_dtrange,
    COALESCE(
        get_tstz_constraint(c.oid, 'end_datetime'),
        inf_range
    ) as constraint_edtrange,
    dtrange,
    edtrange,
    spatial,
    last_updated
FROM
    pg_partition_tree('items')
    JOIN pg_class c ON (relid::regclass = c.oid)
    JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    LEFT JOIN pg_constraint edt ON (conrelid=c.oid AND contype='c')
    JOIN LATERAL get_partition_name(relid) AS partition ON TRUE
    JOIN LATERAL pg_get_expr(c.relpartbound, c.oid) as partition_expr ON TRUE
    JOIN LATERAL pg_get_expr(parent.relpartbound, parent.oid) as parent_partition_expr ON TRUE
    JOIN LATERAL tstzrange('-infinity', 'infinity','[]') as inf_range ON TRUE
    JOIN LATERAL COALESCE(constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)), inf_range) as partboundary ON TRUE
    JOIN LATERAL get_tstz_constraint(c.oid, 'datetime') as datetime_constraint ON TRUE
    JOIN LATERAL get_tstz_constraint(c.oid, 'end_datetime') as end_datetime_constraint ON TRUE
    LEFT JOIN pgstac.partition_stats USING (partition)
WHERE isleaf
;

-- Backfill / self-heal the boundary metadata (collection, partition_dtrange, n) in
-- partition_stats from the authoritative live partition tree, and drop rows whose
-- partition no longer exists. The data extents (dtrange/edtrange/spatial) are filled
-- separately by update_partition_stats() after ANALYZE. Cheap (one pass).
CREATE OR REPLACE FUNCTION sync_partition_stats() RETURNS bigint AS $$
    WITH upserted AS (
        INSERT INTO partition_stats AS ps (partition, collection, partition_dtrange, n)
        SELECT
            p.partition, p.collection, p.partition_dtrange,
            (SELECT reltuples::bigint FROM pg_class WHERE oid = quote_ident(p.partition)::regclass)
        FROM partition_sys_meta p
        ON CONFLICT (partition) DO UPDATE SET
            collection = EXCLUDED.collection,
            partition_dtrange = EXCLUDED.partition_dtrange,
            n = COALESCE(EXCLUDED.n, ps.n)
        RETURNING ps.partition
    ),
    deleted AS (
        DELETE FROM partition_stats d
        WHERE NOT EXISTS (SELECT 1 FROM partition_sys_meta m WHERE m.partition = d.partition)
        RETURNING 1
    )
    SELECT (SELECT count(*) FROM upserted);
$$ LANGUAGE SQL SECURITY DEFINER SET search_path TO pgstac, public;


CREATE OR REPLACE FUNCTION update_partition_stats_q(_partition text, istrigger boolean default false) RETURNS VOID AS $$
DECLARE
BEGIN
    PERFORM run_or_queue(
        format('SELECT update_partition_stats(%L, %L);', _partition, istrigger)
    );
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION update_partition_stats(_partition text, istrigger boolean default false) RETURNS VOID AS $$
DECLARE
    dtrange tstzrange;
    edtrange tstzrange;
    extent geometry;
    collection text;
    _part_dtrange tstzrange;
    _reltuples bigint;
BEGIN
    RAISE NOTICE 'Updating stats for %.', _partition;
    EXECUTE format(
        $q$
            SELECT
                tstzrange(min(datetime), max(datetime),'[]'),
                tstzrange(min(end_datetime), max(end_datetime), '[]')
            FROM %I
        $q$,
        _partition
    ) INTO dtrange, edtrange;
    EXECUTE format('ANALYZE %I;', _partition);
    extent := st_estimatedextent('pgstac', _partition, 'geometry');
    RAISE DEBUG 'Estimated Extent: %', extent;

    SELECT pv.collection, pv.partition_dtrange INTO collection, _part_dtrange
    FROM partitions_view pv WHERE partition = _partition;
    _reltuples := (SELECT reltuples::bigint FROM pg_class WHERE oid = quote_ident(_partition)::regclass);

    -- partition_stats is the single source: write the boundary + data extents + row
    -- estimate together (row lock only, no MV refresh).
    INSERT INTO partition_stats
        (partition, collection, partition_dtrange, dtrange, edtrange, spatial, n, last_updated)
        SELECT _partition, collection, _part_dtrange, dtrange, edtrange, extent, _reltuples, now()
        ON CONFLICT (partition) DO
            UPDATE SET
                collection=EXCLUDED.collection,
                partition_dtrange=EXCLUDED.partition_dtrange,
                dtrange=EXCLUDED.dtrange,
                edtrange=EXCLUDED.edtrange,
                spatial=EXCLUDED.spatial,
                n=EXCLUDED.n,
                last_updated=EXCLUDED.last_updated
    ;

    IF get_setting_bool('update_collection_extent') THEN
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
$$ LANGUAGE PLPGSQL STRICT SECURITY DEFINER;


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
    IF NOT EXISTS (SELECT 1 FROM partitions_view WHERE partition=t) THEN
        RETURN NULL;
    END IF;
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
    IF NOT EXISTS (SELECT 1 FROM partitions_view WHERE partition=t) THEN
        RETURN NULL;
    END IF;
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

    SELECT * INTO pm FROM partition_sys_meta WHERE collection=_collection AND partition_dtrange @> _dtrange;
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
    -- Register the partition_stats row immediately (boundary known; data extents filled
    -- when stats run). Ensures chunker() sees the partition even before stats queue.
    INSERT INTO partition_stats (partition, collection, partition_dtrange)
        VALUES (_partition_name, _collection, _partition_dtrange)
        ON CONFLICT (partition) DO UPDATE SET
            collection = EXCLUDED.collection,
            partition_dtrange = EXCLUDED.partition_dtrange;
    PERFORM maintain_partitions(_partition_name);
    PERFORM update_partition_stats_q(_partition_name, true);
    RETURN _partition_name;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION repartition(_collection text, _partition_trunc text, triggered boolean DEFAULT FALSE) RETURNS text AS $$
DECLARE
    c RECORD;
    q text;
    from_trunc text;
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
        -- The collection's partition tree is about to be dropped & rebuilt; clear its
        -- stale partition_stats rows (check_partition re-adds them as leaves recreate).
        DELETE FROM partition_stats WHERE collection=_collection;
        EXECUTE format(
            $q$
                CREATE TEMP TABLE changepartitionstaging ON COMMIT DROP AS SELECT * FROM %I;
                DROP TABLE IF EXISTS %I CASCADE;
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
            c.partition_trunc,
            c.partition_trunc
        );
    END IF;
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


-- partition_sync_meta: the CHEAP, metadata-first sync signal. For each partition of a
-- collection it returns the boundary range, a row-count estimate, and last_updated --
-- the time the partition's stats were last recomputed (which happens whenever items are
-- loaded into it). Read straight from partition_stats, no item scan. Sync compares
-- last_updated (and the count) to decide what changed; the exact content hash below is
-- used only as a fallback to prove equality when the metadata is ambiguous, so hashing
-- millions of rows never sits on the common sync path.
CREATE OR REPLACE FUNCTION partition_sync_meta(_collection text)
RETURNS TABLE(partition text, dtrange tstzrange, item_count bigint, last_updated timestamptz) AS $$
    SELECT partition, partition_dtrange, n, last_updated
    FROM partition_stats
    WHERE collection = _collection
    ORDER BY partition_dtrange;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

-- partition_hashes: exact per-partition content fingerprint -- the FALLBACK that proves
-- whether a partition really changed when partition_sync_meta() is ambiguous (e.g.
-- last_updated differs but the content may be identical). content_hash = sha256 of the
-- comma-joined hex item_hashes ordered by id; empty partition -> sha256 of the empty
-- string. O(n) per partition, so callers should gate it behind the cheap metadata
-- check rather than run it on every sync. Sync/maintenance op, not a hot path.
--
-- Membership is by datetime range (i.datetime <@ p.partition_dtrange), which mirrors
-- how items are physically routed and lets the planner prune to the matching
-- partition. Caveat: an item with a NULL datetime (only possible in a collection that
-- is LIST-partitioned by collection without datetime sub-partitioning) does not match
-- any range and is therefore not counted/hashed; such items are outside the STAC
-- temporal model. A future variant could group by physical partition (i.tableoid) if
-- NULL-datetime items must be fingerprinted.
CREATE OR REPLACE FUNCTION partition_hashes(_collection text)
RETURNS TABLE(partition text, dtrange tstzrange, item_count bigint, content_hash text) AS $$
    SELECT
        p.partition,
        p.partition_dtrange,
        count(i.id),
        encode(sha256(convert_to(
            coalesce(string_agg(encode(i.item_hash, 'hex'), ',' ORDER BY i.id), ''),
            'UTF8')), 'hex')
    FROM partition_sys_meta p
    LEFT JOIN items i
        ON i.collection = p.collection
       AND i.datetime <@ p.partition_dtrange
    WHERE p.collection = _collection
    GROUP BY p.partition, p.partition_dtrange
    ORDER BY p.partition_dtrange;
$$ LANGUAGE sql STABLE PARALLEL SAFE;
