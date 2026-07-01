CREATE TABLE partition_stats (
    partition text PRIMARY KEY,
    collection text,
    -- dtrange/edtrange are the data bounds used for search pruning. widen_partition_stats fills them
    -- (generously) on ingest; tighten_partition_stats narrows them to the exact extent. The partition's
    -- structural range lives in its own constraint, read via partitions_view.constraint_dtrange.
    dtrange tstzrange,
    edtrange tstzrange,
    spatial geometry,
    last_updated timestamptz,
    n bigint,
    -- Deferred-maintenance flags:
    --   dirty:           the stored envelope may be WIDER than the actual data; an async tightener
    --                    should recompute exact min/max/extent and clear the flag. Search correctness
    --                    never depends on this (a wide envelope only over-includes a partition).
    --   indexes_pending: the partition currently carries only the parent-inherited indexes (id PK,
    --                    datetime, geometry); its queryable-defined indexes have not been built yet.
    dirty boolean NOT NULL DEFAULT false,
    indexes_pending boolean NOT NULL DEFAULT false
) WITH (FILLFACTOR=70);

CREATE INDEX partitions_range_idx ON partition_stats USING GIST(dtrange);
CREATE INDEX partition_stats_collection_idx ON partition_stats (collection);
CREATE INDEX partition_stats_spatial_idx ON partition_stats USING GIST(spatial) WHERE spatial IS NOT NULL;
-- Work-queue indexes: the maintenance sweeps find pending partitions without scanning every row.
CREATE INDEX partition_stats_dirty_idx ON partition_stats (partition) WHERE dirty;
CREATE INDEX partition_stats_indexes_pending_idx ON partition_stats (partition) WHERE indexes_pending;


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
    COALESCE(
        get_tstz_constraint(c.oid, 'datetime'),
        constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)),
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
    COALESCE(
        get_tstz_constraint(c.oid, 'datetime'),
        constraint_tstzrange(pg_get_expr(c.relpartbound, c.oid)),
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
    JOIN LATERAL get_tstz_constraint(c.oid, 'datetime') as datetime_constraint ON TRUE
    JOIN LATERAL get_tstz_constraint(c.oid, 'end_datetime') as end_datetime_constraint ON TRUE
    -- the view computes its own collection + constraint_dtrange from the live tree; pull only the
    -- data-extent columns from partition_stats to avoid colliding with those names.
    LEFT JOIN (
        SELECT partition, dtrange, edtrange, spatial, last_updated
        FROM pgstac.partition_stats
    ) ps USING (partition)
WHERE isleaf
;



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


-- Partitions carry no _dt CHECK constraints; pruning is partition_stats-driven. get_tstz_constraint /
-- constraint_tstzrange remain so the views' constraint_* columns resolve to inf_range when a partition
-- has no datetime constraint.


-- widen_partition_stats: widen a partition's stored envelope to cover a batch. A no-op (snapshot read, no
-- write, no lock) when the envelope already covers the batch; otherwise widens and sets dirty=true:
--   * datetime    : sub-partitioned collections widen to the partition's datetime bound (covers anything
--                   that can land there); a NULL-partition_trunc partition pads the batch range by
--                   partition_stats_widen_buffer (default 1 month) each side.
--   * end_datetime: the datetime target extended by the batch's max (end_datetime - datetime) tail.
--   * spatial     : NULL means "always a search candidate"; a spatial miss resets spatial to NULL until
--                   the tightener computes the real extent.
-- Requires the partition_stats row to exist (check_partition seeds it); raises if it does not.
CREATE OR REPLACE FUNCTION widen_partition_stats(
    _partition text,
    _dtrange tstzrange,
    _edtrange tstzrange,
    _spatial geometry DEFAULT NULL,
    _constraint_dtrange tstzrange DEFAULT NULL
) RETURNS void AS $$
DECLARE
    cur RECORD;
    is_unbounded boolean;
    tail interval;
    buf interval := COALESCE(get_setting('partition_stats_widen_buffer'), '1 month')::interval;
    target_dtrange tstzrange;
    target_edtrange tstzrange;
    new_dtrange tstzrange;
    new_edtrange tstzrange;
    new_spatial geometry;
    dt_covered boolean;
    edt_covered boolean;
    spatial_covered boolean;
BEGIN
    SELECT dtrange, edtrange, spatial
        INTO cur
        FROM partition_stats WHERE partition = _partition;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'partition_stats row for % does not exist; call check_partition first', _partition;
    END IF;

    dt_covered  := COALESCE(cur.dtrange  @> _dtrange,  false);
    edt_covered := COALESCE(cur.edtrange @> _edtrange, false);
    spatial_covered := cur.spatial IS NULL OR _spatial IS NULL OR ST_Covers(cur.spatial, _spatial);
    IF dt_covered AND edt_covered AND spatial_covered THEN
        RETURN; -- already covered: no write, no lock
    END IF;

    -- Clamp the widen to the partition's structural bound (_constraint_dtrange). A sub-partitioned
    -- (month/year) partition's bound is finite: cover the whole partition so dtrange never misses and
    -- never spills into a neighbour. An unbounded partition (NULL-partition_trunc or NULL bound) has
    -- nothing to clamp to, so pad the batch range by the widen buffer each side.
    is_unbounded := _constraint_dtrange IS NULL
                 OR (lower(_constraint_dtrange) = '-infinity'::timestamptz
                     AND upper(_constraint_dtrange) = 'infinity'::timestamptz);
    tail := GREATEST(upper(_edtrange) - upper(_dtrange), '0'::interval);
    IF is_unbounded THEN
        target_dtrange  := tstzrange(lower(_dtrange) - buf, upper(_dtrange) + buf, '[]');
        target_edtrange := tstzrange(lower(_dtrange) - buf, upper(_edtrange) + buf, '[]');
    ELSE
        target_dtrange  := _constraint_dtrange;
        target_edtrange := tstzrange(lower(_constraint_dtrange), upper(_constraint_dtrange) + tail, '[]');
    END IF;

    new_dtrange  := range_merge(COALESCE(cur.dtrange,  target_dtrange),  target_dtrange);
    new_edtrange := range_merge(COALESCE(cur.edtrange, target_edtrange), target_edtrange);
    new_spatial  := CASE WHEN spatial_covered THEN cur.spatial ELSE NULL END;

    UPDATE partition_stats
        SET dtrange = new_dtrange,
            edtrange = new_edtrange,
            spatial = new_spatial,
            dirty = true,
            last_updated = now()
        WHERE partition = _partition;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE FUNCTION check_partition(
    _collection text,
    _dtrange tstzrange,
    _edtrange tstzrange,
    _spatial geometry DEFAULT NULL
) RETURNS text AS $$
DECLARE
    c RECORD;
    _partition_name text;
    _parent_name text;
    _partition_range tstzrange;
BEGIN
    SELECT * INTO c FROM pgstac.collections WHERE id=_collection;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _collection USING ERRCODE = 'foreign_key_violation', HINT = 'Make sure collection exists before adding items';
    END IF;

    IF c.partition_trunc IS NOT NULL THEN
        _partition_range := tstzrange(
            date_trunc(c.partition_trunc, lower(_dtrange)),
            date_trunc(c.partition_trunc, lower(_dtrange)) + (concat('1 ', c.partition_trunc))::interval,
            '[)'
        );
    ELSE
        _partition_range := '[-infinity, infinity]'::tstzrange;
    END IF;

    IF NOT _partition_range @> _dtrange THEN
        RAISE EXCEPTION 'dtrange % spans more than the % partition window for collection %', _dtrange, c.partition_trunc, _collection;
    END IF;

    _parent_name := format('_items_%s', c.key);
    IF c.partition_trunc = 'year' THEN
        _partition_name := format('%s_%s', _parent_name, to_char(lower(_partition_range),'YYYY'));
    ELSIF c.partition_trunc = 'month' THEN
        _partition_name := format('%s_%s', _parent_name, to_char(lower(_partition_range),'YYYYMM'));
    ELSE
        _partition_name := _parent_name;
    END IF;

    -- Create the collection-level PARENT partition (_items_<key>) first, for sub-partitioned collections.
    -- It is shared across every child window, so concurrent setup of different children would race on its
    -- CREATE TABLE. Guard it with a parent-scoped advisory lock, taken before any child lock and only when
    -- the parent is missing: parent-before-child is one lock order (parent < child) that can't deadlock
    -- with ensure_partitions' sorted child locks, and skipping it once the parent exists keeps steady-state
    -- ingest off the parent lock.
    IF c.partition_trunc IS NOT NULL AND to_regclass(format('pgstac.%I', _parent_name)) IS NULL THEN
        PERFORM pg_advisory_xact_lock(hashtext('pgstac.check_partition'), hashtext(_parent_name));
        IF to_regclass(format('pgstac.%I', _parent_name)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L) PARTITION BY RANGE (datetime)',
                _parent_name, _collection
            );
        END IF;
    END IF;

    -- Serialize concurrent setup of THIS (leaf) partition with a partition-scoped advisory lock. Different
    -- partitions hash to different keys, so this never serializes unrelated ingest; ensure_partitions
    -- acquires these in sorted order, so concurrent multi-partition batches cannot deadlock. The lock
    -- releases at commit (setup is fast + idempotent).
    PERFORM pg_advisory_xact_lock(hashtext('pgstac.check_partition'), hashtext(_partition_name));

    -- Create the leaf partition if missing. Parent-inherited indexes only (id PK here; datetime/geometry
    -- come from the items parent); no CHECK constraints; queryable indexes are deferred via
    -- indexes_pending. A SELECT grant lets read/ingest query it; writes reach it only through the
    -- SECURITY DEFINER write functions (the privilege wall in 998_idempotent_post).
    -- Skip the DDL when it already exists: re-running CREATE/GRANT takes a relation lock that deadlocks
    -- with concurrent INSERTs. The existence check is race-safe under the advisory lock.
    IF to_regclass(format('pgstac.%I', _partition_name)) IS NULL THEN
        BEGIN
            IF c.partition_trunc IS NULL THEN
                EXECUTE format(
                    $q$
                        CREATE TABLE IF NOT EXISTS %I partition OF items FOR VALUES IN (%L);
                        CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                        GRANT SELECT ON %I TO pgstac_read, pgstac_ingest;
                    $q$,
                    _partition_name, _collection,
                    concat(_partition_name, '_pk'), _partition_name,
                    _partition_name
                );
            ELSE
                EXECUTE format(
                    $q$
                        CREATE TABLE IF NOT EXISTS %I partition OF %I FOR VALUES FROM (%L) TO (%L);
                        CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (id);
                        GRANT SELECT ON %I TO pgstac_read, pgstac_ingest;
                    $q$,
                    _partition_name, _parent_name, lower(_partition_range), upper(_partition_range),
                    concat(_partition_name, '_pk'), _partition_name,
                    _partition_name
                );
            END IF;
        EXCEPTION
            -- A concurrent creator that finished between our checks: benign, it exists now.
            WHEN duplicate_table THEN
                RAISE DEBUG 'Partition % already exists.', _partition_name;
            -- Do NOT swallow other errors: a failed creation must propagate so the caller never goes on to
            -- write a partition that does not exist (invariant: check_partition succeeds before use).
        END;
    END IF;

    -- Seed the partition_stats row (collection only — the data ranges start NULL), then cover this batch
    -- via the shared widen guard, which fills dtrange/edtrange. ON CONFLICT keeps an existing row (so a
    -- sweep that cleared indexes_pending/dirty is not reset on a later check_partition for the same
    -- partition).
    INSERT INTO partition_stats (partition, collection, dirty, indexes_pending)
        VALUES (_partition_name, _collection, true, true)
        ON CONFLICT (partition) DO NOTHING;
    PERFORM widen_partition_stats(_partition_name, _dtrange, _edtrange, _spatial, _partition_range);

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
