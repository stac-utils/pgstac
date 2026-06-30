
-- tighten_dirty_partition_stats: recompute the exact envelope + row count for dirty partitions (oldest
-- first), clearing dirty. Run off-hours (pg_cron or the maintenance CLI). Optional: a wide envelope only
-- over-includes a partition in search, so skipping it never loses rows. `_limit` caps the batch (NULL =
-- all dirty); returns the number of partitions tightened.
--
-- pg_cron example (operators install this themselves):
--   SELECT cron.schedule('pgstac-tighten', '*/15 * * * *',
--                        $$SELECT pgstac.tighten_dirty_partition_stats(200)$$);
CREATE OR REPLACE FUNCTION tighten_dirty_partition_stats(_limit int DEFAULT NULL)
RETURNS int AS $$
DECLARE
    _part text;
    _count int := 0;
BEGIN
    FOR _part IN
        SELECT partition FROM pgstac.partition_stats
        WHERE dirty
        ORDER BY last_updated NULLS FIRST
        LIMIT _limit
    LOOP
        PERFORM pgstac.tighten_partition_stats(_part);
        _count := _count + 1;
    END LOOP;
    RETURN _count;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


CREATE OR REPLACE PROCEDURE analyze_items() AS $$
DECLARE
    q text;
    timeout_ts timestamptz;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        SELECT format('ANALYZE (VERBOSE, SKIP_LOCKED) %I;', relname) INTO q
        FROM pg_stat_user_tables
        WHERE relname like '_item%' AND (n_mod_since_analyze>0 OR last_analyze IS NULL) LIMIT 1;
        IF NOT FOUND THEN
            EXIT;
        END IF;
        RAISE NOTICE '%', q;
        EXECUTE q;
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE PROCEDURE validate_constraints() AS $$
DECLARE
    q text;
BEGIN
    FOR q IN
    SELECT
        FORMAT(
            'ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;',
            nsp.nspname,
            cls.relname,
            con.conname
        )

    FROM pg_constraint AS con
        JOIN pg_class AS cls
        ON con.conrelid = cls.oid
        JOIN pg_namespace AS nsp
        ON cls.relnamespace = nsp.oid
    WHERE convalidated = FALSE AND contype in ('c','f')
    AND nsp.nspname = 'pgstac'
    LOOP
        RAISE NOTICE '%', q;
        PERFORM run_or_queue(q);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION collection_extent(_collection text, runupdate boolean default false) RETURNS jsonb AS $$
DECLARE
    geom_extent geometry;
    mind timestamptz;
    maxd timestamptz;
    extent jsonb;
BEGIN
    IF runupdate THEN
        PERFORM tighten_partition_stats(partition)
        FROM partitions_view WHERE collection=_collection;
    END IF;
    SELECT
        min(lower(dtrange)),
        max(upper(edtrange)),
        st_extent(spatial)
    INTO
        mind,
        maxd,
        geom_extent
    FROM partitions_view
    WHERE collection=_collection;

    IF geom_extent IS NOT NULL AND mind IS NOT NULL AND maxd IS NOT NULL THEN
        extent := jsonb_build_object(
                'spatial', jsonb_build_object(
                    'bbox', to_jsonb(array[array[st_xmin(geom_extent), st_ymin(geom_extent), st_xmax(geom_extent), st_ymax(geom_extent)]])
                ),
                'temporal', jsonb_build_object(
                    'interval', to_jsonb(array[array[mind, maxd]])
                )
        );
        RETURN extent;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION gc_deleted_items_log_batch(
    retention_interval interval DEFAULT '30 days',
    batch_limit integer DEFAULT 10000
) RETURNS bigint AS $$
DECLARE
    batch_deleted bigint;
BEGIN
    WITH to_delete AS (
        SELECT ctid
        FROM items_deleted_log
        WHERE deleted_at < now() - retention_interval
        ORDER BY deleted_at
        LIMIT GREATEST(COALESCE(batch_limit, 10000), 1)
    ),
    deleted AS (
        DELETE FROM items_deleted_log d
        USING to_delete td
        WHERE d.ctid = td.ctid
        RETURNING 1
    )
    SELECT count(*)::bigint INTO batch_deleted FROM deleted;

    RETURN batch_deleted;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION gc_deleted_items_log(
    retention_interval interval,
    batch_limit integer
) RETURNS bigint AS $$
DECLARE
    deleted_count bigint := 0;
    batch_deleted bigint;
BEGIN
    LOOP
        batch_deleted := gc_deleted_items_log_batch(retention_interval, batch_limit);
        deleted_count := deleted_count + batch_deleted;
        EXIT WHEN batch_deleted = 0;
    END LOOP;

    RETURN deleted_count;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION gc_deleted_items_log(retention_interval interval DEFAULT '30 days') RETURNS bigint AS $$
    SELECT gc_deleted_items_log(retention_interval, 10000);
$$ LANGUAGE SQL SECURITY DEFINER;

CREATE OR REPLACE PROCEDURE gc_deleted_items_log_committed(
    retention_interval interval DEFAULT '30 days',
    batch_limit integer DEFAULT 10000
) AS $$
DECLARE
    batch_deleted bigint;
BEGIN
    LOOP
        batch_deleted := gc_deleted_items_log_batch(retention_interval, batch_limit);
        EXIT WHEN batch_deleted = 0;
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


-- update_collection_extents: recompute every collection's extent from partitions_view.
CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections
    SET content = jsonb_set_lax(
        content,
        '{extent}'::text[],
        collection_extent(id, FALSE),
        true,
        'use_json_null'
    )
;
$$ LANGUAGE SQL;


-- ---------------------------------------------------------------------------
-- Field registry maintenance: track which paths (and value types) exist per
-- collection. Used for schema inference (e.g. the geoparquet export schema).
-- jsonb_field_rows is in 001a_jsonutils.sql.
-- ---------------------------------------------------------------------------

-- update_field_registry_from_sample: UPSERT registry rows from an array of raw item content JSONBs (the
-- caller picks the sample); value_kinds accumulate observed types over time.
CREATE OR REPLACE FUNCTION update_field_registry_from_sample(
    _collection text,
    item_contents jsonb[]
) RETURNS void AS $$
    INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen)
    SELECT
        _collection,
        r.path,
        bool_and(r.is_leaf)                                                       AS is_leaf,
        array_agg(DISTINCT r.value_kind) FILTER (WHERE r.value_kind IS NOT NULL)  AS value_kinds,
        now(),
        now()
    FROM unnest(item_contents) AS item(content)
    CROSS JOIN LATERAL jsonb_field_rows(item.content) AS r(path, is_leaf, value_kind)
    GROUP BY r.path
    ON CONFLICT (collection, path) DO UPDATE SET
        is_leaf     = EXCLUDED.is_leaf,
        value_kinds = (
            SELECT array_agg(DISTINCT v)
            FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)
        ),
        last_seen   = now()
    ;
$$ LANGUAGE SQL VOLATILE SECURITY DEFINER;


-- update_field_registry_from_items: sample a live collection and UPSERT registry rows (TABLESAMPLE
-- BERNOULLI(5) above ~10k rows by pg_class estimate, else LIMIT 1000). Returns (registered_paths,
-- rows_processed).
CREATE OR REPLACE FUNCTION update_field_registry_from_items(
    _collection text
) RETURNS TABLE (registered_paths int, rows_processed int) AS $$
DECLARE
    est_rows bigint;
    nrows    int;
    npaths   int;
BEGIN
    -- Sum reltuples across the registered item partitions for this collection.
    -- reltuples can be -1 (never analyzed); treat negative values as zero.
    SELECT COALESCE(sum(GREATEST(c.reltuples::bigint, 0)), 0) INTO est_rows
    FROM partitions_view p
    JOIN pg_class c ON c.relname = p.partition
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE p.collection = _collection
      AND n.nspname = 'pgstac'
      AND c.relkind = 'r';

    IF est_rows > 10000 THEN
        -- Large collection: use statistical sampling to avoid full seq-scan.
        WITH sampled AS (
            SELECT content_hydrate(i) AS content FROM items i TABLESAMPLE BERNOULLI(5) WHERE i.collection = _collection
        ),
        upserted AS (
            INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen)
            SELECT
                _collection,
                r.path,
                bool_and(r.is_leaf)                                                       AS is_leaf,
                array_agg(DISTINCT r.value_kind) FILTER (WHERE r.value_kind IS NOT NULL)  AS value_kinds,
                now(), now()
            FROM sampled
            CROSS JOIN LATERAL jsonb_field_rows(content) AS r(path, is_leaf, value_kind)
            GROUP BY r.path
            ON CONFLICT (collection, path) DO UPDATE SET
                is_leaf     = EXCLUDED.is_leaf,
                value_kinds = (
                    SELECT array_agg(DISTINCT v)
                    FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)
                ),
                last_seen   = now()
            RETURNING 1
        )
        SELECT
            (SELECT count(*)::int FROM upserted),
            (SELECT count(*)::int FROM sampled)
        INTO npaths, nrows;
    ELSE
        -- Small collection: process up to 1000 rows to avoid BERNOULLI returning 0 rows.
        WITH sampled AS (
            SELECT content_hydrate(i) AS content FROM items i WHERE i.collection = _collection LIMIT 1000
        ),
        upserted AS (
            INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen)
            SELECT
                _collection,
                r.path,
                bool_and(r.is_leaf)                                                       AS is_leaf,
                array_agg(DISTINCT r.value_kind) FILTER (WHERE r.value_kind IS NOT NULL)  AS value_kinds,
                now(), now()
            FROM sampled
            CROSS JOIN LATERAL jsonb_field_rows(content) AS r(path, is_leaf, value_kind)
            GROUP BY r.path
            ON CONFLICT (collection, path) DO UPDATE SET
                is_leaf     = EXCLUDED.is_leaf,
                value_kinds = (
                    SELECT array_agg(DISTINCT v)
                    FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)
                ),
                last_seen   = now()
            RETURNING 1
        )
        SELECT
            (SELECT count(*)::int FROM upserted),
            (SELECT count(*)::int FROM sampled)
        INTO npaths, nrows;
    END IF;

    RETURN QUERY SELECT npaths, nrows;
END;
$$ LANGUAGE PLPGSQL VOLATILE SECURITY DEFINER;


-- refresh_field_registry: expire registry entries not seen within retention_interval (scheduled
-- maintenance). Returns (collection, expired_paths) per affected collection.
CREATE OR REPLACE FUNCTION refresh_field_registry(
    _collection text DEFAULT NULL,
    retention_interval interval DEFAULT '90 days'
) RETURNS TABLE (collection_id text, expired_paths int) AS $$
    WITH deleted AS (
        DELETE FROM item_field_registry
        WHERE (_collection IS NULL OR collection = _collection)
          AND last_seen < now() - retention_interval
        RETURNING collection
    )
    SELECT collection, count(*)::int
    FROM deleted
    GROUP BY collection;
$$ LANGUAGE SQL VOLATILE SECURITY DEFINER;


-- gc_fragments: garbage-collect orphaned item_fragments with a single set-based DELETE (NOT EXISTS
-- anti-join against items.fragment_id). items.fragment_id has no FK (partitioned-items incremental
-- NOT VALID FKs aren't supported), so a fragment unreferenced at the DELETE snapshot but referenced by a
-- concurrent insert can be removed; the retention_interval guard makes this unlikely. Run during
-- low-ingest periods.
CREATE OR REPLACE FUNCTION gc_fragments(
    _collection text DEFAULT NULL,
    retention_interval interval DEFAULT '90 days'
) RETURNS TABLE (
    collection_id text,
    fragments_removed int
) AS $$
    WITH deleted AS (
        DELETE FROM item_fragments f
        WHERE
            (_collection IS NULL OR f.collection = _collection)
            AND f.created_at < now() - retention_interval
            AND NOT EXISTS (SELECT 1 FROM items i WHERE i.fragment_id = f.id)
        RETURNING f.collection
    )
    SELECT collection, count(*)::int
    FROM deleted
    GROUP BY collection;
$$ LANGUAGE SQL VOLATILE PARALLEL UNSAFE SECURITY DEFINER;


-- tighten_partition_stats: recompute the exact envelope (datetime/end_datetime min-max + spatial extent)
-- and row count for one partition, then clear `dirty`. The only function that narrows a partition_stats
-- envelope; the maintenance sweeps drive it over partition_stats WHERE dirty. An empty partition tightens
-- to an 'empty' range so search prunes it out.
CREATE OR REPLACE FUNCTION tighten_partition_stats(_partition text) RETURNS void AS $$
DECLARE
    _n bigint;
    _dtmin timestamptz; _dtmax timestamptz;
    _edtmin timestamptz; _edtmax timestamptz;
    _spatial geometry;
    _dtrange tstzrange;
    _edtrange tstzrange;
    _collection text;
BEGIN
    -- Hold the partition's advisory lock across the scan + write: tighten narrows dtrange to the scanned
    -- extent and clears dirty, so without the lock a concurrent ingest committing a row outside that
    -- extent would be left uncovered (search would prune + miss it). Same lock check_partition uses, so
    -- tighten serializes with ingest into this partition only.
    PERFORM pg_advisory_xact_lock(hashtext('pgstac.check_partition'), hashtext(_partition));

    EXECUTE format(
        $q$
            SELECT count(*), min(datetime), max(datetime), min(end_datetime), max(end_datetime),
                   st_setsrid(st_extent(geometry)::geometry, 4326)
            FROM %I
        $q$,
        _partition
    ) INTO _n, _dtmin, _dtmax, _edtmin, _edtmax, _spatial;

    IF _n = 0 THEN
        _dtrange := 'empty'::tstzrange;
        _edtrange := 'empty'::tstzrange;
        _spatial := NULL;
    ELSE
        _dtrange := tstzrange(_dtmin, _dtmax, '[]');
        _edtrange := tstzrange(_edtmin, _edtmax, '[]');
    END IF;

    SELECT pv.collection
        INTO _collection
    FROM partitions_view pv WHERE partition = _partition;

    INSERT INTO partition_stats
        (partition, collection, dtrange, edtrange, spatial, n, dirty, last_updated)
        VALUES (_partition, _collection, _dtrange, _edtrange, _spatial, _n, false, now())
        ON CONFLICT (partition) DO UPDATE SET
            collection = EXCLUDED.collection,
            dtrange = EXCLUDED.dtrange,
            edtrange = EXCLUDED.edtrange,
            spatial = EXCLUDED.spatial,
            n = EXCLUDED.n,
            dirty = false,
            last_updated = now();
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;
