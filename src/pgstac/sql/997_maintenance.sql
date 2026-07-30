
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
    _partition text;
BEGIN
    IF runupdate THEN
        -- Not queued: the aggregate below reads what this writes. Ordered by
        -- partition, as every other multi-partition writer is.
        FOR _partition IN
            SELECT partition FROM partition_stats
            WHERE collection=_collection
            ORDER BY partition
        LOOP
            PERFORM update_partition_stats(_partition, false, true);
        END LOOP;
    END IF;
    SELECT
        min(lower(dtrange)),
        max(upper(edtrange)),
        st_extent(spatial)
    INTO
        mind,
        maxd,
        geom_extent
    FROM partition_stats
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


-- Reconcile partition_stats against the partition tree: an identity row for
-- every partition, and no rows for partitions that are gone. dtrange, edtrange
-- and spatial are left alone; update_partition_stats maintains those. Used by
-- the idempotent install and as a repair path.
CREATE OR REPLACE FUNCTION sync_partition_stats() RETURNS VOID AS $$
BEGIN
    -- Ordered by partition, as every other writer of these rows is.
    INSERT INTO partition_stats (partition, collection, partition_dtrange)
        SELECT partition, collection, partition_dtrange FROM partitions_view
        ORDER BY partition
        ON CONFLICT (partition) DO UPDATE
            SET collection = EXCLUDED.collection,
                partition_dtrange = EXCLUDED.partition_dtrange
            WHERE
                partition_stats.collection IS DISTINCT FROM EXCLUDED.collection
                OR partition_stats.partition_dtrange IS DISTINCT FROM EXCLUDED.partition_dtrange
    ;

    DELETE FROM partition_stats ps
    WHERE ps.partition IN (
        SELECT partition FROM partition_stats stale
        WHERE NOT EXISTS (
            SELECT 1 FROM partitions_view pv WHERE pv.partition = stale.partition
        )
        ORDER BY partition
        FOR UPDATE
    );
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;
