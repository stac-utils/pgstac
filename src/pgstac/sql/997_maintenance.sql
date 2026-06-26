
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
        PERFORM update_partition_stats_q(partition)
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

