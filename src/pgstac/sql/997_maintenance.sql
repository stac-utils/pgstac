
DROP FUNCTION IF EXISTS analyze_items;
CREATE OR REPLACE PROCEDURE analyze_items() AS $$
DECLARE
    q text;
    timeout_ts timestamptz;
BEGIN
    timeout_ts := statement_timestamp() + queue_timeout();
    WHILE clock_timestamp() < timeout_ts LOOP
        RAISE NOTICE '% % %', clock_timestamp(), timeout_ts, current_setting('statement_timeout', TRUE);
        SELECT format('ANALYZE (VERBOSE, SKIP_LOCKED) %I;', relname) INTO q
        FROM pg_stat_user_tables
        WHERE relname like '_item%' AND (n_mod_since_analyze>0 OR last_analyze IS NULL) LIMIT 1;
        IF NOT FOUND THEN
            EXIT;
        END IF;
        RAISE NOTICE '%', q;
        EXECUTE q;
        COMMIT;
        RAISE NOTICE '%', queue_timeout();
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


DROP FUNCTION IF EXISTS validate_constraints;
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


CREATE OR REPLACE FUNCTION partition_extent(part text) RETURNS jsonb AS $$
DECLARE
    collection_partition collections%ROWTYPE;
    geom_extent geometry;
    mind timestamptz;
    maxd timestamptz;
    extent jsonb;
BEGIN
    EXECUTE FORMAT(
        '
        SELECT
            min(datetime),
            max(end_datetime)
        FROM %I;
        ',
        part
    ) INTO mind, maxd;
    geom_extent := ST_EstimatedExtent(part, 'geometry');
    extent := jsonb_build_object(
        'extent', jsonb_build_object(
            'spatial', jsonb_build_object(
                'bbox', to_jsonb(array[array[st_xmin(geom_extent), st_ymin(geom_extent), st_xmax(geom_extent), st_ymax(geom_extent)]])
            ),
            'temporal', jsonb_build_object(
                'interval', to_jsonb(array[array[mind, maxd]])
            )
        )
    );
    RETURN extent;
END;
$$ LANGUAGE PLPGSQL;
