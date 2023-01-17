CREATE OR REPLACE FUNCTION analyze_items(filter text DEFAULT 'items', force boolean DEFAULT FALSE) RETURNS bigint AS $$
DECLARE
q text;
cnt bigint := 0;
BEGIN
FOR q IN
    SELECT format('ANALYZE (VERBOSE, SKIP_LOCKED) %I;', relname)
    FROM pg_stat_user_tables
    WHERE relname like concat('%_', filter, '%') AND (n_mod_since_analyze>0 OR last_analyze IS NULL OR force)
LOOP
        cnt := cnt + 1;
        PERFORM run_or_queue(q);
END LOOP;
RETURN cnt;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION validate_constraints() RETURNS VOID AS $$
    SELECT run_or_queue(
        FORMAT(
            'ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;',
            nsp.nspname,
            cls.relname,
            con.conname
        )
    )
    FROM pg_constraint AS con
        JOIN pg_class AS cls
        ON con.conrelid = cls.oid
        JOIN pg_namespace AS nsp
        ON cls.relnamespace = nsp.oid
    WHERE convalidated = FALSE AND contype in ('c','f')
    AND nsp.nspname = 'pgstac';
$$ LANGUAGE SQL;


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


CREATE OR REPLACE FUNCTION update_collection_extent(_collection text) RETURNS jsonb AS $$
DECLARE
    collection_partition collections%ROWTYPE;
    part text;
    extent jsonb;
    query text;
BEGIN
    SELECT * INTO collection_partition FROM collections WHERE id=_collection FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    part := concat('_items_', collection_partition.key);

    -- Make sure all partions in collection have recent statistics
    PERFORM analyze_items(part);

    extent := partition_extent(part);
    query := format('UPDATE collections SET content = content || %s WHERE id = %L; UPDATE partitions SET update_summaries=false WHERE collection=%L;', extent, _collection, _collection);
    PERFORM run_or_queue(query);
    RETURN extent;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION update_changed_collections() RETURNS bigint AS $$
DECLARE
    c text;
    cnt bigint := 0;
BEGIN
    FOR c IN
        SELECT DISTINCT collection
        FROM partitions WHERE update_summaries
    LOOP
        cnt := cnt + 1;
        PERFORM update_collection_extent(c);
    END LOOP;
    RETURN cnt;
END;
$$ LANGUAGE PLPGSQL;



-- SELECT
--     collection,
--     relname,
--     reltuples,
--     relpages,
--     pg_size_pretty(pg_total_relation_size(relname::regclass)) as total_size,
--     pg_size_pretty(pg_relation_size(relname::regclass)) as table_size,
--     pg_size_pretty(pg_indexes_size(relname::regclass)) as index_size,
--     seq_scan,
--     seq_tup_read,
--     idx_scan,
--     idx_tup_fetch,
--     n_live_tup,
--     n_dead_tup,
--     pg_size_pretty((pg_total_relation_size(relname::regclass)/nullif(reltuples,0))::numeric) as avg_row_size

-- FROM pg_class JOIN pg_stat_user_tables USING (relname)
-- JOIN partitions ON (relname=name)
-- WHERE relname ~ '_items_'
-- ORDER BY pg_total_relation_size(relname::regclass)/nullif(reltuples,0) desc
-- ;
