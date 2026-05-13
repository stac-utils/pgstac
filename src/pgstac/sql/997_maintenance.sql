
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

CREATE OR REPLACE FUNCTION benchmark_partition_stats_queue(
    strategies text[] DEFAULT ARRAY['sync', 'async', 'adaptive'],
    item_count integer DEFAULT 365,
    partition_trunc text DEFAULT 'month',
    adaptive_queue_max_size integer DEFAULT 10,
    adaptive_queue_max_age interval DEFAULT '5 minutes'
) RETURNS TABLE (
    strategy text,
    ingest_ms float,
    queued_after_ingest bigint,
    queue_drain_ms float,
    remaining_queue bigint,
    partitions_total bigint,
    partitions_with_null_stats bigint
) AS $$
DECLARE
    s text;
    raw_strategy text;
    collection_id text;
    ingest_started timestamptz;
    drain_started timestamptz;
BEGIN
    FOREACH raw_strategy IN ARRAY strategies LOOP
        BEGIN
            s := normalize_queue_strategy(raw_strategy);
        EXCEPTION WHEN others THEN
            RAISE EXCEPTION 'benchmark_partition_stats_queue strategy "%" is invalid: %', raw_strategy, SQLERRM;
        END;

        LOOP
            EXIT WHEN run_queued_queries_intransaction() = 0;
        END LOOP;

        PERFORM set_config('pgstac.use_queue', 'false', TRUE);
        PERFORM set_config('pgstac.queue_strategy', s, TRUE);
        PERFORM set_config('pgstac.queue_max_size', adaptive_queue_max_size::text, TRUE);
        PERFORM set_config('pgstac.queue_max_age', adaptive_queue_max_age::text, TRUE);

        collection_id := format(
            'queuebench_%s',
            regexp_replace(s, '[^a-z0-9]+', '_', 'g')
        );
        DELETE FROM collections WHERE id = collection_id;

        INSERT INTO collections (content, partition_trunc)
        VALUES (jsonb_build_object('id', collection_id), partition_trunc);

        ingest_started := clock_timestamp();
        INSERT INTO items_staging (content)
        SELECT jsonb_build_object(
            'id', format('%s-%s', collection_id, g),
            'collection', collection_id,
            'geometry', jsonb_build_object('type', 'Point', 'coordinates', jsonb_build_array(0, 0)),
            'properties', jsonb_build_object(
                'datetime',
                (timestamptz '2020-01-01 00:00:00+00' + ((g - 1) * interval '1 day'))::text
            )
        )
        FROM generate_series(1, item_count) g;
        ingest_ms := age_ms(ingest_started);

        SELECT count(*) INTO queued_after_ingest FROM query_queue;

        drain_started := clock_timestamp();
        LOOP
            EXIT WHEN run_queued_queries_intransaction() = 0;
        END LOOP;
        queue_drain_ms := age_ms(drain_started);

        SELECT count(*) INTO remaining_queue FROM query_queue;
        SELECT
            count(*),
            count(*) FILTER (WHERE spatial IS NULL)
        INTO
            partitions_total,
            partitions_with_null_stats
        FROM partition_sys_meta
        WHERE collection = collection_id;

        strategy := s;
        RETURN NEXT;

        DELETE FROM collections WHERE id = collection_id;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION benchmark_datetime_limit_strategies(
    _where text DEFAULT 'TRUE',
    _orderby text DEFAULT 'datetime DESC, id DESC',
    limits int[] DEFAULT ARRAY[10, 50, 500],
    strategies text[] DEFAULT ARRAY['chunk', 'big_union', 'hybrid'],
    rounds int DEFAULT 3,
    hybrid_max_partition_queries int DEFAULT 128
) RETURNS TABLE (
    strategy text,
    limit_n int,
    top_n_bucket text,
    round_no int,
    rows_returned int,
    first_datetime timestamptz,
    last_datetime timestamptz,
    elapsed_ms float,
    planning_ms float,
    execution_ms float,
    partitions_touched int,
    matches_chunk boolean
) AS $$
DECLARE
    raw_strategy text;
    normalized_strategy text;
    limit_value int;
    run_no int;
    explain_json jsonb;
    explain_query text;
    started_at timestamptz;
    chunk_ids text[];
    strategy_ids text[];
BEGIN
    IF rounds < 1 THEN
        RAISE EXCEPTION 'rounds must be >= 1';
    END IF;
    IF hybrid_max_partition_queries < 1 THEN
        RAISE EXCEPTION 'hybrid_max_partition_queries must be >= 1';
    END IF;

    FOREACH limit_value IN ARRAY limits LOOP
        IF limit_value < 1 THEN
            RAISE EXCEPTION 'limits values must be >= 1 (got %)', limit_value;
        END IF;

        FOR run_no IN 1..rounds LOOP
            started_at := clock_timestamp();
            SELECT
                array_agg(concat_ws(':', (row_item.item).collection, (row_item.item).id) ORDER BY row_item.ord),
                count(*),
                min((row_item.item).datetime),
                max((row_item.item).datetime)
            INTO
                chunk_ids,
                rows_returned,
                first_datetime,
                last_datetime
            FROM search_rows(_where, _orderby, NULL, limit_value) WITH ORDINALITY AS row_item(item, ord);
            elapsed_ms := age_ms(started_at);

            explain_query := format(
                'SELECT * FROM search_rows(%L, %L, NULL, %L);',
                _where,
                _orderby,
                limit_value
            );
            EXECUTE format('EXPLAIN (ANALYZE, FORMAT JSON) %s', explain_query) INTO explain_json;
            planning_ms := COALESCE((explain_json->0->>'Planning Time')::float, 0.0);
            execution_ms := COALESCE((explain_json->0->>'Execution Time')::float, 0.0);
            SELECT count(DISTINCT rel->>0)::int INTO partitions_touched
            FROM jsonb_path_query(explain_json, 'strict $.**."Relation Name" ? (@ != null)') rel;

            strategy := 'chunk';
            limit_n := limit_value;
            top_n_bucket := CASE
                WHEN limit_value <= 50 THEN 'small'
                WHEN limit_value <= 500 THEN 'medium'
                ELSE 'large'
            END;
            round_no := run_no;
            matches_chunk := TRUE;
            RETURN NEXT;

            FOREACH raw_strategy IN ARRAY strategies LOOP
                normalized_strategy := normalize_datetime_limit_strategy(raw_strategy);
                IF normalized_strategy = 'chunk' THEN
                    CONTINUE;
                END IF;

                started_at := clock_timestamp();
                SELECT
                    array_agg(concat_ws(':', (row_item.item).collection, (row_item.item).id) ORDER BY row_item.ord),
                    count(*),
                    min((row_item.item).datetime),
                    max((row_item.item).datetime)
                INTO
                    strategy_ids,
                    rows_returned,
                    first_datetime,
                    last_datetime
                FROM search_rows_strategy(
                    _where,
                    _orderby,
                    NULL,
                    limit_value,
                    normalized_strategy,
                    hybrid_max_partition_queries
                ) WITH ORDINALITY AS row_item(item, ord);
                elapsed_ms := age_ms(started_at);

                explain_query := format(
                    'SELECT * FROM search_rows_strategy(%L, %L, NULL, %L, %L, %L);',
                    _where,
                    _orderby,
                    limit_value,
                    normalized_strategy,
                    hybrid_max_partition_queries
                );
                EXECUTE format('EXPLAIN (ANALYZE, FORMAT JSON) %s', explain_query) INTO explain_json;
                planning_ms := COALESCE((explain_json->0->>'Planning Time')::float, 0.0);
                execution_ms := COALESCE((explain_json->0->>'Execution Time')::float, 0.0);
                SELECT count(DISTINCT rel->>0)::int INTO partitions_touched
                FROM jsonb_path_query(explain_json, 'strict $.**."Relation Name" ? (@ != null)') rel;

                strategy := normalized_strategy;
                limit_n := limit_value;
                top_n_bucket := CASE
                    WHEN limit_value <= 50 THEN 'small'
                    WHEN limit_value <= 500 THEN 'medium'
                    ELSE 'large'
                END;
                round_no := run_no;
                matches_chunk := COALESCE(strategy_ids, ARRAY[]::text[]) = COALESCE(chunk_ids, ARRAY[]::text[]);
                RETURN NEXT;
            END LOOP;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;
