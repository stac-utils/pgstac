CREATE OR REPLACE PROCEDURE analyze_items() AS $$
DECLARE
q text;
BEGIN
FOR q IN
    SELECT format('ANALYZE (VERBOSE, SKIP_LOCKED) %I;', relname)
    FROM pg_stat_user_tables
    WHERE relname ilike '%item%' AND n_mod_since_analyze>0
LOOP
        RAISE NOTICE 'RUNNING: %', q;
        EXECUTE q;
END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION update_partition_record(poid oid) RETURNS json AS $$
DECLARE
    part_record partitions%ROWTYPE;
    new_part_record partitions%ROWYTPE;
    geom_extent geometry;
    mind timestamptz;
    maxd timestamptz;
    mined timestamptz;
    maxed timestamptz;
BEGIN
    -- Skip update if partition record is locked otherwise take out a lock.
    SELECT * INTO part_record FROM partitions WHERE partition_oid = poid FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- Make sure that table has been analyzed
    IF EXISTS (
        SELECT 1 FROM pg_stat_user_tables WHERE relid=poid AND n_mod_since_analyze > 0
    ) THEN
        EXECUTE FORMAT(
            'ANALYZE (SKIP_LOCKED) %I;',
            poid::regclass::text
        );
        RAISE NOTICE 'Analyzed %', poid::regclass::text;
    END IF;

    SELECT st_estimatedextent('pgstac', poid::regclass::text, 'geometry') INTO geom_extent;
    RAISE NOTICE 'Got Extent %', poid::regclass::text;
    EXECUTE FORMAT(
        '
        SELECT
            min(datetime),
            max(datetime),
            min(end_datetime),
            max(end_datetime),
            count(*)
        FROM %I;
        ',
        poid::regclass::text
    ) INTO mind, maxd, mined, maxed;
    RAISE NOTICE 'Got Temporal Extent %', poid::regclass::text;
    RAISE NOTICE '%, %, %, %, %', geom_extent, mind, maxd, mined, maxed;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS maintain_partition;
CREATE OR REPLACE FUNCTION maintain_partition(
    part text,
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS SETOF text AS $$
DECLARE
    stats pg_stat_user_tables%ROWTYPE;
    parent text;
    level int;
    isleaf bool;
    collection collections%ROWTYPE;
    subpart text;
    baseidx text;
    queryable_name text;
    queryable_property_index_type text;
    queryable_property_wrapper text;
    queryable_parsed RECORD;
    deletedidx pg_indexes%ROWTYPE;
    q text;
    idx text;
    collection_partition bigint;
BEGIN
    RAISE NOTICE 'Maintaining partition: %', part;
    SELECT * INTO stats
    FROM pg_stat_user_tables
    WHERE schemaname='pgstac' AND relname=$1;
    IF NOT FOUND THEN
        RAISE NOTICE 'Partition % Does Not Exist', part;
        RETURN;
    END IF;

    -- Get root partition
    SELECT parentrelid::text, pt.isleaf, pt.level
        INTO parent, isleaf, level
    FROM pg_partition_tree('items') pt
    WHERE relid::text = part;
    IF NOT FOUND THEN
        RAISE NOTICE 'Partition % Does Not Exist In Partition Tree', part;
        RETURN;
    END IF;

    -- If this is a parent partition, recurse to leaves
    IF NOT isleaf THEN
        FOR subpart IN
            SELECT relid::text
            FROM pg_partition_tree(part)
            WHERE relid::text != part
        LOOP
            RAISE NOTICE 'Recursing to %', subpart;
            RETURN QUERY SELECT * FROM maintain_partition(subpart, dropindexes, rebuildindexes);
        END LOOP;
        RETURN; -- Don't continue since not an end leaf
    END IF;


    -- Get collection
    collection_partition := ((regexp_match(part, E'^_items_([0-9]+)'))[1])::bigint;
    RAISE NOTICE 'COLLECTION PARTITION: %', collection_partition;
    SELECT * INTO STRICT collection
    FROM collections
    WHERE key = collection_partition;
    RAISE NOTICE 'COLLECTION ID: %s', collection.id;


    -- Create temp table with existing indexes
    CREATE TEMP TABLE existing_indexes ON COMMIT DROP AS
    SELECT *
    FROM pg_indexes
    WHERE schemaname='pgstac' AND tablename=part;


    -- Check if index exists for each queryable.
    FOR
        queryable_name,
        queryable_property_index_type,
        queryable_property_wrapper
    IN
        SELECT
            name,
            COALESCE(property_index_type, 'BTREE'),
            COALESCE(property_wrapper, 'to_text')
        FROM queryables
        WHERE
            name NOT in ('id', 'datetime', 'geometry')
            AND (
                collection_ids IS NULL
                OR collection_ids = '{}'::text[]
                OR collection.id = ANY (collection_ids)
            )
        UNION ALL
        SELECT 'datetime desc, end_datetime', 'BTREE', ''
        UNION ALL
        SELECT 'geometry', 'GIST', ''
        UNION ALL
        SELECT 'id', 'BTREE', ''
    LOOP
        baseidx := format(
            $q$ON %I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
            part,
            queryable_property_index_type,
            queryable_property_wrapper,
            queryable_name
        );
        -- If index already exists, delete it from existing indexes type table
        DELETE FROM existing_indexes
        WHERE indexdef ~* format($q$[(']%s[')]$q$, queryable_name)
        RETURNING * INTO deletedidx;
        IF NOT FOUND THEN -- index did not exist, create it
            RETURN NEXT format('CREATE INDEX CONCURRENTLY %s;', baseidx);
        ELSIF rebuildindexes THEN
            RETURN NEXT format('REINDEX %I CONCURRENTLY;', deletedidx.indexname);
        END IF;
    END LOOP;

    -- Remove indexes that were not expected
    IF dropindexes THEN
        FOR idx IN SELECT indexname::text FROM existing_indexes
        LOOP
            RETURN NEXT format('DROP INDEX IF EXISTS %I;', idx);
        END LOOP;
    END IF;

    DROP TABLE existing_indexes;


    -- Check that constraints are present


    -- Check that constraints are valid


    -- Check if partition needs to be analyzed
    IF stats.n_mod_since_analyze > 0 THEN
        RETURN NEXT FORMAT ('ANALYZE (VERBOSE, SKIP_LOCKED) %I', stats.relname);
    END IF;
    RETURN;

END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS check_pgstac_settings;
CREATE OR REPLACE FUNCTION check_pgstac_settings(_sysmem text) RETURNS VOID AS $$
DECLARE
settingval text;
sysmem bigint := pg_size_bytes(_sysmem);
effective_cache_size bigint := pg_size_bytes(current_setting('effective_cache_size', TRUE));
shared_buffers bigint := pg_size_bytes(current_setting('shared_buffers', TRUE));
work_mem bigint := pg_size_bytes(current_setting('work_mem', TRUE));
max_connections int := current_setting('max_connections', TRUE);
maintenance_work_mem bigint := pg_size_bytes(current_setting('maintenance_work_mem', TRUE));
seq_page_cost float := current_setting('seq_page_cost', TRUE);
random_page_cost float := current_setting('random_page_cost', TRUE);
temp_buffers bigint := pg_size_bytes(current_setting('temp_buffers', TRUE));
BEGIN
IF effective_cache_size < (sysmem * 0.5) THEN
    RAISE WARNING 'effective_cache_size of % is set low for a system with %. Recomended value between % and %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.5), pg_size_pretty(sysmem * 0.75);
ELSIF effective_cache_size > (sysmem * 0.75) THEN
    RAISE WARNING 'effective_cache_size of % is set high for a system with %. Recomended value between % and %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.5), pg_size_pretty(sysmem * 0.75);
ELSE
    RAISE NOTICE 'effective_cache_size of % is set appropriately for a system with %', pg_size_pretty(effective_cache_size), pg_size_pretty(sysmem);
END IF;

IF shared_buffers < (sysmem * 0.2) THEN
    RAISE WARNING 'shared_buffers of % is set low for a system with %. Recomended value between % and %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.2), pg_size_pretty(sysmem * 0.3);
ELSIF shared_buffers > (sysmem * 0.3) THEN
    RAISE WARNING 'shared_buffers of % is set high for a system with %. Recomended value between % and %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem), pg_size_pretty(sysmem * 0.2), pg_size_pretty(sysmem * 0.3);
ELSE
    RAISE NOTICE 'shared_buffers of % is set appropriately for a system with %', pg_size_pretty(shared_buffers), pg_size_pretty(sysmem);
END IF;

IF maintenance_work_mem < (sysmem * 0.2) THEN
    RAISE WARNING 'maintenance_work_mem of % is set low for shared_buffers of %. Recomended value between % and %', pg_size_pretty(maintenance_work_mem), pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers * 0.2), pg_size_pretty(shared_buffers * 0.3);
ELSIF maintenance_work_mem > (shared_buffers * 0.3) THEN
    RAISE WARNING 'maintenance_work_mem of % is set high for shared_buffers of %. Recomended value between % and %', pg_size_pretty(maintenance_work_mem), pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers * 0.2), pg_size_pretty(shared_buffers * 0.3);
ELSE
    RAISE NOTICE 'maintenance_work_mem of % is set appropriately for shared_buffers of %', pg_size_pretty(shared_buffers), pg_size_pretty(shared_buffers);
END IF;

IF work_mem * max_connections > shared_buffers THEN
    RAISE WARNING 'work_mem setting of % is set high for % max_connections please reduce work_mem to % or decrease max_connections to %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers/max_connections), floor(shared_buffers/work_mem);
ELSIF work_mem * max_connections < (shared_buffers * 0.75) THEN
    RAISE WARNING 'work_mem setting of % is set low for % max_connections you may consider raising work_mem to % or increasing max_connections to %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers/max_connections), floor(shared_buffers/work_mem);
ELSE
    RAISE NOTICE 'work_mem setting of % and max_connections of % are adequate for shared_buffers of %', pg_size_pretty(work_mem), max_connections, pg_size_pretty(shared_buffers);
END IF;

IF random_page_cost / seq_page_cost != 1.1 THEN
    RAISE WARNING 'random_page_cost (%) /seq_page_cost (%) should be set to 1.1 for SSD. Change random_page_cost to %', random_page_cost, seq_page_cost, 1.1 * seq_page_cost;
ELSE
    RAISE NOTICE 'random_page_cost and seq_page_cost set appropriately for SSD';
END IF;

IF temp_buffers < greatest(pg_size_bytes('128MB'),(maintenance_work_mem / 2)) THEN
    RAISE WARNING 'pgstac makes heavy use of temp tables, consider raising temp_buffers from % to %', pg_size_pretty(temp_buffers), greatest('128MB', pg_size_pretty((maintenance_work_mem / 4)));
END IF;

RAISE NOTICE 'VALUES FOR PGSTAC VARIABLES';
RAISE NOTICE 'These can be set either as GUC system variables or by setting in the pgstac_settings table.';

RAISE NOTICE 'context: %', get_setting('context');

RAISE NOTICE 'context_estimated_count: %', get_setting('context_estimated_count');

RAISE NOTICE 'context_estimated_cost: %', get_setting('context_estimated_cost');

RAISE NOTICE 'context_stats_ttl: %', get_setting('context_stats_ttl');

RAISE NOTICE 'default-filter-lang: %', get_setting('default-filter-lang');

RAISE NOTICE 'additional_properties: %', get_setting('additional_properties');

SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pg_cron';
IF NOT FOUND OR settingval IS NULL THEN
    RAISE WARNING 'Additional capabilities are available if the pg_cron extension is installed alongside pgstac.';
ELSE
    RAISE NOTICE 'pg_cron % is installed', settingval;
END IF;

SELECT installed_version INTO settingval from pg_available_extensions WHERE name = 'pgstattuple';
IF NOT FOUND OR settingval IS NULL THEN
    RAISE WARNING 'Additional capabilities are available if the pgstattuple extension is installed alongside pgstac.';
ELSE
    RAISE NOTICE 'pgstattuple % is installed', settingval;
END IF;

END;
$$ LANGUAGE PLPGSQL;
SELECT check_pgstac_settings('128GB');
