CREATE TABLE queryables (
    id bigint GENERATED ALWAYS AS identity PRIMARY KEY,
    name text NOT NULL,
    collection_ids text[], -- used to determine what partitions to create indexes on
    definition jsonb,
    property_path text,
    property_wrapper text,
    property_index_type text
);
CREATE INDEX queryables_name_idx ON queryables (name);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);




CREATE OR REPLACE FUNCTION array_to_path(arr text[]) RETURNS text AS $$
    SELECT string_agg(
        quote_literal(v),
        '->'
    ) FROM unnest(arr) v;
$$ LANGUAGE SQL IMMUTABLE STRICT;




CREATE OR REPLACE FUNCTION queryable(
    IN dotpath text,
    OUT path text,
    OUT expression text,
    OUT wrapper text,
    OUT nulled_wrapper text
) AS $$
DECLARE
    q RECORD;
    path_elements text[];
BEGIN
    IF dotpath IN ('id', 'geometry', 'datetime', 'end_datetime', 'collection') THEN
        path := dotpath;
        expression := dotpath;
        wrapper := NULL;
        RETURN;
    END IF;
    SELECT * INTO q FROM queryables
        WHERE
            name=dotpath
            OR name = 'properties.' || dotpath
            OR name = replace(dotpath, 'properties.', '')
    ;
    IF q.property_wrapper IS NULL THEN
        IF q.definition->>'type' = 'number' THEN
            wrapper := 'to_float';
            nulled_wrapper := wrapper;
        ELSIF q.definition->>'format' = 'date-time' THEN
            wrapper := 'to_tstz';
            nulled_wrapper := wrapper;
        ELSE
            nulled_wrapper := NULL;
            wrapper := 'to_text';
        END IF;
    ELSE
        wrapper := q.property_wrapper;
        nulled_wrapper := wrapper;
    END IF;
    IF q.property_path IS NOT NULL THEN
        path := q.property_path;
    ELSE
        path_elements := string_to_array(dotpath, '.');
        IF path_elements[1] IN ('links', 'assets', 'stac_version', 'stac_extensions') THEN
            path := format('content->%s', array_to_path(path_elements));
        ELSIF path_elements[1] = 'properties' THEN
            path := format('content->%s', array_to_path(path_elements));
        ELSE
            path := format($F$content->'properties'->%s$F$, array_to_path(path_elements));
        END IF;
    END IF;
    expression := format('%I(%s)', wrapper, path);
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;


DROP VIEW IF EXISTS pgstac_indexes;
CREATE VIEW pgstac_indexes AS
SELECT
    i.schemaname,
    i.tablename,
    i.indexname,
    indexdef,
    COALESCE(
        (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
        (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_]+)''::text'))[1],
        CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime_end_datetime' ELSE NULL END
    ) AS field,
    pg_table_size(i.indexname::text) as index_size,
    pg_size_pretty(pg_table_size(i.indexname::text)) as index_size_pretty,
    n_distinct,
    most_common_vals::text::text[],
    most_common_freqs::text::text[],
    histogram_bounds::text::text[],
    correlation
FROM
    pg_indexes i
    LEFT JOIN pg_stats s ON (s.tablename = i.indexname)
WHERE i.schemaname='pgstac' and i.tablename ~ '_items_';

set check_function_bodies to off;
CREATE OR REPLACE FUNCTION maintain_partition_queries(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS SETOF text AS $$
DECLARE
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
            RETURN QUERY SELECT * FROM maintain_partition_queries(subpart, dropindexes, rebuildindexes);
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
            $q$ ON %I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
            part,
            queryable_property_index_type,
            queryable_property_wrapper,
            queryable_name
        );
        RAISE NOTICE 'BASEIDX: %', baseidx;
        RAISE NOTICE 'IDXSEARCH: %', format($q$[(']%s[')]$q$, queryable_name);
        -- If index already exists, delete it from existing indexes type table
        DELETE FROM existing_indexes
        WHERE indexdef ~* format($q$[(']%s[')]$q$, queryable_name)
        RETURNING * INTO deletedidx;
        RAISE NOTICE 'EXISTING INDEX: %', deletedidx;
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
    RETURN;

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION maintain_partitions(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partitions_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$$ LANGUAGE SQL;


-- CREATE OR REPLACE FUNCTION queryables_trigger_func() RETURNS TRIGGER AS $$
-- DECLARE
-- BEGIN
--     PERFORM maintain_partitions();
--     RETURN NEW;
-- END;
-- $$ LANGUAGE PLPGSQL;

-- CREATE TRIGGER queryables_trigger AFTER INSERT OR UPDATE ON queryables
-- FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();

-- CREATE TRIGGER queryables_collection_trigger
--     AFTER INSERT OR UPDATE ON collections
--     FOR EACH STATEMENT
--     WHEN OLD.partition_trunc IS DISTINCT FROM NEW.partition_trunc
--     EXECUTE PROCEDURE queryables_trigger_func();


CREATE OR REPLACE FUNCTION get_queryables(_collection_ids text[] DEFAULT NULL) RETURNS jsonb AS $$
DECLARE
BEGIN
    -- Build up queryables if the input contains valid collection ids or is empty
    IF EXISTS (
        SELECT 1 FROM collections
        WHERE
            _collection_ids IS NULL
            OR cardinality(_collection_ids) = 0
            OR id = ANY(_collection_ids)
    )
    THEN
        RETURN (
            WITH base AS (
                SELECT
                    unnest(collection_ids) as collection_id,
                    name,
                    coalesce(definition, '{"type":"string"}'::jsonb) as definition
                FROM queryables
                WHERE
                    _collection_ids IS NULL OR
                    _collection_ids = '{}'::text[] OR
                    _collection_ids && collection_ids
                UNION ALL
                SELECT null, name, coalesce(definition, '{"type":"string"}'::jsonb) as definition
                FROM queryables WHERE collection_ids IS NULL OR collection_ids = '{}'::text[]
            ), g AS (
                SELECT
                    name,
                    first_notnull(definition) as definition,
                    jsonb_array_unique_merge(definition->'enum') as enum,
                    jsonb_min(definition->'minimum') as minimum,
                    jsonb_min(definition->'maxiumn') as maximum
                FROM base
                GROUP BY 1
            )
            SELECT
                jsonb_build_object(
                    '$schema', 'http://json-schema.org/draft-07/schema#',
                    '$id', '',
                    'type', 'object',
                    'title', 'STAC Queryables.',
                    'properties', jsonb_object_agg(
                        name,
                        definition
                        ||
                        jsonb_strip_nulls(jsonb_build_object(
                            'enum', enum,
                            'minimum', minimum,
                            'maximum', maximum
                        ))
                    )
                )
                FROM g
        );
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables(_collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT
        CASE
            WHEN _collection IS NULL THEN get_queryables(NULL::text[])
            ELSE get_queryables(ARRAY[_collection])
        END
    ;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_queryables() RETURNS jsonb AS $$
    SELECT get_queryables(NULL::text[]);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION schema_qualify_refs(url text, j jsonb) returns jsonb as $$
    SELECT regexp_replace(j::text, '"\$ref": "#', concat('"$ref": "', url, '#'), 'g')::jsonb;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE VIEW stac_extension_queryables AS
SELECT DISTINCT key as name, schema_qualify_refs(e.url, j.value) as definition FROM stac_extensions e, jsonb_each(e.content->'definitions'->'fields'->'properties') j;


CREATE OR REPLACE FUNCTION missing_queryables(_collection text, _tablesample float DEFAULT 5, minrows float DEFAULT 10) RETURNS TABLE(collection text, name text, definition jsonb, property_wrapper text) AS $$
DECLARE
    q text;
    _partition text;
    explain_json json;
    psize float;
    estrows float;
BEGIN
    SELECT format('_items_%s', key) INTO _partition FROM collections WHERE id=_collection;

    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM %I;', _partition)
    INTO explain_json;
    psize := explain_json->0->'Plan'->'Plan Rows';
    estrows := _tablesample * .01 * psize;
    IF estrows < minrows THEN
        _tablesample := least(100,greatest(_tablesample, (estrows / psize) / 100));
        RAISE NOTICE '%', (psize / estrows) / 100;
    END IF;
    RAISE NOTICE 'Using tablesample % to find missing queryables from % % that has ~% rows estrows: %', _tablesample, _collection, _partition, psize, estrows;

    q := format(
        $q$
            WITH q AS (
                SELECT * FROM queryables
                WHERE
                    collection_ids IS NULL
                    OR %L = ANY(collection_ids)
            ), t AS (
                SELECT
                    content->'properties' AS properties
                FROM
                    %I
                TABLESAMPLE SYSTEM(%L)
            ), p AS (
                SELECT DISTINCT ON (key)
                    key,
                    value,
                    s.definition
                FROM t
                JOIN LATERAL jsonb_each(properties) ON TRUE
                LEFT JOIN q ON (q.name=key)
                LEFT JOIN stac_extension_queryables s ON (s.name=key)
                WHERE q.definition IS NULL
            )
            SELECT
                %L,
                key,
                COALESCE(definition, jsonb_build_object('type',jsonb_typeof(value))) as definition,
                CASE
                    WHEN definition->>'type' = 'integer' THEN 'to_int'
                    WHEN COALESCE(definition->>'type', jsonb_typeof(value)) = 'number' THEN 'to_float'
                    WHEN COALESCE(definition->>'type', jsonb_typeof(value)) = 'array' THEN 'to_text_array'
                    ELSE 'to_text'
                END
            FROM p;
        $q$,
        _collection,
        _partition,
        _tablesample,
        _collection
    );
    RETURN QUERY EXECUTE q;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION missing_queryables(_tablesample float DEFAULT 5) RETURNS TABLE(collection_ids text[], name text, definition jsonb, property_wrapper text) AS $$
    SELECT
        array_agg(collection),
        name,
        definition,
        property_wrapper
    FROM
        collections
        JOIN LATERAL
        missing_queryables(id, _tablesample) c
        ON TRUE
    GROUP BY
        2,3,4
    ORDER BY 2,1
    ;
$$ LANGUAGE SQL;
