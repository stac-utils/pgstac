CREATE OR REPLACE FUNCTION queryable_signature(n text, c text[]) RETURNS text AS $$
    SELECT concat(n, c);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

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
CREATE INDEX queryables_collection_idx ON queryables USING GIN (collection_ids);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);

CREATE OR REPLACE FUNCTION pgstac.queryables_constraint_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    allcollections text[];
BEGIN
    RAISE NOTICE 'Making sure that name/collection is unique for queryables %', NEW;
    IF NEW.collection_ids IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
                FROM unnest(NEW.collection_ids) c
                LEFT JOIN
                collections
                ON (collections.id = c)
                WHERE collections.id IS NULL
        ) THEN
            RAISE foreign_key_violation USING MESSAGE = format(
                'One or more collections in %s do not exist.', NEW.collection_ids
            );
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF EXISTS (
            SELECT 1 FROM queryables q
            WHERE
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                )
        ) THEN
            RAISE unique_violation USING MESSAGE = format(
                'There is already a queryable for %s for a collection in %s: %s',
                NEW.name,
                NEW.collection_ids,
				(SELECT json_agg(row_to_json(q)) FROM queryables q WHERE
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                ))
            );
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF EXISTS (
            SELECT 1 FROM queryables q
            WHERE
                q.id != NEW.id
                AND
                q.name = NEW.name
                AND (
                    q.collection_ids && NEW.collection_ids
                    OR
                    q.collection_ids IS NULL
                    OR
                    NEW.collection_ids IS NULL
                )
        ) THEN
            RAISE unique_violation
            USING MESSAGE = format(
                'There is already a queryable for %s for a collection in %s',
                NEW.name,
                NEW.collection_ids
            );
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER queryables_constraint_insert_trigger
BEFORE INSERT ON queryables
FOR EACH ROW EXECUTE PROCEDURE queryables_constraint_triggerfunc();

CREATE TRIGGER queryables_constraint_update_trigger
BEFORE UPDATE ON queryables
FOR EACH ROW
WHEN (NEW.name = OLD.name AND NEW.collection_ids IS DISTINCT FROM OLD.collection_ids)
EXECUTE PROCEDURE queryables_constraint_triggerfunc();


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
    dotpath := replace(dotpath, 'properties.', '');
    IF dotpath = 'start_datetime' THEN
        dotpath := 'datetime';
    END IF;
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

CREATE OR REPLACE FUNCTION unnest_collection(collection_ids text[] DEFAULT NULL) RETURNS SETOF text AS $$
    DECLARE
    BEGIN
        IF collection_ids IS NULL THEN
            RETURN QUERY SELECT id FROM collections;
        END IF;
        RETURN QUERY SELECT unnest(collection_ids);
    END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION normalize_indexdef(def text) RETURNS text AS $$
DECLARE
BEGIN
    def := btrim(def, ' \n\t');
	def := regexp_replace(def, '^CREATE (UNIQUE )?INDEX ([^ ]* )?ON (ONLY )?([^ ]* )?', '', 'i');
    RETURN def;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION indexdef(q queryables) RETURNS text AS $$
    DECLARE
        out text;
    BEGIN
        IF q.name = 'id' THEN
            out := 'CREATE UNIQUE INDEX ON %I USING btree (id)';
        ELSIF q.name = 'datetime' THEN
            out := 'CREATE INDEX ON %I USING btree (datetime DESC, end_datetime)';
        ELSIF q.name = 'geometry' THEN
            out := 'CREATE INDEX ON %I USING gist (geometry)';
        ELSE
            out := format($q$CREATE INDEX ON %%I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
                lower(COALESCE(q.property_index_type, 'BTREE')),
                lower(COALESCE(q.property_wrapper, 'to_text')),
                q.name
            );
        END IF;
        RETURN btrim(out, ' \n\t');
    END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

DROP VIEW IF EXISTS pgstac_indexes;
CREATE VIEW pgstac_indexes AS
SELECT
    i.schemaname,
    i.tablename,
    i.indexname,
    regexp_replace(btrim(replace(replace(indexdef, i.indexname, ''),'pgstac.',''),' \t\n'), '[ ]+', ' ', 'g') as idx,
    COALESCE(
        (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
        (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'))[1],
        CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime' ELSE NULL END
    ) AS field,
    pg_table_size(i.indexname::text) as index_size,
    pg_size_pretty(pg_table_size(i.indexname::text)) as index_size_pretty
FROM
    pg_indexes i
WHERE i.schemaname='pgstac' and i.tablename ~ '_items_' AND indexdef !~* ' only ';

DROP VIEW IF EXISTS pgstac_index_stats;
CREATE VIEW pgstac_indexes_stats AS
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

CREATE OR REPLACE FUNCTION queryable_indexes(
    IN treeroot text DEFAULT 'items',
    IN changes boolean DEFAULT FALSE,
    OUT collection text,
    OUT partition text,
    OUT field text,
    OUT indexname text,
    OUT existing_idx text,
    OUT queryable_idx text
) RETURNS SETOF RECORD AS $$
WITH p AS (
        SELECT
            relid::text as partition,
            replace(replace(
                CASE
                    WHEN parentrelid::regclass::text='items' THEN pg_get_expr(c.relpartbound, c.oid)
                    ELSE pg_get_expr(parent.relpartbound, parent.oid)
                END,
                'FOR VALUES IN (''',''), ''')',
                ''
            ) AS collection
        FROM pg_partition_tree(treeroot)
        JOIN pg_class c ON (relid::regclass = c.oid)
        JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    ), i AS (
        SELECT
            partition,
            indexname,
            regexp_replace(btrim(replace(replace(indexdef, indexname, ''),'pgstac.',''),' \t\n'), '[ ]+', ' ', 'g') as iidx,
            COALESCE(
                (regexp_match(indexdef, '\(([a-zA-Z]+)\)'))[1],
                (regexp_match(indexdef,  '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'))[1],
                CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime' ELSE NULL END
            ) AS field
        FROM
            pg_indexes
            JOIN p ON (tablename=partition)
    ), q AS (
        SELECT
            name AS field,
            collection,
            partition,
            format(indexdef(queryables), partition) as qidx
        FROM queryables, unnest_collection(queryables.collection_ids) collection
            JOIN p USING (collection)
        WHERE property_index_type IS NOT NULL OR name IN ('datetime','geometry','id')
    )
    SELECT
        collection,
        partition,
        field,
        indexname,
        iidx as existing_idx,
        qidx as queryable_idx
    FROM i FULL JOIN q USING (field, partition)
    WHERE CASE WHEN changes THEN lower(iidx) IS DISTINCT FROM lower(qidx) ELSE TRUE END;
;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION maintain_index(
    indexname text,
    queryable_idx text,
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE,
    idxconcurrently boolean DEFAULT FALSE
) RETURNS VOID AS $$
DECLARE
BEGIN
    IF indexname IS NOT NULL THEN
        IF dropindexes OR queryable_idx IS NOT NULL THEN
            EXECUTE format('DROP INDEX IF EXISTS %I;', indexname);
        ELSIF rebuildindexes THEN
            IF idxconcurrently THEN
                EXECUTE format('REINDEX INDEX CONCURRENTLY %I;', indexname);
            ELSE
                EXECUTE format('REINDEX INDEX CONCURRENTLY %I;', indexname);
            END IF;
        END IF;
    END IF;
    IF queryable_idx IS NOT NULL THEN
        IF idxconcurrently THEN
            EXECUTE replace(queryable_idx, 'INDEX', 'INDEX CONCURRENTLY');
        ELSE EXECUTE queryable_idx;
        END IF;
    END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;



set check_function_bodies to off;
CREATE OR REPLACE FUNCTION maintain_partition_queries(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE,
    idxconcurrently boolean DEFAULT FALSE
) RETURNS SETOF text AS $$
DECLARE
   rec record;
   q text;
BEGIN
    FOR rec IN (
        SELECT * FROM queryable_indexes(part,true)
    ) LOOP
        q := format(
            'SELECT maintain_index(%L, %L, %L, %L, %L);',
            rec.indexname,
            rec.queryable_idx,
            dropindexes,
            rebuildindexes,
            idxconcurrently
        );
        RAISE NOTICE 'Q: %', q;
        RETURN NEXT q;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION maintain_partitions(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partition_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION queryables_trigger_func() RETURNS TRIGGER AS $$
DECLARE
BEGIN
    PERFORM maintain_partitions();
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER queryables_trigger AFTER INSERT OR UPDATE ON queryables
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();


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
                    ),
                    'additionalProperties', pgstac.additional_properties()
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
