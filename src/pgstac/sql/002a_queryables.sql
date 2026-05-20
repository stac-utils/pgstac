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

-- queryable_uses_native_path: Returns true when a queryable path string is a
-- bare identifier (e.g. 'proj_epsg', 'platform') that maps to a native promoted
-- column on the items table. Such paths do not need a content->'properties'->...
-- expression or a type-cast wrapper; the column type already matches.
CREATE OR REPLACE FUNCTION queryable_uses_native_path(path text) RETURNS boolean AS $$
    SELECT path ~ '^[a-zA-Z_][a-zA-Z0-9_]*$';
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;




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
            -- links, assets, stac_version, stac_extensions are now split columns.
            IF array_length(path_elements, 1) = 1 THEN
                path := path_elements[1];
            ELSE
                path := format('%I->%s', path_elements[1], array_to_path(path_elements[2:]));
            END IF;
        ELSIF path_elements[1] = 'properties' THEN
            -- properties is a split JSONB column; generate properties->... path.
            IF array_length(path_elements, 1) = 1 THEN
                path := 'properties';
            ELSE
                path := format('properties->%s', array_to_path(path_elements[2:]));
            END IF;
        ELSE
            -- Non-prefixed queryable names are assumed to live in properties.
            path := format($F$properties->%s$F$, array_to_path(path_elements));
        END IF;
    END IF;
    IF queryable_uses_native_path(path) THEN
        IF q.definition->>'type' IN ('number', 'integer') OR q.property_wrapper IN ('to_int', 'to_float') THEN
            wrapper := 'to_float';
            nulled_wrapper := wrapper;
        ELSIF q.definition->>'format' = 'date-time' THEN
            wrapper := 'to_tstz';
            nulled_wrapper := wrapper;
        ELSIF q.property_wrapper IS NULL THEN
            wrapper := 'to_text';
            nulled_wrapper := NULL;
        END IF;
    END IF;
    IF wrapper IS NULL OR queryable_uses_native_path(path) THEN
        expression := path;
    ELSE
        expression := format('%I(%s)', wrapper, path);
    END IF;
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

-- queryable_index_field: Returns the index field name for a queryable row.
-- For promoted native columns (property_path is a bare identifier) the field name
-- is the column name itself. For JSON-path queryables it is the STAC property name.
-- Used by the index consistency view to correlate existing indexes with queryables.
CREATE OR REPLACE FUNCTION queryable_index_field(q queryables) RETURNS text AS $$
    SELECT CASE
        WHEN q.property_path IS NOT NULL AND queryable_uses_native_path(q.property_path) THEN q.property_path
        ELSE q.name
    END;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


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
        ELSIF q.property_path IS NOT NULL AND queryable_uses_native_path(q.property_path) THEN
            -- Native promoted column: index the column directly, no type-cast wrapper needed.
            out := format(
                'CREATE INDEX ON %%I USING %s (%s)',
                lower(COALESCE(q.property_index_type, 'BTREE')),
                q.property_path
            );
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
        substring(indexdef FROM '\(([a-zA-Z0-9_]+)\)'),
        substring(indexdef FROM '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'),
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
        substring(indexdef FROM '\(([a-zA-Z0-9_]+)\)'),
        substring(indexdef FROM '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_]+)''::text'),
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
                substring(indexdef FROM '\(([a-zA-Z0-9_]+)\)'),
                substring(indexdef FROM '\(content -> ''properties''::text\) -> ''([a-zA-Z0-9\:\_-]+)''::text'),
                CASE WHEN indexdef ~* '\(datetime desc, end_datetime\)' THEN 'datetime' ELSE NULL END
            ) AS field
        FROM
            pg_indexes
            JOIN p ON (tablename=partition)
    ), q AS (
        SELECT
            queryable_index_field(queryables) AS field,
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
            'SELECT maintain_index(
                %L,%L,%L,%L,%L
            );',
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
                    properties
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

-- promoted_queryables_defaults: Single source of truth for the promoted native-column
-- queryable seed data.  Called by 998_idempotent_post.sql for both the INSERT (new rows)
-- and UPDATE (backfill existing rows that have property_path=NULL) passes so the
-- values list only needs to be maintained in one place.
CREATE OR REPLACE FUNCTION promoted_queryables_defaults()
RETURNS TABLE (
    name            text,
    definition      jsonb,
    property_path   text,
    property_wrapper text
) AS $$
    SELECT * FROM (VALUES
      ('stac_version',       '{"description": "STAC specification version","type": "string","title": "STAC Version"}'::jsonb,                                            'stac_version',       'to_text'),
      ('stac_extensions',    '{"description": "List of STAC extension schema URIs","type": "array","title": "STAC Extensions"}'::jsonb,                                  'stac_extensions',    'to_text'),
      ('created',            '{"description": "Metadata creation timestamp","type": "string","format": "date-time","title": "Created"}'::jsonb,                          'created',            'to_tstz'),
      ('updated',            '{"description": "Metadata update timestamp","type": "string","format": "date-time","title": "Updated"}'::jsonb,                            'updated',            'to_tstz'),
      ('platform',           '{"description": "Platform name","type": "string","title": "Platform"}'::jsonb,                                                             'platform',           'to_text'),
      ('instruments',        '{"description": "Instrument names","type": "array","title": "Instruments"}'::jsonb,                                                        'instruments',        'to_text_array'),
      ('constellation',      '{"description": "Constellation name","type": "string","title": "Constellation"}'::jsonb,                                                  'constellation',      'to_text'),
      ('mission',            '{"description": "Mission name","type": "string","title": "Mission"}'::jsonb,                                                               'mission',            'to_text'),
      ('eo:cloud_cover',     '{"description": "EO cloud cover percentage","type": "number","title": "Cloud Cover"}'::jsonb,                                              'eo_cloud_cover',     'to_float'),
      ('eo:bands',           '{"description": "EO band metadata","type": "array","title": "EO Bands"}'::jsonb,                                                           'eo_bands',           'to_text'),
      ('eo:snow_cover',      '{"description": "EO snow cover percentage","type": "number","title": "Snow Cover"}'::jsonb,                                                'eo_snow_cover',      'to_float'),
      ('gsd',                '{"description": "Ground sample distance","type": "number","title": "Ground Sample Distance"}'::jsonb,                                      'gsd',                'to_float'),
      ('proj:epsg',          '{"description": "EPSG code","type": "integer","title": "Projection EPSG"}'::jsonb,                                                         'proj_epsg',          'to_int'),
      ('proj:wkt2',          '{"description": "WKT2 CRS definition","type": "string","title": "Projection WKT2"}'::jsonb,                                                'proj_wkt2',          'to_text'),
      ('proj:projjson',      '{"description": "PROJJSON CRS definition","type": ["object", "string"],"title": "Projection PROJJSON"}'::jsonb,                           'proj_projjson',      'to_text'),
      ('proj:bbox',          '{"description": "Projection bbox","type": "array","title": "Projection BBOX"}'::jsonb,                                                    'proj_bbox',          'to_text'),
      ('proj:centroid',      '{"description": "Projection centroid","type": "object","title": "Projection Centroid"}'::jsonb,                                           'proj_centroid',      'to_text'),
      ('proj:shape',         '{"description": "Projection shape","type": "array","title": "Projection Shape"}'::jsonb,                                                  'proj_shape',         'to_text'),
      ('proj:transform',     '{"description": "Projection affine transform","type": "array","title": "Projection Transform"}'::jsonb,                                   'proj_transform',     'to_text'),
      ('sci:doi',            '{"description": "Scientific DOI","type": "string","title": "Scientific DOI"}'::jsonb,                                                     'sci_doi',            'to_text'),
      ('sci:citation',       '{"description": "Scientific citation","type": "string","title": "Scientific Citation"}'::jsonb,                                           'sci_citation',       'to_text'),
      ('sci:publications',   '{"description": "Scientific publications","type": "array","title": "Scientific Publications"}'::jsonb,                                    'sci_publications',   'to_text'),
      ('view:off_nadir',     '{"description": "Viewing angle off nadir","type": "number","title": "View Off Nadir"}'::jsonb,                                             'view_off_nadir',     'to_float'),
      ('view:incidence_angle','{"description": "View incidence angle","type": "number","title": "View Incidence Angle"}'::jsonb,                                        'view_incidence_angle','to_float'),
      ('view:azimuth',       '{"description": "View azimuth angle","type": "number","title": "View Azimuth"}'::jsonb,                                                   'view_azimuth',       'to_float'),
      ('view:sun_azimuth',   '{"description": "Sun azimuth angle","type": "number","title": "View Sun Azimuth"}'::jsonb,                                                 'view_sun_azimuth',   'to_float'),
      ('view:sun_elevation', '{"description": "Sun elevation angle","type": "number","title": "View Sun Elevation"}'::jsonb,                                             'view_sun_elevation', 'to_float'),
      ('file:size',          '{"description": "File size in bytes","type": "integer","title": "File Size"}'::jsonb,                                                     'file_size',          'to_int'),
      ('file:header_size',   '{"description": "File header size in bytes","type": "integer","title": "File Header Size"}'::jsonb,                                       'file_header_size',   'to_int'),
      ('file:checksum',      '{"description": "File checksum","type": "string","title": "File Checksum"}'::jsonb,                                                       'file_checksum',      'to_text'),
      ('file:byte_order',    '{"description": "File byte order","type": "string","title": "File Byte Order"}'::jsonb,                                                   'file_byte_order',    'to_text'),
      ('file:values_regex',  '{"description": "File values regex","type": "string","title": "File Values Regex"}'::jsonb,                                               'file_values_regex',  'to_text'),
      ('sat:orbit_state',    '{"description": "Satellite orbit state","type": "string","title": "Orbit State"}'::jsonb,                                                 'sat_orbit_state',    'to_text'),
      ('sat:relative_orbit', '{"description": "Satellite relative orbit","type": "integer","title": "Relative Orbit"}'::jsonb,                                          'sat_relative_orbit', 'to_int'),
      ('sat:absolute_orbit', '{"description": "Satellite absolute orbit","type": "integer","title": "Absolute Orbit"}'::jsonb,                                          'sat_absolute_orbit', 'to_int')
    ) AS t(name, definition, property_path, property_wrapper);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
