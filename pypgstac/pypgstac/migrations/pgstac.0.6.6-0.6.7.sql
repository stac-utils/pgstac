SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;


INSERT INTO queryables (name, definition) VALUES
('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}'),
('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}'),
('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}')
ON CONFLICT DO NOTHING;

INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
('eo:cloud_cover','{"$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fieldsproperties/eo:cloud_cover"}','to_int','BTREE')
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION pgstac.get_queryables(_collection text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE sql
AS $function$
    SELECT
        CASE
            WHEN _collection IS NULL THEN get_queryables(NULL::text[])
            ELSE get_queryables(ARRAY[_collection])
        END
    ;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_queryables(_collection_ids text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        jsonb_build_object(
            '$schema', 'http://json-schema.org/draft-07/schema#',
            '$id', 'https://example.org/queryables',
            'type', 'object',
            'title', 'Stac Queryables.',
            'properties', jsonb_object_agg(
                name,
                definition
            )
        )
        FROM queryables
        WHERE
            _collection_ids IS NULL OR
            cardinality(_collection_ids) = 0 OR
            collection_ids IS NULL OR
            _collection_ids && collection_ids
        ;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.missing_queryables(_collection text, _tablesample integer DEFAULT 5)
 RETURNS TABLE(collection text, name text, definition jsonb, property_wrapper text)
 LANGUAGE plpgsql
AS $function$
DECLARE
    q text;
    _partition text;
    explain_json json;
    psize bigint;
BEGIN
    SELECT format('_items_%s', key) INTO _partition FROM collections WHERE id=_collection;

    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM %I;', _partition)
    INTO explain_json;
    psize := explain_json->0->'Plan'->'Plan Rows';
    IF _tablesample * .01 * psize < 10 THEN
        _tablesample := 100;
    END IF;
    RAISE NOTICE 'Using tablesample % to find missing queryables from % % that has ~% rows', _tablesample, _collection, _partition, psize;

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
                    value
                FROM t
                JOIN LATERAL jsonb_each(properties) ON TRUE
                LEFT JOIN q ON (q.name=key)
                WHERE q.definition IS NULL
            )
            SELECT
                %L,
                key,
                jsonb_build_object('type',jsonb_typeof(value)) as definition,
                CASE jsonb_typeof(value)
                    WHEN 'number' THEN 'to_float'
                    WHEN 'array' THEN 'to_text_array'
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
$function$
;

CREATE OR REPLACE FUNCTION pgstac.missing_queryables(_tablesample integer DEFAULT 5)
 RETURNS TABLE(collection_ids text[], name text, definition jsonb, property_wrapper text)
 LANGUAGE sql
AS $function$
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
$function$
;



SELECT set_version('0.6.7');
