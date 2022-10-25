SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.get_queryables(_collection_ids text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
AS $function$
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
            SELECT
                jsonb_build_object(
                    '$schema', 'http://json-schema.org/draft-07/schema#',
                    '$id', 'https://example.org/queryables',
                    'type', 'object',
                    'title', 'STAC Queryables.',
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
        );
    ELSE
        RETURN NULL;
    END IF;
END;

$function$
;



SELECT set_version('0.6.10');
