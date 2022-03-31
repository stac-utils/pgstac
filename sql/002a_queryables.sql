DROP TABLE IF EXISTS queryables CASCADE;

CREATE TABLE queryables (
    id bigint GENERATED ALWAYS AS identity PRIMARY KEY,
    name text UNIQUE NOT NULL,
    collection_ids text[], -- used to determine what partitions to create indexes on
    definition jsonb,
    property_path text,
    property_wrapper text,
    property_index_type text
);
CREATE INDEX queryables_name_idx ON queryables (name);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);




INSERT INTO queryables (name, definition) VALUES
('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}'),
('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}')
ON CONFLICT DO NOTHING;



INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
('eo:cloud_cover','{"$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fieldsproperties/eo:cloud_cover"}','to_int','BTREE'),
('platform','{}','to_text','BTREE')
ON CONFLICT DO NOTHING;

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
    OUT wrapper text
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
    SELECT * INTO q FROM queryables WHERE name=dotpath;
    IF q.property_wrapper IS NULL THEN
        IF q.definition->>'type' = 'number' THEN
            wrapper := 'to_float';
        ELSIF q.definition->>'format' = 'date-time' THEN
            wrapper := 'to_tstz';
        ELSE
            wrapper := 'to_text';
        END IF;
    ELSE
        wrapper := q.property_wrapper;
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

CREATE OR REPLACE FUNCTION create_queryable_indexes() RETURNS VOID AS $$
DECLARE
    queryable RECORD;
    q text;
BEGIN
    FOR queryable IN
        SELECT
            queryables.id as qid,
            CASE WHEN collections.key IS NULL THEN 'items' ELSE format('_items_%s',collections.key) END AS part,
            property_index_type,
            expression
            FROM
            queryables
            LEFT JOIN collections ON (collections.id = ANY (queryables.collection_ids))
            JOIN LATERAL queryable(queryables.name) ON (queryables.property_index_type IS NOT NULL)
        LOOP
        q := format(
            $q$
                CREATE INDEX IF NOT EXISTS %I ON %I USING %s ((%s));
            $q$,
            format('%s_%s_idx', queryable.part, queryable.qid),
            queryable.part,
            COALESCE(queryable.property_index_type, 'to_text'),
            queryable.expression
            );
        RAISE NOTICE '%',q;
        EXECUTE q;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
