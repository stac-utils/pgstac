-- collection_fragment_config_default: Derive a fragment_config text[] from item_assets if present.
-- Returns one serialized path per item_asset key (e.g. 'assets.thumbnail'), or NULL if no item_assets.
-- This is used when creating/upserting a collection to auto-populate fragment_config.
CREATE OR REPLACE FUNCTION collection_fragment_config_default(content jsonb) RETURNS text[] AS $$
    SELECT CASE
        WHEN content->'item_assets' IS NOT NULL
             AND jsonb_typeof(content->'item_assets') = 'object'
             AND content->'item_assets' != '{}'::jsonb
        THEN ARRAY(
            SELECT fragment_path_text(ARRAY['assets', k])
            FROM jsonb_object_keys(content->'item_assets') k
        )
        ELSE NULL
    END;
$$ LANGUAGE SQL STABLE;


CREATE TABLE IF NOT EXISTS collections (
    key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id text GENERATED ALWAYS AS (content->>'id') STORED UNIQUE NOT NULL,
    content JSONB NOT NULL,
    -- fragment_config: list of serialized root-relative fragment paths (fragment_path_text format).
    -- NULL means no fragmentation for this collection.
    -- Each element is a dot-delimited path like 'assets.thumbnail' or 'properties.eo:cloud_cover'.
    fragment_config text[],
    geometry geometry GENERATED ALWAYS AS (pgstac.collection_geom(content)) STORED,
    datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_datetime(content)) STORED,
    end_datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_enddatetime(content)) STORED,
    private jsonb,
    partition_trunc text CHECK (partition_trunc IN ('year', 'month'))
);

-- create_collection: Insert a new collection and derive fragment_config from item_assets.
CREATE OR REPLACE FUNCTION create_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content, fragment_config)
    VALUES (data, collection_fragment_config_default(data))
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

-- update_collection: Replace collection content. Does NOT update fragment_config
-- so operator-configured paths survive content updates.
CREATE OR REPLACE FUNCTION update_collection(data jsonb) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    UPDATE collections SET content=data WHERE id = data->>'id' RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- upsert_collection: Insert or update a collection.
-- On conflict, preserves any operator-set fragment_config; only populates it
-- from the item_assets default when it is currently NULL.
CREATE OR REPLACE FUNCTION upsert_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content, fragment_config)
    VALUES (data, collection_fragment_config_default(data))
    ON CONFLICT (id) DO
    UPDATE
        SET content=EXCLUDED.content,
            -- Preserve any operator-configured fragment_config; only set from default if currently NULL.
            fragment_config=COALESCE(collections.fragment_config, EXCLUDED.fragment_config)
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION delete_collection(_id text) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    DELETE FROM collections WHERE id = _id RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
    SELECT content FROM collections
    WHERE id=$1
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION all_collections() RETURNS jsonb AS $$
    SELECT coalesce(jsonb_agg(content), '[]'::jsonb) FROM collections;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION collection_delete_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    collection_base_partition text := concat('_items_', OLD.key);
BEGIN
    EXECUTE format($q$
        DELETE FROM partition_stats WHERE partition IN (
            SELECT partition FROM partition_sys_meta
            WHERE collection=%L
        );
        DROP TABLE IF EXISTS %I CASCADE;
        $q$,
        OLD.id,
        collection_base_partition
    );
    RETURN OLD;
END;
$$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS collection_delete_trigger ON collections;
CREATE TRIGGER collection_delete_trigger BEFORE DELETE ON collections
FOR EACH ROW EXECUTE FUNCTION collection_delete_trigger_func();
