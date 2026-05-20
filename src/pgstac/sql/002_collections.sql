-- collection_fragment_config_default: Derive a sensible starting fragment_config for a
-- collection.  Returns the paths for values that are reliably identical across every item
-- in any STAC collection:
--
--   • stac_version    — set once at the collection level, never varies per item.
--   • stac_extensions — same extension list for every item in a collection.
--
-- NOTE: asset sub-fields (e.g. type, title, roles) from item_assets are also good
-- candidates for fragmentation because they are constant per collection.  However,
-- the current extract_fragment implementation supports depth-1 and depth-2 paths only;
-- a depth-2 path like 'assets.thumbnail' captures the *whole* asset object, including
-- 'href', which is unique per item and eliminates any dedup benefit.  Proper support
-- requires depth-3 paths (e.g. 'assets.thumbnail.type') so that 'href' can be excluded.
-- Until that is implemented, asset paths are NOT included in the default to avoid
-- creating one unique fragment per item.  Operators may add explicit fragment_config
-- paths after collection creation via UPDATE collections SET fragment_config = ...
CREATE OR REPLACE FUNCTION collection_fragment_config_default(content jsonb) RETURNS text[] AS $$
    SELECT ARRAY['stac_version', 'stac_extensions'];
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

-- create_collection: Insert a new collection.
-- _partition_trunc: optional 'year' or 'month' sub-partitioning; defaults to none.
-- _fragment_config: explicit fragment path list; defaults to collection_fragment_config_default(data).
CREATE OR REPLACE FUNCTION create_collection(
    data jsonb,
    _partition_trunc text DEFAULT NULL,
    _fragment_config text[] DEFAULT NULL
) RETURNS VOID AS $$
    INSERT INTO collections (content, fragment_config, partition_trunc)
    VALUES (
        data,
        COALESCE(_fragment_config, collection_fragment_config_default(data)),
        _partition_trunc
    )
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

-- update_collection: Replace collection content.
-- _partition_trunc: when not NULL, updates the partition truncation setting.
--   Pass NULL to preserve the existing value.
-- _fragment_config: when not NULL, replaces the fragment configuration.
--   Pass NULL to preserve the existing operator-configured value.
CREATE OR REPLACE FUNCTION update_collection(
    data jsonb,
    _partition_trunc text DEFAULT NULL,
    _fragment_config text[] DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    UPDATE collections
    SET content         = data,
        partition_trunc = COALESCE(_partition_trunc, partition_trunc),
        fragment_config = COALESCE(_fragment_config, fragment_config)
    WHERE id = data->>'id'
    RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- upsert_collection: Insert or update a collection.
-- _partition_trunc: optional 'year' or 'month' sub-partitioning; defaults to none on insert,
--   preserved from existing row on conflict (pass an explicit value to override).
-- _fragment_config: explicit fragment path list; defaults to collection_fragment_config_default(data)
--   on insert.  On conflict, any operator-configured (non-NULL) value is preserved unless an
--   explicit _fragment_config is passed.
CREATE OR REPLACE FUNCTION upsert_collection(
    data jsonb,
    _partition_trunc text DEFAULT NULL,
    _fragment_config text[] DEFAULT NULL
) RETURNS VOID AS $$
    INSERT INTO collections (content, fragment_config, partition_trunc)
    VALUES (
        data,
        COALESCE(_fragment_config, collection_fragment_config_default(data)),
        _partition_trunc
    )
    ON CONFLICT (id) DO
    UPDATE
        SET content         = EXCLUDED.content,
            -- Preserve any operator-configured fragment_config; only replace when an
            -- explicit _fragment_config was supplied or when currently NULL.
            fragment_config = CASE
                WHEN _fragment_config IS NOT NULL THEN _fragment_config
                ELSE COALESCE(collections.fragment_config, EXCLUDED.fragment_config)
            END,
            partition_trunc = COALESCE(EXCLUDED.partition_trunc, collections.partition_trunc)
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
