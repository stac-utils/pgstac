-- collection_fragment_config_default: Derive a sensible starting fragment_config for a
-- collection.  Returns paths for values that are reliably identical across every item
-- in a STAC collection:
--
--   • stac_version    — set once at the collection level, never varies per item.
--   • stac_extensions — same extension list for every item in a collection.
--
-- When item_assets is present, depth-3 paths are generated for each stable asset sub-key
-- (e.g. 'assets.B1.type', 'assets.B1.roles') so that the fragment stores the shared
-- per-asset metadata while the per-item column retains only the fields that vary between
-- items (primarily 'href').  Known per-item-varying sub-keys are excluded.
--
-- The fragment system now supports paths at arbitrary depth, so these depth-3 paths are
-- handled correctly by extract_fragment and strip_fragment_col.
CREATE OR REPLACE FUNCTION collection_fragment_config_default(content jsonb) RETURNS text[] AS $$
DECLARE
    paths text[] := ARRAY['stac_version', 'stac_extensions'];
    asset_key text;
    sub_key   text;
    -- Fields within each asset that differ between items and must NOT be fragmented.
    -- href is unique per item; file:* fields reflect per-file measurements;
    -- alternate/storage:* are access-layer derived paths also unique per item.
    per_item_asset_fields CONSTANT text[] := ARRAY[
        'href',
        'file:size', 'file:checksum', 'file:local_path',
        'alternate',
        'storage:path', 'storage:platform', 'storage:region',
        'storage:requester_pays', 'storage:tier'
    ];
BEGIN
    -- Always include top-level fields that are identical across all items in a collection.
    -- stac_version and stac_extensions are set at the collection level and never vary per item.

    -- For item_assets: each asset key's sub-keys (except known per-item fields like href)
    -- are the same for every item in the collection because item_assets describes the
    -- collection-level asset schema (type, title, roles, eo:bands, raster:bands, etc.).
    -- Using depth-3 paths means only the stable metadata is fragmented; href and other
    -- per-item fields stay in the per-item assets column so the dedup still works.
    IF content->'item_assets' IS NOT NULL
       AND jsonb_typeof(content->'item_assets') = 'object'
       AND content->'item_assets' != '{}'::jsonb
    THEN
        FOR asset_key IN SELECT jsonb_object_keys(content->'item_assets') LOOP
            FOR sub_key IN SELECT jsonb_object_keys(content->'item_assets'->asset_key) LOOP
                IF NOT (sub_key = ANY(per_item_asset_fields)) THEN
                    paths := paths || fragment_path_text(ARRAY['assets', asset_key, sub_key]);
                END IF;
            END LOOP;
        END LOOP;
    END IF;

    RETURN paths;
END;
$$ LANGUAGE PLPGSQL STABLE;


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
