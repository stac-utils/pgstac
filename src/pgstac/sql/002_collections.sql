CREATE OR REPLACE FUNCTION collection_base_item(content jsonb) RETURNS jsonb AS $$
    SELECT jsonb_build_object(
        'type', 'Feature',
        'stac_version', content->'stac_version',
        'assets', content->'item_assets',
        'collection', content->'id'
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE TABLE IF NOT EXISTS collections (
    key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id text GENERATED ALWAYS AS (content->>'id') STORED UNIQUE,
    content JSONB NOT NULL,
    base_item jsonb GENERATED ALWAYS AS (pgstac.collection_base_item(content)) STORED,
    geometry geometry GENERATED ALWAYS AS (pgstac.collection_geom(content)) STORED,
    datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_datetime(content)) STORED,
    end_datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_enddatetime(content)) STORED,
    private jsonb,
    partition_trunc text CHECK (partition_trunc IN ('year', 'month'))
);

CREATE OR REPLACE FUNCTION collection_base_item(cid text) RETURNS jsonb AS $$
    SELECT pgstac.collection_base_item(content) FROM pgstac.collections WHERE id = cid LIMIT 1;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION create_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION update_collection(data jsonb) RETURNS VOID AS $$
DECLARE
    out collections%ROWTYPE;
BEGIN
    UPDATE collections SET content=data WHERE id = data->>'id' RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ON CONFLICT (id) DO
    UPDATE
        SET content=EXCLUDED.content
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

CREATE TRIGGER collection_delete_trigger BEFORE DELETE ON collections
FOR EACH ROW EXECUTE FUNCTION collection_delete_trigger_func();
