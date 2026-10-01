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
    id text GENERATED ALWAYS AS (content->>'id') STORED UNIQUE NOT NULL,
    content JSONB NOT NULL,
    base_item jsonb GENERATED ALWAYS AS (pgstac.collection_base_item(content)) STORED,
    geometry geometry GENERATED ALWAYS AS (pgstac.collection_geom(content)) STORED,
    datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_datetime(content)) STORED,
    end_datetime timestamptz GENERATED ALWAYS AS (pgstac.collection_enddatetime(content)) STORED,
    private jsonb,
    partition_trunc text CHECK (partition_trunc IN ('year', 'month'))
);

-- Base items a collection has had since its first edit; none until then.
CREATE TABLE IF NOT EXISTS base_items (
    -- ON UPDATE CASCADE because collections.id is generated from the content: rewriting a
    -- collection's id would otherwise orphan these rows under the old key, and a collection
    -- later recreated with that id would inherit base items it was never dehydrated against.
    collection text NOT NULL REFERENCES collections(id) ON DELETE CASCADE ON UPDATE CASCADE,
    id int GENERATED ALWAYS AS IDENTITY,
    base_item jsonb NOT NULL,
    PRIMARY KEY (collection, id)
);

CREATE OR REPLACE FUNCTION collection_base_item(cid text, _base_item_id int DEFAULT NULL) RETURNS jsonb AS $$
    SELECT CASE
        WHEN _base_item_id IS NOT NULL THEN
            (SELECT base_item FROM pgstac.base_items WHERE collection = cid AND id = _base_item_id)
        ELSE coalesce(
            (SELECT base_item FROM pgstac.base_items WHERE collection = cid ORDER BY id ASC LIMIT 1),
            (SELECT base_item FROM pgstac.collections WHERE id = cid)
        )
    END;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

-- The collection's current base item and its base_items id (NULL if never edited).
CREATE OR REPLACE FUNCTION current_base_item(cid text, OUT base_item_id int, OUT base_item jsonb) AS $$
    -- The id only when that row really holds the current base item: these are two independent
    -- reads, and a tag naming anything else hydrates against content the item was not dehydrated
    -- from, silently. Before the first edit there are no rows, so the id is NULL and the item is
    -- stored untagged.
    SELECT b.id, c.base_item
    FROM pgstac.collections c
    LEFT JOIN LATERAL (
        SELECT id FROM pgstac.base_items
        WHERE collection = c.id AND base_item = c.base_item
        ORDER BY id DESC LIMIT 1
    ) b ON TRUE
    WHERE c.id = cid;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;


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


-- SECURITY DEFINER: the delete trigger drops partition tables, which are owned
-- by pgstac_admin.
CREATE OR REPLACE FUNCTION delete_collection(_id text) RETURNS VOID AS $$
BEGIN
    DELETE FROM collections WHERE id = _id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Collection % does not exist', _id USING ERRCODE = 'no_data_found';
    END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
    SELECT content FROM collections
    WHERE id=$1
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION all_collections() RETURNS jsonb AS $$
    SELECT coalesce(jsonb_agg(content), '[]'::jsonb) FROM collections;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

-- SECURITY DEFINER: the partitions dropped here are owned by pgstac_admin.
CREATE OR REPLACE FUNCTION collection_delete_trigger_func() RETURNS TRIGGER AS $$
DECLARE
    collection_base_partition text := concat('_items_', OLD.key);
BEGIN
    -- A recreated collection id must not inherit these rows.
    DELETE FROM base_items WHERE collection = OLD.id;
    -- A queryable that applies only to this collection goes with it; the others drop it from
    -- their collection_ids.
    DELETE FROM queryables WHERE collection_ids = ARRAY[OLD.id];
    UPDATE queryables SET collection_ids = array_remove(collection_ids, OLD.id) WHERE OLD.id = ANY(collection_ids);
    -- Tables before rows: check_partition takes these locks in the same order,
    -- and the reverse deadlocks against a concurrent partition create.
    EXECUTE format($q$
        DROP TABLE IF EXISTS %I CASCADE;
        DELETE FROM partition_stats WHERE collection=%L;
        $q$,
        collection_base_partition,
        OLD.id
    );
    RETURN OLD;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collection_delete_trigger BEFORE DELETE ON collections
FOR EACH ROW EXECUTE FUNCTION collection_delete_trigger_func();
