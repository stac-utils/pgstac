SET SEARCH_PATH TO pgstac, public;

CREATE TABLE IF NOT EXISTS collections (
    id VARCHAR GENERATED ALWAYS AS (content->>'id') STORED PRIMARY KEY,
    content JSONB
);

CREATE OR REPLACE FUNCTION create_collections(data jsonb) RETURNS jsonb AS $$
    WITH newcollections AS (
        SELECT value FROM jsonb_array_elements('[]'::jsonb || data)
    )
        INSERT INTO collections (content)
        SELECT value FROM newcollections
        ON CONFLICT (id) DO
        UPDATE
            SET content=EXCLUDED.content
    ;
    WITH newcollections AS (
        SELECT data->>'id' as id FROM jsonb_array_elements('[]'::jsonb || data)
    )
    SELECT jsonb_agg(content) FROM collections WHERE id IN (SELECT id FROM newcollections);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
SELECT content FROM collections
WHERE id=$1
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION all_collections(_limit int = 10, _offset int = 0, _token varchar = NULL) RETURNS SETOF jsonb AS $$
SELECT content FROM collections
WHERE
    CASE
        WHEN _token is NULL THEN TRUE
        ELSE id > _token
    END
ORDER BY id ASC
OFFSET _offset
LIMIT _limit
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;



/* staging table and triggers that allows using copy directly from ndjson */
CREATE UNLOGGED TABLE IF NOT EXISTS collections_staging (data jsonb);

CREATE OR REPLACE FUNCTION collections_staging_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM create_collections(NEW.data);
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collections_staging_trigger
BEFORE INSERT ON collections_staging
FOR EACH ROW EXECUTE PROCEDURE collections_staging_trigger_func();
