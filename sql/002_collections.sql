SET SEARCH_PATH TO pgstac, public;

CREATE TABLE IF NOT EXISTS collections (
    id VARCHAR GENERATED ALWAYS AS (content->>'id') STORED PRIMARY KEY,
    content JSONB
);

CREATE OR REPLACE FUNCTION create_collection(data jsonb) RETURNS VOID AS $$
    INSERT INTO collections (content)
    VALUES (data)
    ON CONFLICT (id) DO
    UPDATE
        SET content=EXCLUDED.content
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;


CREATE OR REPLACE FUNCTION get_collection(id text) RETURNS jsonb AS $$
SELECT content FROM collections
WHERE id=$1
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION all_collections() RETURNS jsonb AS $$
SELECT jsonb_agg(content) FROM collections;
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;



CREATE OR REPLACE FUNCTION collections_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    IF pg_trigger_depth() = 1 THEN
        PERFORM create_collection(NEW.content);
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

CREATE TRIGGER collections_trigger
BEFORE INSERT ON collections
FOR EACH ROW EXECUTE PROCEDURE collections_trigger_func();
