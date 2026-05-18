-- Item fragments: deduplicated part of item content (shared across items in a collection)
CREATE TABLE IF NOT EXISTS item_fragments (
    id bigserial PRIMARY KEY,
    collection text NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    hash text NOT NULL,
    content jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (collection, hash)
);
CREATE INDEX IF NOT EXISTS item_fragments_collection_idx ON item_fragments (collection);

CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    pgstac_updated_at timestamptz NOT NULL DEFAULT now(),
    content_hash text NOT NULL DEFAULT '',
    content JSONB NOT NULL,
    private jsonb,
    -- Split columns (populated from v0.10+; item_fragments must exist first)
    fragment_id bigint REFERENCES item_fragments(id),
    bbox jsonb,
    links jsonb DEFAULT '[]',
    assets jsonb DEFAULT '{}',
    properties jsonb DEFAULT '{}',
    extra jsonb,
    -- Promoted queryable columns (redundant copies for index-only scans)
    eo_cloud_cover float8,
    eo_snow_cover float8,
    gsd float8,
    view_off_nadir float8,
    view_sun_azimuth float8,
    view_sun_elevation float8
)
PARTITION BY LIST (collection)
;

CREATE TABLE IF NOT EXISTS items_deleted_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_id text NOT NULL,
    collection text NOT NULL,
    partition text,
    datetime timestamptz,
    end_datetime timestamptz,
    content_hash text NOT NULL DEFAULT '',
    deleted_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS items_deleted_log_deleted_at_idx ON items_deleted_log (deleted_at);

-- Field registry: tracks which JSON paths exist in each collection (for queryables)
CREATE TABLE IF NOT EXISTS item_field_registry (
    collection text NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    path text NOT NULL,
    is_leaf boolean DEFAULT true,
    value_kinds text[] DEFAULT '{}',
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (collection, path)
);
CREATE INDEX IF NOT EXISTS item_field_registry_path_idx ON item_field_registry (path);

CREATE INDEX "datetime_idx" ON items USING BTREE (datetime DESC, end_datetime ASC);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION partition_after_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p text;
    t timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Updating partition stats %', t;
    FOR p IN SELECT DISTINCT partition
        FROM newdata n JOIN partition_sys_meta p
        ON (n.collection=p.collection AND n.datetime <@ p.partition_dtrange)
    LOOP
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L, %L);', p, true));
    END LOOP;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        DELETE FROM format_item_cache c USING newdata n WHERE c.collection = n.collection AND c.id = n.id;
    END IF;
    RAISE NOTICE 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE TRIGGER items_after_insert_trigger
AFTER INSERT ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE TRIGGER items_after_update_trigger
AFTER DELETE ON items
REFERENCING OLD TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE TRIGGER items_after_delete_trigger
AFTER UPDATE ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE OR REPLACE FUNCTION items_touch_triggerfunc() RETURNS TRIGGER AS $$
BEGIN
    NEW.pgstac_updated_at := now();
    NEW.content_hash := encode(sha256(content_hydrate(NEW)::text::bytea), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

DROP TRIGGER IF EXISTS items_before_upsert_trigger ON items;
DROP TRIGGER IF EXISTS items_before_update_trigger ON items;
-- WHEN guard: skip the expensive content_hydrate hash recomputation when only
-- non-content fields change (e.g. fragment_id assignment, pgstac_updated_at).
CREATE TRIGGER items_before_update_trigger
BEFORE UPDATE ON items
FOR EACH ROW
WHEN (
    OLD.content IS DISTINCT FROM NEW.content
    OR OLD.bbox IS DISTINCT FROM NEW.bbox
    OR OLD.links IS DISTINCT FROM NEW.links
    OR OLD.assets IS DISTINCT FROM NEW.assets
    OR OLD.properties IS DISTINCT FROM NEW.properties
    OR OLD.extra IS DISTINCT FROM NEW.extra
)
EXECUTE FUNCTION items_touch_triggerfunc();

CREATE OR REPLACE FUNCTION items_delete_log_trigger() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO items_deleted_log (
        item_id,
        collection,
        partition,
        datetime,
        end_datetime,
        content_hash
    )
    SELECT
        old_rows.id,
        old_rows.collection,
        (partition_name(old_rows.collection, old_rows.datetime)).partition_name,
        old_rows.datetime,
        old_rows.end_datetime,
        old_rows.content_hash
    FROM old_rows;

    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

DROP TRIGGER IF EXISTS items_delete_log_after_delete_trigger ON items;
CREATE TRIGGER items_delete_log_after_delete_trigger
    AFTER DELETE ON items
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT EXECUTE FUNCTION items_delete_log_trigger();


CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
DECLARE
    out items;
    props jsonb;
    base_item jsonb;
BEGIN
    out.id := content->>'id';
    out.geometry := stac_geom(content);
    out.collection := content->>'collection';
    out.datetime := stac_datetime(content);
    out.end_datetime := stac_end_datetime(content);
    out.pgstac_updated_at := now();
    out.content_hash := encode(sha256(content::text::bytea), 'hex');

    base_item := collection_base_item(content->>'collection');
    props := content->'properties';

    -- Split columns: dedicated storage for standard top-level STAC fields.
    -- These enable index-only scans on promoted queryables and avoid JSONB parse
    -- on the hot SELECT path once the legacy content column is retired.
    out.bbox       := content->'bbox';
    out.links      := COALESCE(content->'links', '[]'::jsonb);
    out.assets     := COALESCE(content->'assets', '{}'::jsonb);
    out.properties := COALESCE(props, '{}'::jsonb);
    -- extra: non-standard top-level fields not in id/geometry/collection/type/bbox/links/assets/properties
    out.extra      := content - '{id,geometry,collection,type,bbox,links,assets,properties}'::text[];

    -- Promoted queryable columns: direct float8 storage avoids JSONB parse on range queries.
    out.eo_cloud_cover    := (props->>'eo:cloud_cover')::float8;
    out.eo_snow_cover     := (props->>'eo:snow_cover')::float8;
    out.gsd               := (props->>'gsd')::float8;
    out.view_off_nadir    := (props->>'view:off_nadir')::float8;
    out.view_sun_azimuth  := (props->>'view:sun_azimuth')::float8;
    out.view_sun_elevation := (props->>'view:sun_elevation')::float8;

    -- Legacy content column: kept for backwards compatibility with clients that
    -- read items.content directly. Contains all fields except id/geometry/collection/type,
    -- with base_item fields stripped out for dedup storage.
    -- NOTE: content_hash above hashes the raw incoming JSONB (pre-strip), which is
    -- intentional for change detection; it differs from the hash produced by
    -- items_touch_triggerfunc (which hashes the hydrated form on UPDATE).
    out.content := strip_jsonb(
        content - '{id,geometry,collection,type}'::text[],
        base_item
    ) - '{id,geometry,collection,type}'::text[];

    out.private := null;
    RETURN out;
END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION include_field(f text, fields jsonb DEFAULT '{}'::jsonb) RETURNS boolean AS $$
DECLARE
    includes jsonb := fields->'include';
    excludes jsonb := fields->'exclude';
BEGIN
    IF f IS NULL THEN
        RETURN NULL;
    END IF;


    IF
        jsonb_typeof(excludes) = 'array'
        AND jsonb_array_length(excludes)>0
        AND excludes ? f
    THEN
        RETURN FALSE;
    END IF;

    IF
        (
            jsonb_typeof(includes) = 'array'
            AND jsonb_array_length(includes) > 0
            AND includes ? f
        ) OR
        (
            includes IS NULL
            OR jsonb_typeof(includes) = 'null'
            OR jsonb_array_length(includes) = 0
        )
    THEN
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS content_hydrate(jsonb, jsonb, jsonb);
CREATE OR REPLACE FUNCTION content_hydrate(
    _item jsonb,
    _base_item jsonb,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
    SELECT merge_jsonb(
            jsonb_fields(_item, fields),
            jsonb_fields(_base_item, fields)
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION content_hydrate(_item items, _collection collections, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    output jsonb;
    content jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;

    IF _item.fragment_id IS NOT NULL THEN
        -- Preferred path: reconstruct item from split columns.
        -- fragment_id IS NOT NULL is the canonical indicator that split columns
        -- are populated; checking a nullable bigint is cheaper than a JSONB equality.
        content := jsonb_build_object(
            'id',         _item.id,
            'geometry',   geom,
            'collection', _item.collection,
            'type',       'Feature'
        );
        IF _item.bbox IS NOT NULL THEN
            content := content || jsonb_build_object('bbox', _item.bbox);
        END IF;
        IF _item.links IS NOT NULL THEN
            content := content || jsonb_build_object('links', _item.links);
        END IF;
        IF _item.assets IS NOT NULL THEN
            content := content || jsonb_build_object('assets', _item.assets);
        END IF;
        IF _item.properties IS NOT NULL THEN
            content := content || jsonb_build_object('properties', _item.properties);
        END IF;
        IF _item.extra IS NOT NULL THEN
            content := content || _item.extra;
        END IF;
    ELSE
        -- Legacy fallback: reconstruct from the content column (pre-v0.10 rows).
        content := jsonb_build_object(
            'id',         _item.id,
            'geometry',   geom,
            'collection', _item.collection,
            'type',       'Feature'
        ) || _item.content;
    END IF;

    output := content_hydrate(content, _collection.base_item, fields);
    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := jsonb_build_object(
                'id', _item.id,
                'geometry', geom,
                'collection', _item.collection,
                'type', 'Feature'
            ) || _item.content;
    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_hydrate(_item items, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
    SELECT content_hydrate(
        _item,
        (SELECT c FROM collections c WHERE id=_item.collection LIMIT 1),
        fields
    );
$$ LANGUAGE SQL STABLE;


CREATE UNLOGGED TABLE items_staging (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_ignore (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_upsert (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p record;
    _partitions text[];
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
BEGIN
    RAISE NOTICE 'Creating Partitions. %', clock_timestamp() - ts;

    FOR part IN WITH t AS (
        SELECT
            n.content->>'collection' as collection,
            stac_daterange(n.content->'properties') as dtr,
            partition_trunc
        FROM newdata n JOIN collections ON (n.content->>'collection'=collections.id)
    ), p AS (
        SELECT
            collection,
            COALESCE(date_trunc(partition_trunc::text, lower(dtr)),'-infinity') as d,
            tstzrange(min(lower(dtr)),max(lower(dtr)),'[]') as dtrange,
            tstzrange(min(upper(dtr)),max(upper(dtr)),'[]') as edtrange
        FROM t
        GROUP BY 1,2
    ) SELECT check_partition(collection, dtrange, edtrange) FROM p LOOP
        RAISE NOTICE 'Partition %', part;
    END LOOP;

    RAISE NOTICE 'Creating temp table with data to be added. %', clock_timestamp() - ts;
    DROP TABLE IF EXISTS tmpdata;
    CREATE TEMP TABLE tmpdata ON COMMIT DROP AS
    SELECT
        (content_dehydrate(content)).*
    FROM newdata;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE 'Added % rows to tmpdata. %', nrows, clock_timestamp() - ts;

    -- Batch fragment dedup: insert all unique fragments in one statement rather than
    -- calling get_or_create_fragment() per row (which is O(N) round-trips).
    -- pgstac_hash_fragment(content) is computed twice (once for insert, once for the
    -- join update) but both calls are IMMUTABLE so the planner can CSE them; the net
    -- cost is far lower than N individual PL/pgSQL function round-trips.
    -- Concurrent inserts of identical fragments are safe: ON CONFLICT DO NOTHING means
    -- both sides succeed with the same row; the join below finds it for either winner.
    RAISE NOTICE 'Batch inserting fragments. %', clock_timestamp() - ts;
    INSERT INTO item_fragments (collection, hash, content)
    SELECT DISTINCT ON (collection, pgstac_hash_fragment(content))
        collection,
        pgstac_hash_fragment(content) AS hash,
        content
    FROM tmpdata
    WHERE content IS NOT NULL AND content != '{}'::jsonb
    ON CONFLICT (collection, hash) DO NOTHING;

    RAISE NOTICE 'Assigning fragment_id. %', clock_timestamp() - ts;
    UPDATE tmpdata t
    SET fragment_id = f.id
    FROM item_fragments f
    WHERE f.collection = t.collection
      AND f.hash = pgstac_hash_fragment(t.content)
      AND t.content IS NOT NULL AND t.content != '{}'::jsonb;

    -- Queue registry sampling per collection (async via run_or_queue so it does not
    -- block the ingest transaction).  One queued call per distinct collection in the batch.
    PERFORM run_or_queue(format('SELECT update_field_registry_from_items(%L);', c))
    FROM (SELECT DISTINCT collection FROM tmpdata) AS cte(c);

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT * FROM tmpdata;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        DELETE FROM items i USING tmpdata s
            WHERE
                i.id = s.id
                AND i.collection = s.collection
                AND i IS DISTINCT FROM s
        ;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;
        INSERT INTO items AS t
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    END IF;

    RAISE NOTICE 'Deleting data from staging table. %', clock_timestamp() - ts;
    -- Use TG_TABLE_NAME so the correct staging table is cleared.
    -- The previous hard-coded 'DELETE FROM items_staging' was a bug that left
    -- items_staging_ignore and items_staging_upsert un-cleared after processing.
    EXECUTE format('DELETE FROM %I', TG_TABLE_NAME);
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER items_staging_insert_trigger AFTER INSERT ON items_staging REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_ignore_trigger AFTER INSERT ON items_staging_ignore REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_upsert_trigger AFTER INSERT ON items_staging_upsert REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();


CREATE OR REPLACE FUNCTION item_by_id(_id text, _collection text DEFAULT NULL) RETURNS items AS
$$
DECLARE
    i items%ROWTYPE;
BEGIN
    SELECT * INTO i FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection) LIMIT 1;
    RETURN i;
END;
$$ LANGUAGE PLPGSQL STABLE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION get_item(_id text, _collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT content_hydrate(items) FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection);
$$ LANGUAGE SQL STABLE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION delete_item(_id text, _collection text DEFAULT NULL) RETURNS VOID AS $$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id AND (_collection IS NULL OR collection=_collection) RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL;

--/*
CREATE OR REPLACE FUNCTION create_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION update_item(content jsonb) RETURNS VOID AS $$
DECLARE
    old items %ROWTYPE;
    out items%ROWTYPE;
BEGIN
    PERFORM delete_item(content->>'id', content->>'collection');
    PERFORM create_item(content);
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION collection_bbox(id text) RETURNS jsonb AS $$
    SELECT (replace(replace(replace(st_extent(geometry)::text,'BOX(','[['),')',']]'),' ',','))::jsonb
    FROM items WHERE collection=$1;
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_temporal_extent(id text) RETURNS jsonb AS $$
    SELECT to_jsonb(array[array[min(datetime), max(datetime)]])
    FROM items WHERE collection=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections
    SET content = jsonb_set_lax(
        content,
        '{extent}'::text[],
        collection_extent(id, FALSE),
        true,
        'use_json_null'
    )
;
$$ LANGUAGE SQL;

-- ---------------------------------------------------------------------------
-- Field Registry: walks JSONB item content to track which paths exist in each
-- collection.  Used to auto-populate queryables and support schema inference.
-- ---------------------------------------------------------------------------

-- jsonb_field_rows: Recursively walk a JSONB document and emit one row per field path.
-- max_depth guards against runaway recursion on pathologically nested documents.
CREATE OR REPLACE FUNCTION jsonb_field_rows(
    data jsonb,
    parent_path text DEFAULT '',
    max_depth int DEFAULT 20
) RETURNS TABLE (path text, is_leaf boolean, value_kind text) AS $$
DECLARE
    k text;
    v jsonb;
    current_path text;
    jtype text;
BEGIN
    IF data IS NULL OR max_depth <= 0 THEN
        RETURN;
    END IF;
    jtype := jsonb_typeof(data);
    IF jtype = 'object' THEN
        FOR k, v IN SELECT * FROM jsonb_each(data) LOOP
            current_path := CASE WHEN parent_path = '' THEN k ELSE parent_path || '.' || k END;
            IF jsonb_typeof(v) IN ('object', 'array') THEN
                RETURN QUERY SELECT current_path, FALSE, jsonb_typeof(v);
                RETURN QUERY SELECT * FROM jsonb_field_rows(v, current_path, max_depth - 1);
            ELSE
                RETURN QUERY SELECT current_path, TRUE, jsonb_typeof(v);
            END IF;
        END LOOP;
    ELSIF jtype = 'array' THEN
        -- Walk array elements (e.g. arrays of nested objects); arrays of scalars
        -- are already handled as leaves in the object branch above.
        FOR v IN SELECT jsonb_array_elements(data) LOOP
            IF jsonb_typeof(v) = 'object' THEN
                RETURN QUERY SELECT * FROM jsonb_field_rows(v, parent_path, max_depth - 1);
            END IF;
        END LOOP;
    END IF;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

-- update_field_registry_from_sample: UPSERT registry rows from a pre-selected array of
-- raw item content JSONBs.  Callers supply the sample to decouple sampling strategy
-- from the registry write; merge value_kinds to accumulate observed types over time.
CREATE OR REPLACE FUNCTION update_field_registry_from_sample(
    _collection text,
    item_contents jsonb[]
) RETURNS void AS $$
    INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen)
    SELECT
        _collection,
        r.path,
        bool_and(r.is_leaf)                                                       AS is_leaf,
        array_agg(DISTINCT r.value_kind) FILTER (WHERE r.value_kind IS NOT NULL)  AS value_kinds,
        now(),
        now()
    FROM unnest(item_contents) AS item(content)
    CROSS JOIN LATERAL jsonb_field_rows(item.content) AS r(path, is_leaf, value_kind)
    GROUP BY r.path
    ON CONFLICT (collection, path) DO UPDATE SET
        is_leaf     = EXCLUDED.is_leaf,
        value_kinds = (
            SELECT array_agg(DISTINCT v)
            FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)
        ),
        last_seen   = now()
    WHERE item_field_registry.last_seen < now() - interval '1 hour';
$$ LANGUAGE SQL VOLATILE;

-- update_field_registry_from_items: Sample a live collection and UPSERT registry rows.
-- Uses TABLESAMPLE BERNOULLI(5) for large collections (>10k rows by pg_class estimate)
-- and LIMIT 1000 for smaller ones to avoid a full seq-scan for tiny collections.
-- pg_class.reltuples is an estimate (may be stale); its only role is threshold selection.
-- Returns (registered_paths, rows_processed) for observability.
CREATE OR REPLACE FUNCTION update_field_registry_from_items(
    _collection text
) RETURNS TABLE (registered_paths int, rows_processed int) AS $$
DECLARE
    est_rows bigint;
    nrows    int;
    npaths   int;
BEGIN
    -- Sum reltuples across all partitions for this collection.
    -- reltuples can be -1 (never analyzed); treat negative values as zero.
    SELECT COALESCE(sum(GREATEST(c.reltuples::bigint, 0)), 0) INTO est_rows
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'pgstac'
      AND c.relkind = 'r'
      AND c.relname LIKE '_items_%'
      AND c.relname LIKE '%' || regexp_replace(_collection, '[^a-zA-Z0-9_-]', '', 'g') || '%';

    IF est_rows > 10000 THEN
        -- Large collection: use statistical sampling to avoid full seq-scan.
        WITH sampled AS (
            SELECT content FROM items TABLESAMPLE BERNOULLI(5) WHERE collection = _collection
        ),
        upserted AS (
            INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen)
            SELECT
                _collection,
                r.path,
                bool_and(r.is_leaf)                                                       AS is_leaf,
                array_agg(DISTINCT r.value_kind) FILTER (WHERE r.value_kind IS NOT NULL)  AS value_kinds,
                now(), now()
            FROM sampled
            CROSS JOIN LATERAL jsonb_field_rows(content) AS r(path, is_leaf, value_kind)
            GROUP BY r.path
            ON CONFLICT (collection, path) DO UPDATE SET
                is_leaf     = EXCLUDED.is_leaf,
                value_kinds = (
                    SELECT array_agg(DISTINCT v)
                    FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)
                ),
                last_seen   = now()
            RETURNING 1
        )
        SELECT count(*) INTO nrows FROM sampled;
        GET DIAGNOSTICS npaths = ROW_COUNT;
    ELSE
        -- Small collection: process up to 1000 rows to avoid BERNOULLI returning 0 rows.
        WITH sampled AS (
            SELECT content FROM items WHERE collection = _collection LIMIT 1000
        ),
        upserted AS (
            INSERT INTO item_field_registry (collection, path, is_leaf, value_kinds, first_seen, last_seen)
            SELECT
                _collection,
                r.path,
                bool_and(r.is_leaf)                                                       AS is_leaf,
                array_agg(DISTINCT r.value_kind) FILTER (WHERE r.value_kind IS NOT NULL)  AS value_kinds,
                now(), now()
            FROM sampled
            CROSS JOIN LATERAL jsonb_field_rows(content) AS r(path, is_leaf, value_kind)
            GROUP BY r.path
            ON CONFLICT (collection, path) DO UPDATE SET
                is_leaf     = EXCLUDED.is_leaf,
                value_kinds = (
                    SELECT array_agg(DISTINCT v)
                    FROM unnest(item_field_registry.value_kinds || EXCLUDED.value_kinds) t(v)
                ),
                last_seen   = now()
            RETURNING 1
        )
        SELECT count(*) INTO nrows FROM sampled;
        GET DIAGNOSTICS npaths = ROW_COUNT;
    END IF;

    RETURN QUERY SELECT npaths, nrows;
END;
$$ LANGUAGE PLPGSQL VOLATILE SECURITY DEFINER;

-- refresh_field_registry: Expire stale registry entries that haven't been seen recently.
-- Intended for scheduled maintenance (e.g. pg_cron daily job).
-- Returns (collection, expired_paths) for each collection affected.
CREATE OR REPLACE FUNCTION refresh_field_registry(
    _collection text DEFAULT NULL,
    retention_interval interval DEFAULT '90 days'
) RETURNS TABLE (collection_id text, expired_paths int) AS $$
    WITH deleted AS (
        DELETE FROM item_field_registry
        WHERE (_collection IS NULL OR collection = _collection)
          AND last_seen < now() - retention_interval
        RETURNING collection
    )
    SELECT collection, count(*)::int
    FROM deleted
    GROUP BY collection;
$$ LANGUAGE SQL VOLATILE;

-- Item Fragment Management functions
-- extract_fragment: Strip the per-item keys from content to get the dedup-eligible portion.
-- Pure SQL so PostgreSQL can inline and constant-fold it; avoid PLPGSQL wrapper overhead.
CREATE OR REPLACE FUNCTION extract_fragment(
    content jsonb,
    excluded_keys text[] DEFAULT '{id,geometry,collection,type}'::text[]
) RETURNS jsonb AS $$
    SELECT content - COALESCE(excluded_keys, '{id,geometry,collection,type}'::text[]);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- pgstac_hash_fragment: Hash a fragment content for dedup
CREATE OR REPLACE FUNCTION pgstac_hash_fragment(fragment jsonb) RETURNS text AS $$
SELECT pgstac_hash(fragment::text);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- get_or_create_fragment: Look up or insert a fragment, returning its id.
-- Uses INSERT … ON CONFLICT … RETURNING to avoid a redundant pre-check SELECT;
-- only falls back to a SELECT when the conflict path suppresses the RETURNING row.
-- This is safe under concurrent inserts: two transactions racing to create the same
-- fragment both see ON CONFLICT DO NOTHING; the loser's RETURNING is empty so it
-- falls through to the SELECT which finds the winner's row.
CREATE OR REPLACE FUNCTION get_or_create_fragment(
    content jsonb,
    _collection text,
    excluded_keys text[] DEFAULT '{id,geometry,collection,type}'::text[]
) RETURNS bigint AS $$
DECLARE
    frag_content jsonb;
    frag_hash    text;
    frag_id      bigint;
BEGIN
    IF content IS NULL OR _collection IS NULL THEN
        RETURN NULL;
    END IF;

    frag_content := extract_fragment(content, excluded_keys);
    frag_hash    := pgstac_hash_fragment(frag_content);

    -- Insert-first: one round trip when the fragment is new.
    WITH ins AS (
        INSERT INTO item_fragments (collection, hash, content)
        VALUES (_collection, frag_hash, frag_content)
        ON CONFLICT (collection, hash) DO NOTHING
        RETURNING id
    )
    SELECT id INTO frag_id FROM ins;

    -- Fallback SELECT: one extra round trip only on the conflict path.
    IF frag_id IS NULL THEN
        SELECT id INTO frag_id
        FROM item_fragments
        WHERE collection = _collection AND hash = frag_hash;
    END IF;

    RETURN frag_id;
END;
$$ LANGUAGE PLPGSQL VOLATILE PARALLEL UNSAFE;

-- gc_fragments: Garbage collect orphaned fragments using a single set-based DELETE.
-- Replaces the previous per-collection FOR LOOP with a single statement that lets
-- the planner choose the optimal join/anti-join strategy across all collections.
-- The NOT EXISTS sub-select is evaluated per fragment; with an index on items.fragment_id
-- this is an efficient anti-join rather than a full seq-scan.
CREATE OR REPLACE FUNCTION gc_fragments(
    _collection text DEFAULT NULL,
    retention_interval interval DEFAULT '90 days'
) RETURNS TABLE (
    collection_id text,
    fragments_removed int
) AS $$
    WITH deleted AS (
        DELETE FROM item_fragments f
        WHERE
            (_collection IS NULL OR f.collection = _collection)
            AND f.created_at < now() - retention_interval
            AND NOT EXISTS (SELECT 1 FROM items i WHERE i.fragment_id = f.id)
        RETURNING f.collection
    )
    SELECT collection, count(*)::int
    FROM deleted
    GROUP BY collection;
$$ LANGUAGE SQL VOLATILE PARALLEL UNSAFE;
