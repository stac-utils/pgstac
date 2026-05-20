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
    stac_version text,
    stac_extensions jsonb DEFAULT '[]'::jsonb,
    pgstac_updated_at timestamptz NOT NULL DEFAULT now(),
    content_hash text NOT NULL DEFAULT '',
    -- Split columns. Keep fragment_id unmanaged by an FK
    -- because incremental NOT VALID FKs on partitioned items are not supported.
    fragment_id bigint,
    bbox jsonb,
    links jsonb DEFAULT '[]',
    assets jsonb DEFAULT '{}',
    properties jsonb DEFAULT '{}',
    extra jsonb,
    -- Promoted queryable columns (redundant copies for index-only scans)
    created timestamptz,
    updated timestamptz,
    platform text,
    instruments text[],
    constellation text,
    mission text,
    eo_cloud_cover float8,
    eo_bands jsonb,
    eo_snow_cover float8,
    gsd float8,
    proj_epsg integer,
    proj_wkt2 text,
    proj_projjson jsonb,
    proj_bbox jsonb,
    proj_centroid jsonb,
    proj_shape jsonb,
    proj_transform jsonb,
    sci_doi text,
    sci_citation text,
    sci_publications jsonb,
    view_off_nadir float8,
    view_incidence_angle float8,
    view_azimuth float8,
    view_sun_azimuth float8,
    view_sun_elevation float8,
    file_size bigint,
    file_header_size bigint,
    file_checksum text,
    file_byte_order text,
    file_values_regex text,
    sat_orbit_state text,
    sat_relative_orbit integer,
    sat_absolute_orbit integer
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
CREATE INDEX IF NOT EXISTS items_fragment_id_idx ON items (fragment_id) WHERE fragment_id IS NOT NULL;

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

-- partition_after_triggerfunc: After-statement trigger on items.
-- Updates partition statistics for every partition touched by the current batch,
-- using run_or_queue() so the work is deferred rather than blocking the ingest
-- transaction. On UPDATE or DELETE it also evicts stale format_item_cache entries
-- for the affected items so subsequent reads see the new content immediately.
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

DROP TRIGGER IF EXISTS items_after_insert_trigger ON items;
CREATE TRIGGER items_after_insert_trigger
AFTER INSERT ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

DROP TRIGGER IF EXISTS items_after_update_trigger ON items;
CREATE TRIGGER items_after_update_trigger
AFTER UPDATE ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

DROP TRIGGER IF EXISTS items_after_delete_trigger ON items;
CREATE TRIGGER items_after_delete_trigger
AFTER DELETE ON items
REFERENCING OLD TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

-- items_touch_triggerfunc: Before-row UPDATE trigger on items.
-- Refreshes the pgstac_updated_at timestamp and recomputes content_hash from
-- the fully hydrated (reassembled) item JSON. The trigger carries a WHEN guard
-- (see CREATE TRIGGER below) that skips this expensive hydration when only
-- internal metadata fields change (e.g. pgstac_updated_at itself).
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
-- non-content fields change (e.g. pgstac_updated_at).
CREATE TRIGGER items_before_update_trigger
BEFORE UPDATE ON items
FOR EACH ROW
WHEN (
    OLD.bbox IS DISTINCT FROM NEW.bbox
    OR OLD.links IS DISTINCT FROM NEW.links
    OR OLD.assets IS DISTINCT FROM NEW.assets
    OR OLD.properties IS DISTINCT FROM NEW.properties
    OR OLD.extra IS DISTINCT FROM NEW.extra
    OR OLD.stac_version IS DISTINCT FROM NEW.stac_version
    OR OLD.stac_extensions IS DISTINCT FROM NEW.stac_extensions
    OR OLD.created IS DISTINCT FROM NEW.created
    OR OLD.updated IS DISTINCT FROM NEW.updated
    OR OLD.platform IS DISTINCT FROM NEW.platform
    OR OLD.instruments IS DISTINCT FROM NEW.instruments
    OR OLD.constellation IS DISTINCT FROM NEW.constellation
    OR OLD.mission IS DISTINCT FROM NEW.mission
    OR OLD.eo_cloud_cover IS DISTINCT FROM NEW.eo_cloud_cover
    OR OLD.eo_bands IS DISTINCT FROM NEW.eo_bands
    OR OLD.eo_snow_cover IS DISTINCT FROM NEW.eo_snow_cover
    OR OLD.gsd IS DISTINCT FROM NEW.gsd
    OR OLD.proj_epsg IS DISTINCT FROM NEW.proj_epsg
    OR OLD.proj_wkt2 IS DISTINCT FROM NEW.proj_wkt2
    OR OLD.proj_projjson IS DISTINCT FROM NEW.proj_projjson
    OR OLD.proj_bbox IS DISTINCT FROM NEW.proj_bbox
    OR OLD.proj_centroid IS DISTINCT FROM NEW.proj_centroid
    OR OLD.proj_shape IS DISTINCT FROM NEW.proj_shape
    OR OLD.proj_transform IS DISTINCT FROM NEW.proj_transform
    OR OLD.sci_doi IS DISTINCT FROM NEW.sci_doi
    OR OLD.sci_citation IS DISTINCT FROM NEW.sci_citation
    OR OLD.sci_publications IS DISTINCT FROM NEW.sci_publications
    OR OLD.view_off_nadir IS DISTINCT FROM NEW.view_off_nadir
    OR OLD.view_incidence_angle IS DISTINCT FROM NEW.view_incidence_angle
    OR OLD.view_azimuth IS DISTINCT FROM NEW.view_azimuth
    OR OLD.view_sun_azimuth IS DISTINCT FROM NEW.view_sun_azimuth
    OR OLD.view_sun_elevation IS DISTINCT FROM NEW.view_sun_elevation
    OR OLD.file_size IS DISTINCT FROM NEW.file_size
    OR OLD.file_header_size IS DISTINCT FROM NEW.file_header_size
    OR OLD.file_checksum IS DISTINCT FROM NEW.file_checksum
    OR OLD.file_byte_order IS DISTINCT FROM NEW.file_byte_order
    OR OLD.file_values_regex IS DISTINCT FROM NEW.file_values_regex
    OR OLD.sat_orbit_state IS DISTINCT FROM NEW.sat_orbit_state
    OR OLD.sat_relative_orbit IS DISTINCT FROM NEW.sat_relative_orbit
    OR OLD.sat_absolute_orbit IS DISTINCT FROM NEW.sat_absolute_orbit
    OR OLD.fragment_id IS DISTINCT FROM NEW.fragment_id
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
BEGIN
    out.id := content->>'id';
    out.geometry := stac_geom(content);
    out.collection := content->>'collection';
    out.datetime := stac_datetime(content);
    out.end_datetime := stac_end_datetime(content);
    out.stac_version := content->>'stac_version';
    out.stac_extensions := COALESCE(content->'stac_extensions', '[]'::jsonb);
    out.pgstac_updated_at := now();
    out.content_hash := encode(sha256(content::text::bytea), 'hex');

    props := content->'properties';

    -- Split columns: dedicated storage for standard top-level STAC fields.
    -- These enable index-only scans on promoted queryables and JSONB-free hot paths.
    out.bbox       := content->'bbox';
    out.links      := COALESCE(content->'links', '[]'::jsonb);
    out.assets     := COALESCE(content->'assets', '{}'::jsonb);
    out.properties := COALESCE(props, '{}'::jsonb);
    out.extra      := content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[];

    out.created             := (props->>'created')::timestamptz;
    out.updated             := (props->>'updated')::timestamptz;
    out.platform            := props->>'platform';
    out.instruments         := to_text_array(props->'instruments');
    out.constellation       := props->>'constellation';
    out.mission             := props->>'mission';
    out.eo_cloud_cover    := (props->>'eo:cloud_cover')::float8;
    out.eo_bands          := props->'eo:bands';
    out.eo_snow_cover     := (props->>'eo:snow_cover')::float8;
    out.gsd               := (props->>'gsd')::float8;
    out.proj_epsg         := (props->>'proj:epsg')::integer;
    out.proj_wkt2         := props->>'proj:wkt2';
    out.proj_projjson     := props->'proj:projjson';
    out.proj_bbox         := props->'proj:bbox';
    out.proj_centroid     := props->'proj:centroid';
    out.proj_shape        := props->'proj:shape';
    out.proj_transform    := props->'proj:transform';
    out.sci_doi           := props->>'sci:doi';
    out.sci_citation      := props->>'sci:citation';
    out.sci_publications  := props->'sci:publications';
    out.view_off_nadir    := (props->>'view:off_nadir')::float8;
    out.view_incidence_angle := (props->>'view:incidence_angle')::float8;
    out.view_azimuth      := (props->>'view:azimuth')::float8;
    out.view_sun_azimuth  := (props->>'view:sun_azimuth')::float8;
    out.view_sun_elevation := (props->>'view:sun_elevation')::float8;
    out.file_size         := (props->>'file:size')::bigint;
    out.file_header_size  := (props->>'file:header_size')::bigint;
    out.file_checksum     := props->>'file:checksum';
    out.file_byte_order   := props->>'file:byte_order';
    out.file_values_regex := props->>'file:values_regex';
    out.sat_orbit_state   := props->>'sat:orbit_state';
    out.sat_relative_orbit := (props->>'sat:relative_orbit')::integer;
    out.sat_absolute_orbit := (props->>'sat:absolute_orbit')::integer;

    -- fragment_id is NULL on initial dehydration; assigned by the staging trigger.
    out.fragment_id := NULL;
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

CREATE OR REPLACE FUNCTION content_hydrate(_item items, _collection collections, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    output jsonb;
    frag_content jsonb;
    merged_assets jsonb;
    merged_properties jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;

    -- Fetch shared fragment content (NULL when item has no fragment).
    IF _item.fragment_id IS NOT NULL THEN
        SELECT content INTO frag_content FROM item_fragments WHERE id = _item.fragment_id;
    END IF;

    -- Merge: fragment provides shared asset/property values; per-item provides individual values.
    -- No key overlap expected: the staging trigger strips fragment-covered keys from per-item columns.
    merged_assets     := COALESCE(frag_content->'assets',     '{}'::jsonb)
                      || COALESCE(_item.assets,               '{}'::jsonb);
    merged_properties := COALESCE(frag_content->'properties', '{}'::jsonb)
                      || COALESCE(_item.properties,           '{}'::jsonb);

    output := jsonb_build_object(
        'id',         _item.id,
        'geometry',   geom,
        'collection', _item.collection,
        'type',       'Feature'
    );
    IF _item.bbox IS NOT NULL THEN
        output := output || jsonb_build_object('bbox', _item.bbox);
    END IF;
    IF _item.stac_version IS NOT NULL THEN
        output := output || jsonb_build_object('stac_version', _item.stac_version);
    END IF;
    IF _item.stac_extensions IS NOT NULL AND _item.stac_extensions <> '[]'::jsonb THEN
        output := output || jsonb_build_object('stac_extensions', _item.stac_extensions);
    END IF;
    IF _item.links IS NOT NULL THEN
        output := output || jsonb_build_object('links', _item.links);
    END IF;
    IF merged_assets != '{}'::jsonb THEN
        output := output || jsonb_build_object('assets', merged_assets);
    END IF;
    IF merged_properties IS NOT NULL THEN
        output := output || jsonb_build_object('properties', merged_properties);
    END IF;
    IF _item.extra IS NOT NULL THEN
        output := output || _item.extra;
    END IF;

    RETURN jsonb_fields(output, fields);
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

-- content_nonhydrated: with fragment-backed storage, hydration is always required to produce
-- a complete STAC item.  This function is kept for API compatibility and delegates to content_hydrate.
CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
    SELECT content_hydrate(_item, fields);
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

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

-- items_staging_triggerfunc: Central ingest trigger invoked after any batch
-- INSERT into items_staging, items_staging_ignore, or items_staging_upsert.
-- It performs the full ingest pipeline:
--   1. Ensures all required item partitions exist (check_partition).
--   2. Dehydrates each STAC JSON payload into individual items columns.
--   3. Computes the fragment payload per item using collections.fragment_config,
--      deduplicates fragments via ON CONFLICT hash-based upsert, assigns
--      fragment_id, and strips fragment-covered keys from per-item columns.
--   4. Bulk-inserts the processed rows into items (or IGNORE / UPSERT variant).
--   5. Queues a field-registry refresh per distinct collection in the batch.
--   6. Clears the staging table for the next batch.
CREATE OR REPLACE FUNCTION items_staging_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
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
        -- orig_content stores the full STAC JSON so we can extract fragment keys later.
        -- It is NOT a column in items; we use an explicit column list on INSERT below.
        n.content AS orig_content,
        (content_dehydrate(n.content)).*
    FROM newdata n;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE 'Added % rows to tmpdata. %', nrows, clock_timestamp() - ts;

    -- Batch fragment dedup: compute the configured fragment payload per row using
    -- fragment_config from the collection row, insert unique fragments, then assign
    -- fragment_id and strip the fragment-covered keys from per-item assets/properties.
    RAISE NOTICE 'Batch inserting fragments. %', clock_timestamp() - ts;
    INSERT INTO item_fragments (collection, hash, content)
    SELECT DISTINCT ON (collection, pgstac_hash_fragment(fragment_content))
        collection,
        pgstac_hash_fragment(fragment_content) AS hash,
        fragment_content
    FROM (
        SELECT
            t.collection,
            extract_fragment(t.orig_content, c.fragment_config) AS fragment_content
        FROM tmpdata t
        JOIN collections c ON c.id = t.collection
    ) fragments
    WHERE fragment_content IS NOT NULL AND fragment_content != '{}'::jsonb
    ON CONFLICT (collection, hash) DO NOTHING;

    RAISE NOTICE 'Assigning fragment_id. %', clock_timestamp() - ts;
    UPDATE tmpdata t
    SET
        fragment_id = f.id,
        -- Strip the fragment-covered keys from per-item columns so items.assets/properties
        -- only contain per-item-specific values; fragment provides the shared baseline.
        assets     = strip_fragment_col(t.assets,     'assets',     c.fragment_config),
        properties = strip_fragment_col(t.properties, 'properties', c.fragment_config)
    FROM collections c,
         item_fragments f
    WHERE c.id = t.collection
      AND f.collection = t.collection
      AND c.fragment_config IS NOT NULL
      AND f.hash = pgstac_hash_fragment(extract_fragment(t.orig_content, c.fragment_config));

    -- Queue registry sampling per collection (async via run_or_queue so it does not
    -- block the ingest transaction).  One queued call per distinct collection in the batch.
    PERFORM run_or_queue(format('SELECT update_field_registry_from_items(%L);', c))
    FROM (SELECT DISTINCT collection FROM tmpdata) AS cte(c);

    -- Explicit column list excludes the orig_content extra column we added to tmpdata.
    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items (id, geometry, collection, datetime, end_datetime, pgstac_updated_at,
            stac_version, stac_extensions, content_hash, fragment_id, bbox, links, assets, properties, extra,
            created, updated, platform, instruments, constellation, mission,
            eo_cloud_cover, eo_bands, eo_snow_cover, gsd,
            proj_epsg, proj_wkt2, proj_projjson, proj_bbox, proj_centroid, proj_shape, proj_transform,
            sci_doi, sci_citation, sci_publications,
            view_off_nadir, view_incidence_angle, view_azimuth, view_sun_azimuth, view_sun_elevation,
            file_size, file_header_size, file_checksum, file_byte_order, file_values_regex,
            sat_orbit_state, sat_relative_orbit, sat_absolute_orbit)
        SELECT id, geometry, collection, datetime, end_datetime, pgstac_updated_at,
            stac_version, stac_extensions, content_hash, fragment_id, bbox, links, assets, properties, extra,
            created, updated, platform, instruments, constellation, mission,
            eo_cloud_cover, eo_bands, eo_snow_cover, gsd,
            proj_epsg, proj_wkt2, proj_projjson, proj_bbox, proj_centroid, proj_shape, proj_transform,
            sci_doi, sci_citation, sci_publications,
            view_off_nadir, view_incidence_angle, view_azimuth, view_sun_azimuth, view_sun_elevation,
            file_size, file_header_size, file_checksum, file_byte_order, file_values_regex,
            sat_orbit_state, sat_relative_orbit, sat_absolute_orbit
        FROM tmpdata;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items (id, geometry, collection, datetime, end_datetime, pgstac_updated_at,
            stac_version, stac_extensions, content_hash, fragment_id, bbox, links, assets, properties, extra,
            created, updated, platform, instruments, constellation, mission,
            eo_cloud_cover, eo_bands, eo_snow_cover, gsd,
            proj_epsg, proj_wkt2, proj_projjson, proj_bbox, proj_centroid, proj_shape, proj_transform,
            sci_doi, sci_citation, sci_publications,
            view_off_nadir, view_incidence_angle, view_azimuth, view_sun_azimuth, view_sun_elevation,
            file_size, file_header_size, file_checksum, file_byte_order, file_values_regex,
            sat_orbit_state, sat_relative_orbit, sat_absolute_orbit)
        SELECT id, geometry, collection, datetime, end_datetime, pgstac_updated_at,
            stac_version, stac_extensions, content_hash, fragment_id, bbox, links, assets, properties, extra,
            created, updated, platform, instruments, constellation, mission,
            eo_cloud_cover, eo_bands, eo_snow_cover, gsd,
            proj_epsg, proj_wkt2, proj_projjson, proj_bbox, proj_centroid, proj_shape, proj_transform,
            sci_doi, sci_citation, sci_publications,
            view_off_nadir, view_incidence_angle, view_azimuth, view_sun_azimuth, view_sun_elevation,
            file_size, file_header_size, file_checksum, file_byte_order, file_values_regex,
            sat_orbit_state, sat_relative_orbit, sat_absolute_orbit
        FROM tmpdata ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        DELETE FROM items i USING tmpdata s
            WHERE
                i.id = s.id
                AND i.collection = s.collection
                AND (
                    i.datetime IS DISTINCT FROM s.datetime
                    OR i.end_datetime IS DISTINCT FROM s.end_datetime
                    OR i.geometry IS DISTINCT FROM s.geometry
                    OR i.assets IS DISTINCT FROM s.assets
                    OR i.properties IS DISTINCT FROM s.properties
                    OR i.bbox IS DISTINCT FROM s.bbox
                    OR i.links IS DISTINCT FROM s.links
                    OR i.extra IS DISTINCT FROM s.extra
                    OR i.stac_version IS DISTINCT FROM s.stac_version
                    OR i.stac_extensions IS DISTINCT FROM s.stac_extensions
                    OR i.created IS DISTINCT FROM s.created
                    OR i.updated IS DISTINCT FROM s.updated
                    OR i.platform IS DISTINCT FROM s.platform
                    OR i.instruments IS DISTINCT FROM s.instruments
                    OR i.constellation IS DISTINCT FROM s.constellation
                    OR i.mission IS DISTINCT FROM s.mission
                    OR i.eo_cloud_cover IS DISTINCT FROM s.eo_cloud_cover
                    OR i.eo_bands IS DISTINCT FROM s.eo_bands
                    OR i.eo_snow_cover IS DISTINCT FROM s.eo_snow_cover
                    OR i.gsd IS DISTINCT FROM s.gsd
                    OR i.proj_epsg IS DISTINCT FROM s.proj_epsg
                    OR i.proj_wkt2 IS DISTINCT FROM s.proj_wkt2
                    OR i.proj_projjson IS DISTINCT FROM s.proj_projjson
                    OR i.proj_bbox IS DISTINCT FROM s.proj_bbox
                    OR i.proj_centroid IS DISTINCT FROM s.proj_centroid
                    OR i.proj_shape IS DISTINCT FROM s.proj_shape
                    OR i.proj_transform IS DISTINCT FROM s.proj_transform
                    OR i.sci_doi IS DISTINCT FROM s.sci_doi
                    OR i.sci_citation IS DISTINCT FROM s.sci_citation
                    OR i.sci_publications IS DISTINCT FROM s.sci_publications
                    OR i.view_off_nadir IS DISTINCT FROM s.view_off_nadir
                    OR i.view_incidence_angle IS DISTINCT FROM s.view_incidence_angle
                    OR i.view_azimuth IS DISTINCT FROM s.view_azimuth
                    OR i.view_sun_azimuth IS DISTINCT FROM s.view_sun_azimuth
                    OR i.view_sun_elevation IS DISTINCT FROM s.view_sun_elevation
                    OR i.file_size IS DISTINCT FROM s.file_size
                    OR i.file_header_size IS DISTINCT FROM s.file_header_size
                    OR i.file_checksum IS DISTINCT FROM s.file_checksum
                    OR i.file_byte_order IS DISTINCT FROM s.file_byte_order
                    OR i.file_values_regex IS DISTINCT FROM s.file_values_regex
                    OR i.sat_orbit_state IS DISTINCT FROM s.sat_orbit_state
                    OR i.sat_relative_orbit IS DISTINCT FROM s.sat_relative_orbit
                    OR i.sat_absolute_orbit IS DISTINCT FROM s.sat_absolute_orbit
                    OR i.fragment_id IS DISTINCT FROM s.fragment_id
                )
        ;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;
        INSERT INTO items (id, geometry, collection, datetime, end_datetime, pgstac_updated_at,
            stac_version, stac_extensions, content_hash, fragment_id, bbox, links, assets, properties, extra,
            created, updated, platform, instruments, constellation, mission,
            eo_cloud_cover, eo_bands, eo_snow_cover, gsd,
            proj_epsg, proj_wkt2, proj_projjson, proj_bbox, proj_centroid, proj_shape, proj_transform,
            sci_doi, sci_citation, sci_publications,
            view_off_nadir, view_incidence_angle, view_azimuth, view_sun_azimuth, view_sun_elevation,
            file_size, file_header_size, file_checksum, file_byte_order, file_values_regex,
            sat_orbit_state, sat_relative_orbit, sat_absolute_orbit)
        SELECT id, geometry, collection, datetime, end_datetime, pgstac_updated_at,
            stac_version, stac_extensions, content_hash, fragment_id, bbox, links, assets, properties, extra,
            created, updated, platform, instruments, constellation, mission,
            eo_cloud_cover, eo_bands, eo_snow_cover, gsd,
            proj_epsg, proj_wkt2, proj_projjson, proj_bbox, proj_centroid, proj_shape, proj_transform,
            sci_doi, sci_citation, sci_publications,
            view_off_nadir, view_incidence_angle, view_azimuth, view_sun_azimuth, view_sun_elevation,
            file_size, file_header_size, file_checksum, file_byte_order, file_values_regex,
            sat_orbit_state, sat_relative_orbit, sat_absolute_orbit
        FROM tmpdata ON CONFLICT DO NOTHING;
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


DROP TRIGGER IF EXISTS items_staging_insert_trigger ON items_staging;
CREATE TRIGGER items_staging_insert_trigger AFTER INSERT ON items_staging REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

DROP TRIGGER IF EXISTS items_staging_insert_ignore_trigger ON items_staging_ignore;
CREATE TRIGGER items_staging_insert_ignore_trigger AFTER INSERT ON items_staging_ignore REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

DROP TRIGGER IF EXISTS items_staging_insert_upsert_trigger ON items_staging_upsert;
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
    ;
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
    -- Sum reltuples across the registered item partitions for this collection.
    -- reltuples can be -1 (never analyzed); treat negative values as zero.
    SELECT COALESCE(sum(GREATEST(c.reltuples::bigint, 0)), 0) INTO est_rows
    FROM partitions_view p
    JOIN pg_class c ON c.relname = p.partition
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE p.collection = _collection
      AND n.nspname = 'pgstac'
      AND c.relkind = 'r';

    IF est_rows > 10000 THEN
        -- Large collection: use statistical sampling to avoid full seq-scan.
        WITH sampled AS (
            SELECT content_hydrate(i) AS content FROM items i TABLESAMPLE BERNOULLI(5) WHERE i.collection = _collection
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
        SELECT
            (SELECT count(*)::int FROM upserted),
            (SELECT count(*)::int FROM sampled)
        INTO npaths, nrows;
    ELSE
        -- Small collection: process up to 1000 rows to avoid BERNOULLI returning 0 rows.
        WITH sampled AS (
            SELECT content_hydrate(i) AS content FROM items i WHERE i.collection = _collection LIMIT 1000
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
        SELECT
            (SELECT count(*)::int FROM upserted),
            (SELECT count(*)::int FROM sampled)
        INTO npaths, nrows;
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

-- extract_fragment: Given full STAC item JSONB and a list of serialized fragment paths
-- (each element is fragment_path_text(text[]) — a dot-delimited root-relative path),
-- extract the sparse overlay JSONB that will be stored in item_fragments for dedup.
-- Returns NULL when fragment_paths is NULL or empty, or when no values are found.
-- Supports depth-1 paths (whole top-level key) and depth-2 paths (single named sub-key).
-- Depth-1 wins when both depths share the same top-level key.
CREATE OR REPLACE FUNCTION extract_fragment(
    content jsonb,
    fragment_paths text[]
) RETURNS jsonb AS $$
DECLARE
    top_keys  text[];
    top_key   text;
    has_full  boolean;
    sub_obj   jsonb;
    result    jsonb := '{}'::jsonb;
    p         text;
    pth       text[];
    val       jsonb;
BEGIN
    IF content IS NULL OR fragment_paths IS NULL OR cardinality(fragment_paths) = 0 THEN
        RETURN NULL;
    END IF;

    SELECT array_agg(DISTINCT (fragment_path_array(fp))[1])
    INTO top_keys
    FROM unnest(fragment_paths) fp
    WHERE fragment_path_array(fp) IS NOT NULL
      AND cardinality(fragment_path_array(fp)) >= 1;

    IF top_keys IS NULL THEN RETURN NULL; END IF;

    FOREACH top_key IN ARRAY top_keys LOOP
        has_full := false;
        sub_obj  := '{}'::jsonb;

        FOREACH p IN ARRAY fragment_paths LOOP
            pth := fragment_path_array(p);
            IF pth IS NULL OR pth[1] <> top_key THEN CONTINUE; END IF;

            IF cardinality(pth) = 1 THEN
                has_full := true;
            ELSIF cardinality(pth) = 2 THEN
                val := content #> pth;
                IF val IS NOT NULL THEN
                    sub_obj := sub_obj || jsonb_build_object(pth[2], val);
                END IF;
            END IF;
            -- depth > 2 is intentionally not supported in v0.10; extend here if needed.
        END LOOP;

        IF has_full THEN
            val := content->top_key;
            IF val IS NOT NULL THEN
                result := result || jsonb_build_object(top_key, val);
            END IF;
        ELSIF sub_obj <> '{}'::jsonb THEN
            result := result || jsonb_build_object(top_key, sub_obj);
        END IF;
    END LOOP;

    IF result = '{}'::jsonb THEN RETURN NULL; END IF;
    RETURN result;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

-- pgstac_hash_fragment: Hash a fragment content for dedup
CREATE OR REPLACE FUNCTION pgstac_hash_fragment(fragment jsonb) RETURNS text AS $$
SELECT pgstac_hash(fragment::text);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- get_or_create_fragment: Look up or insert a fragment from a full STAC item, returning its id.
-- _fragment_paths is the collection's fragment_config text[] (NULL means no fragmentation).
CREATE OR REPLACE FUNCTION get_or_create_fragment(
    content jsonb,
    _collection text,
    _fragment_paths text[] DEFAULT NULL
) RETURNS bigint AS $$
DECLARE
    frag_content jsonb;
    frag_hash    text;
    frag_id      bigint;
BEGIN
    IF content IS NULL OR _collection IS NULL OR _fragment_paths IS NULL THEN
        RETURN NULL;
    END IF;

    frag_content := extract_fragment(content, _fragment_paths);
    IF frag_content IS NULL THEN RETURN NULL; END IF;
    frag_hash    := pgstac_hash_fragment(frag_content);

    WITH ins AS (
        INSERT INTO item_fragments (collection, hash, content)
        VALUES (_collection, frag_hash, frag_content)
        ON CONFLICT (collection, hash) DO NOTHING
        RETURNING id
    )
    SELECT id INTO frag_id FROM ins;

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

-- strip_fragment_col: Remove fragment-owned sub-keys from a split column value.
-- col_name is the top-level STAC key that this column represents (e.g. 'assets' or 'properties').
-- fragment_paths is the collection's fragment_config text[].
-- For depth-1 paths matching col_name, the entire column is zeroed out (empty JSONB object).
-- For depth-2 paths matching col_name, only the named sub-key is removed.
-- Returns col_value unchanged when there are no matching fragment paths.
CREATE OR REPLACE FUNCTION strip_fragment_col(
    col_value jsonb,
    col_name  text,
    fragment_paths text[]
) RETURNS jsonb AS $$
DECLARE
    result    jsonb := col_value;
    p         text;
    pth       text[];
    strip_keys text[] := '{}';
BEGIN
    IF col_value IS NULL OR fragment_paths IS NULL THEN RETURN col_value; END IF;

    FOREACH p IN ARRAY fragment_paths LOOP
        pth := fragment_path_array(p);
        IF pth IS NULL OR pth[1] <> col_name THEN CONTINUE; END IF;
        IF cardinality(pth) = 1 THEN
            RETURN '{}'::jsonb;  -- entire column goes to fragment
        ELSIF cardinality(pth) = 2 THEN
            strip_keys := strip_keys || pth[2];
        END IF;
    END LOOP;

    IF cardinality(strip_keys) > 0 THEN
        result := result - strip_keys;
    END IF;
    RETURN result;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;
