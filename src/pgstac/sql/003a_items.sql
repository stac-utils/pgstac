-- -----------------------------------------------------------------------
-- COLUMN LIST SYNC CONTRACT
-- When adding or removing promoted columns from the items table, ALL 6 of
-- these locations must be updated together:
--
--   1. items TABLE DDL (below)
--   2. content_dehydrate() — assigns each promoted column from props
--   3. promoted_item_property_defs() — metadata table mapping name→column
--   4. promoted_properties_from_item() — hydrates promoted columns back to jsonb
--   5. items_content_distinct_sql() — UPDATE-path / upsert DELETE content compare
--      (auto-derives promoted columns from promoted_items_column_list())
--   6. items_staging_dehydrate() — the single `enriched` SELECT that builds the
--      items rows for all three staging branches (insert / ignore / upsert)
--
-- Use promoted_items_column_list() to verify consistency at runtime.
-- The PGTap test 003_items.sql cross-references this against information_schema.
-- -----------------------------------------------------------------------

-- Item fragments: deduplicated part of item content (shared across items in a collection)
CREATE TABLE IF NOT EXISTS item_fragments (
    id bigserial PRIMARY KEY,
    collection text NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    -- Raw 32-byte sha256 of the fragment payload (see pgstac_hash_fragment).
    -- Stored as bytea, not 64-char hex text: half the size and a faster btree
    -- comparison on the (collection, hash) unique index. This hash is an
    -- internal dedup key only and is never exposed in the STAC API.
    hash bytea NOT NULL,
    content jsonb,
    links_template jsonb,
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
    datetime_is_range boolean NOT NULL DEFAULT false,
    stac_version text,
    stac_extensions jsonb DEFAULT '[]'::jsonb,
    pgstac_updated_at timestamptz NOT NULL DEFAULT now(),
    -- 32-byte sha256 of the canonical (RFC 8785-aligned) STAC item JSON set at
    -- ingest time. Allows external clients to detect unchanged items without a
    -- full fetch. Does NOT include the private column (operator metadata).
    item_hash bytea NOT NULL DEFAULT '\x'::bytea,
    -- Split columns. Keep fragment_id unmanaged by an FK
    -- because incremental NOT VALID FKs on partitioned items are not supported.
    fragment_id bigint,
    bbox jsonb,
    links jsonb,
    assets jsonb,
    properties jsonb,
    extra jsonb,
    -- Promoted queryable columns (redundant copies for index-only scans)
    created timestamptz,
    updated timestamptz,
    platform text,
    instruments text[],
    constellation text,
    mission text,
    eo_cloud_cover float8,
    bands jsonb,
    eo_snow_cover float8,
    gsd float8,
    proj_code text,
    proj_geometry jsonb,
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
    view_moon_azimuth float8,
    view_moon_elevation float8,
    file_size bigint,
    file_header_size bigint,
    file_checksum text,
    file_byte_order text,
    sat_orbit_state text,
    sat_relative_orbit integer,
    sat_absolute_orbit integer,
    sat_platform_international_designator text,
    sat_anx_datetime timestamptz,
    link_hrefs text[],
    -- Operator-private metadata: not returned by the STAC API, not included in
    -- item_hash, not part of the dehydrate/hydrate path. Set via direct UPDATE.
    private jsonb
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
    item_hash bytea NOT NULL DEFAULT '\x'::bytea,
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
-- transaction.
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

-- items_content_distinct_sql / items_content_changed: detect whether a direct
-- UPDATE actually changed the stored item content. Used by the touch trigger
-- (to refresh pgstac_updated_at) and by the upsert DELETE predicate, keeping
-- those two content comparisons in sync.
CREATE OR REPLACE FUNCTION items_content_distinct_sql(left_ref text, right_ref text)
RETURNS text AS $$
DECLARE
    clauses text[];
BEGIN
    clauses := ARRAY[
        format('%s.datetime_is_range IS DISTINCT FROM %s.datetime_is_range', left_ref, right_ref),
        format('%s.datetime IS DISTINCT FROM %s.datetime', left_ref, right_ref),
        format('%s.end_datetime IS DISTINCT FROM %s.end_datetime', left_ref, right_ref),
        format('%s.item_hash IS DISTINCT FROM %s.item_hash', left_ref, right_ref),
        format('%s.geometry IS DISTINCT FROM %s.geometry', left_ref, right_ref),
        format('%s.bbox IS DISTINCT FROM %s.bbox', left_ref, right_ref),
        format('%s.links IS DISTINCT FROM %s.links', left_ref, right_ref),
        format('%s.link_hrefs IS DISTINCT FROM %s.link_hrefs', left_ref, right_ref),
        format('%s.assets IS DISTINCT FROM %s.assets', left_ref, right_ref),
        format('%s.properties IS DISTINCT FROM %s.properties', left_ref, right_ref),
        format('%s.extra IS DISTINCT FROM %s.extra', left_ref, right_ref),
        format('%s.stac_version IS DISTINCT FROM %s.stac_version', left_ref, right_ref),
        format('%s.stac_extensions IS DISTINCT FROM %s.stac_extensions', left_ref, right_ref)
    ];

    clauses := clauses || ARRAY(
        SELECT format('%s.%I IS DISTINCT FROM %s.%I', left_ref, column_name, right_ref, column_name)
        FROM unnest(promoted_items_column_list()) AS column_name
    );

    RETURN array_to_string(clauses, E'\n                    OR ');
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION items_content_changed(left_item items, right_item items)
RETURNS boolean AS $$
DECLARE
    changed boolean;
BEGIN
    EXECUTE format('SELECT %s', items_content_distinct_sql('($1)', '($2)'))
    INTO changed
    USING left_item, right_item;

    RETURN COALESCE(changed, FALSE);
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

-- items_touch_triggerfunc: refresh pgstac_updated_at when a direct UPDATE changes
-- the stored item content. It deliberately does NOT recompute item_hash:
-- item_hash is the canonical (RFC 8785-aligned) hash of the item *as ingested*
-- through create_item / upsert_item / update_item (set once in content_dehydrate),
-- so it stays externally reproducible by a client hashing its own copy.
-- A raw `UPDATE items SET ...` that bypasses the staging path leaves item_hash
-- referring to the last ingested document; re-ingest via upsert_item to refresh.
CREATE OR REPLACE FUNCTION items_touch_triggerfunc() RETURNS TRIGGER AS $$
BEGIN
    IF NOT items_content_changed(OLD, NEW) THEN
        RETURN NEW;
    END IF;

    NEW.pgstac_updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

DROP TRIGGER IF EXISTS items_before_upsert_trigger ON items;
DROP TRIGGER IF EXISTS items_before_update_trigger ON items;
CREATE TRIGGER items_before_update_trigger
BEFORE UPDATE ON items
FOR EACH ROW
EXECUTE FUNCTION items_touch_triggerfunc();

CREATE OR REPLACE FUNCTION items_delete_log_trigger() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO items_deleted_log (
        item_id,
        collection,
        partition,
        datetime,
        end_datetime,
        item_hash
    )
    SELECT
        old_rows.id,
        old_rows.collection,
        (partition_name(old_rows.collection, old_rows.datetime)).partition_name,
        old_rows.datetime,
        old_rows.end_datetime,
        old_rows.item_hash
    FROM old_rows;

    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION strip_promoted_properties(props jsonb) RETURNS jsonb AS $$
    SELECT COALESCE(props, '{}'::jsonb) - COALESCE(
        (SELECT array_agg(name ORDER BY name) FROM promoted_item_property_defs()),
        '{}'::text[]
    ) - ARRAY['datetime', 'start_datetime', 'end_datetime'];
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tstz_to_stac_text(value timestamptz) RETURNS text AS $$
    SELECT CASE
        WHEN value IS NULL THEN NULL
        ELSE trim(trailing '.' FROM trim(trailing '0' FROM to_char(
            value AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US'
        ))) || 'Z'
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION temporal_properties_from_item(_item items) RETURNS jsonb AS $$
    SELECT CASE
        WHEN _item.datetime_is_range THEN jsonb_build_object('datetime', NULL)
            || jsonb_strip_nulls(jsonb_build_object(
                'start_datetime', tstz_to_stac_text(_item.datetime),
                'end_datetime', tstz_to_stac_text(_item.end_datetime)
            ))
        ELSE jsonb_build_object('datetime', tstz_to_stac_text(_item.datetime))
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- promoted_properties_from_item: Rehydrate the promoted scalar columns back into
-- a STAC properties object. This is the inverse of content_dehydrate's explicit
-- per-column extraction and is written in the same explicit style on purpose:
-- a direct jsonb_strip_nulls(jsonb_build_object(...)) is ~35% faster than routing
-- the column->STAC-name mapping through promoted_item_property_defs() with a
-- per-item join + jsonb_object_agg, and produces identical output (verified on
-- the PC fixtures + an all-fields synthetic item). Keep the property list in sync
-- with content_dehydrate and the items DDL (see the COLUMN LIST SYNC CONTRACT at
-- the top of this file).
CREATE OR REPLACE FUNCTION promoted_properties_from_item(_item items) RETURNS jsonb AS $$
    SELECT temporal_properties_from_item(_item) || jsonb_strip_nulls(jsonb_build_object(
        'created', CASE WHEN _item.created IS NULL THEN NULL ELSE tstz_to_stac_text(_item.created) END,
        'updated', CASE WHEN _item.updated IS NULL THEN NULL ELSE tstz_to_stac_text(_item.updated) END,
        'platform', _item.platform,
        'instruments', _item.instruments,
        'constellation', _item.constellation,
        'mission', _item.mission,
        'eo:cloud_cover', _item.eo_cloud_cover,
        'bands', _item.bands,
        'eo:snow_cover', _item.eo_snow_cover,
        'gsd', _item.gsd,
        'proj:code', _item.proj_code,
        'proj:geometry', _item.proj_geometry,
        'proj:wkt2', _item.proj_wkt2,
        'proj:projjson', _item.proj_projjson,
        'proj:bbox', _item.proj_bbox,
        'proj:centroid', _item.proj_centroid,
        'proj:shape', _item.proj_shape,
        'proj:transform', _item.proj_transform,
        'sci:doi', _item.sci_doi,
        'sci:citation', _item.sci_citation,
        'sci:publications', _item.sci_publications,
        'view:off_nadir', _item.view_off_nadir,
        'view:incidence_angle', _item.view_incidence_angle,
        'view:azimuth', _item.view_azimuth,
        'view:sun_azimuth', _item.view_sun_azimuth,
        'view:sun_elevation', _item.view_sun_elevation,
        'view:moon_azimuth', _item.view_moon_azimuth,
        'view:moon_elevation', _item.view_moon_elevation,
        'file:size', _item.file_size,
        'file:header_size', _item.file_header_size,
        'file:checksum', _item.file_checksum,
        'file:byte_order', _item.file_byte_order,
        'sat:orbit_state', _item.sat_orbit_state,
        'sat:relative_orbit', _item.sat_relative_orbit,
        'sat:absolute_orbit', _item.sat_absolute_orbit,
        'sat:platform_international_designator', _item.sat_platform_international_designator,
        'sat:anx_datetime', CASE WHEN _item.sat_anx_datetime IS NULL THEN NULL ELSE tstz_to_stac_text(_item.sat_anx_datetime) END
    ));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

DROP TRIGGER IF EXISTS items_delete_log_after_delete_trigger ON items;
CREATE TRIGGER items_delete_log_after_delete_trigger
    AFTER DELETE ON items
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT EXECUTE FUNCTION items_delete_log_trigger();




CREATE OR REPLACE FUNCTION stac_links_strip_hrefs(links jsonb) RETURNS jsonb AS $$
    SELECT CASE
        WHEN links IS NULL OR jsonb_typeof(links) <> 'array' OR jsonb_array_length(links) = 0 THEN NULL
        ELSE
            (
                SELECT jsonb_agg(
                    CASE
                        WHEN jsonb_typeof(link) = 'object' THEN link - 'href'
                        ELSE link
                    END
                    ORDER BY ord
                )
                FROM jsonb_array_elements(links) WITH ORDINALITY AS elements(link, ord)
            )
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION stac_links_href_array(links jsonb) RETURNS text[] AS $$
    SELECT CASE
        WHEN links IS NULL OR jsonb_typeof(links) <> 'array' OR jsonb_array_length(links) = 0 THEN NULL
        ELSE ARRAY(
            SELECT link->>'href'
            FROM jsonb_array_elements(links) WITH ORDINALITY AS elements(link, ord)
            ORDER BY ord
        )
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION stac_links_hydrate(
    links_template jsonb,
    link_hrefs text[]
) RETURNS jsonb AS $$
    SELECT CASE
        WHEN links_template IS NULL OR jsonb_typeof(links_template) <> 'array' THEN '[]'::jsonb
        WHEN link_hrefs IS NULL OR array_length(link_hrefs, 1) IS NULL THEN
            COALESCE(
                (SELECT jsonb_agg(link ORDER BY ord)
                 FROM jsonb_array_elements(links_template) WITH ORDINALITY AS elements(link, ord)),
                '[]'::jsonb
            )
        ELSE COALESCE(
            (
                SELECT jsonb_agg(
                    CASE
                        WHEN jsonb_typeof(link) = 'object' AND hrefs.href IS NOT NULL THEN
                            jsonb_set(link - 'href', '{href}', to_jsonb(hrefs.href), true)
                        WHEN jsonb_typeof(link) = 'object' THEN link - 'href'
                        ELSE link
                    END
                    ORDER BY ord
                )
                FROM jsonb_array_elements(links_template) WITH ORDINALITY AS elements(link, ord)
                LEFT JOIN LATERAL (
                    SELECT link_hrefs[ord] AS href
                ) AS hrefs ON TRUE
            ),
            '[]'::jsonb
        )
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
    SELECT
        content->>'id' AS id,
        stac_geom(content) AS geometry,
        content->>'collection' AS collection,
        stac_datetime(content) AS datetime,
        stac_end_datetime(content) AS end_datetime,
        CASE
            WHEN (content->'properties')->'datetime' IS NOT NULL AND (content->'properties')->'datetime' <> 'null'::jsonb THEN FALSE
            ELSE (
                ((content->'properties')->'start_datetime' IS NOT NULL AND (content->'properties')->'start_datetime' <> 'null'::jsonb)
                OR ((content->'properties')->'end_datetime' IS NOT NULL AND (content->'properties')->'end_datetime' <> 'null'::jsonb)
            )
        END AS datetime_is_range,
        content->>'stac_version' AS stac_version,
        COALESCE(content->'stac_extensions', '[]'::jsonb) AS stac_extensions,
        now() AS pgstac_updated_at,
        pgstac.jsonb_hash(content) AS item_hash,
        NULL::bigint AS fragment_id,
        content->'bbox' AS bbox,
        CASE WHEN content->'links' IS NOT NULL AND content->'links' <> '[]'::jsonb THEN content->'links' END AS links,
        CASE WHEN content->'assets' IS NOT NULL AND content->'assets' <> '{}'::jsonb THEN content->'assets' END AS assets,
        strip_promoted_properties(content->'properties') AS properties,
        content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[] AS extra,

        ((content->'properties')->>'created')::timestamptz AS created,
        ((content->'properties')->>'updated')::timestamptz AS updated,
        ((content->'properties')->>'platform') AS platform,
        to_text_array((content->'properties')->'instruments') AS instruments,
        ((content->'properties')->>'constellation') AS constellation,
        ((content->'properties')->>'mission') AS mission,
        ((content->'properties')->>'eo:cloud_cover')::float8 AS eo_cloud_cover,
        ((content->'properties')->'bands') AS bands,
        ((content->'properties')->>'eo:snow_cover')::float8 AS eo_snow_cover,
        ((content->'properties')->>'gsd')::float8 AS gsd,

        ((content->'properties')->>'proj:code') AS proj_code,
        ((content->'properties')->'proj:geometry') AS proj_geometry,
        ((content->'properties')->>'proj:wkt2') AS proj_wkt2,
        ((content->'properties')->'proj:projjson') AS proj_projjson,
        ((content->'properties')->'proj:bbox') AS proj_bbox,
        ((content->'properties')->'proj:centroid') AS proj_centroid,
        ((content->'properties')->'proj:shape') AS proj_shape,
        ((content->'properties')->'proj:transform') AS proj_transform,

        ((content->'properties')->>'sci:doi') AS sci_doi,
        ((content->'properties')->>'sci:citation') AS sci_citation,
        ((content->'properties')->'sci:publications') AS sci_publications,

        ((content->'properties')->>'view:off_nadir')::float8 AS view_off_nadir,
        ((content->'properties')->>'view:incidence_angle')::float8 AS view_incidence_angle,
        ((content->'properties')->>'view:azimuth')::float8 AS view_azimuth,
        ((content->'properties')->>'view:sun_azimuth')::float8 AS view_sun_azimuth,
        ((content->'properties')->>'view:sun_elevation')::float8 AS view_sun_elevation,
        ((content->'properties')->>'view:moon_azimuth')::float8 AS view_moon_azimuth,
        ((content->'properties')->>'view:moon_elevation')::float8 AS view_moon_elevation,

        ((content->'properties')->>'file:size')::bigint AS file_size,
        ((content->'properties')->>'file:header_size')::bigint AS file_header_size,
        ((content->'properties')->>'file:checksum') AS file_checksum,
        ((content->'properties')->>'file:byte_order') AS file_byte_order,

        ((content->'properties')->>'sat:orbit_state') AS sat_orbit_state,
        ((content->'properties')->>'sat:relative_orbit')::integer AS sat_relative_orbit,
        ((content->'properties')->>'sat:absolute_orbit')::integer AS sat_absolute_orbit,
        ((content->'properties')->>'sat:platform_international_designator') AS sat_platform_international_designator,
        ((content->'properties')->>'sat:anx_datetime')::timestamptz AS sat_anx_datetime,
        stac_links_href_array(content->'links') AS link_hrefs,
        -- private is operator-managed metadata outside the STAC item; always NULL from ingest
        NULL::jsonb AS private;
$$ LANGUAGE SQL STABLE;

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

-- content_hydrate: Reassemble a full STAC item JSON from the split columns
-- and the shared fragment content. This is the single hydrate function;
-- the old content_nonhydrated wrapper and 3-arg _collection parameter have
-- been removed.
CREATE OR REPLACE FUNCTION content_hydrate(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    output jsonb;
    frag_content jsonb;
    frag_links_template jsonb;
    merged_assets jsonb;
    merged_properties jsonb;
    hydrated_stac_version text;
    hydrated_stac_extensions jsonb;
    hydrated_links jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;

    -- Fetch shared fragment content (NULL when item has no fragment).
    IF _item.fragment_id IS NOT NULL THEN
        SELECT content, links_template
        INTO frag_content, frag_links_template
        FROM item_fragments
        WHERE id = _item.fragment_id;
    END IF;

    -- Merge: fragment provides shared asset/property values; per-item provides individual values.
    merged_assets     := jsonb_merge_recursive(frag_content->'assets', COALESCE(_item.assets, '{}'::jsonb));
    merged_properties := jsonb_merge_recursive(frag_content->'properties', COALESCE(_item.properties, '{}'::jsonb));
    merged_properties := promoted_properties_from_item(_item) || COALESCE(merged_properties, '{}'::jsonb);
    hydrated_stac_version := COALESCE(_item.stac_version, frag_content->>'stac_version');
    hydrated_stac_extensions := CASE
        WHEN _item.stac_extensions IS NOT NULL AND _item.stac_extensions <> '[]'::jsonb THEN _item.stac_extensions
        ELSE COALESCE(frag_content->'stac_extensions', _item.stac_extensions)
    END;
    IF _item.fragment_id IS NOT NULL THEN
        hydrated_links := stac_links_hydrate(frag_links_template, _item.link_hrefs);
    ELSE
        hydrated_links := COALESCE(_item.links, '[]'::jsonb);
    END IF;

    output := jsonb_build_object(
        'id',         _item.id,
        'geometry',   geom,
        'collection', _item.collection,
        'type',       'Feature'
    );
    IF _item.bbox IS NOT NULL THEN
        output := output || jsonb_build_object('bbox', _item.bbox);
    END IF;
    IF hydrated_stac_version IS NOT NULL THEN
        output := output || jsonb_build_object('stac_version', hydrated_stac_version);
    END IF;
    IF hydrated_stac_extensions IS NOT NULL AND hydrated_stac_extensions <> '[]'::jsonb THEN
        output := output || jsonb_build_object('stac_extensions', hydrated_stac_extensions);
    END IF;
    IF hydrated_links IS NOT NULL THEN
        output := output || jsonb_build_object('links', hydrated_links);
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


CREATE UNLOGGED TABLE items_staging (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_ignore (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_upsert (
    content JSONB NOT NULL
);

-- items_staging_dehydrate: Shared ingest pipeline for all three staging tables.
-- Given the batch of raw STAC item JSONs it dehydrates each into items columns,
-- computes and deduplicates the per-collection fragment payload (inserting new
-- item_fragments rows via ON CONFLICT), assigns fragment_id, and strips
-- fragment-covered keys. Returns the fully-enriched rows as the items rowtype so
-- each staging trigger branch is a single INSERT differing only in conflict
-- policy. The enriched column list lives here once (previously duplicated 3x).
CREATE OR REPLACE FUNCTION items_staging_dehydrate(_contents jsonb[]) RETURNS SETOF items AS $$
        WITH raw AS MATERIALIZED (
            SELECT
                n.content AS orig_content,
                d.*
            FROM unnest(_contents) AS n(content)
            CROSS JOIN LATERAL content_dehydrate(n.content) d
        ),
        fragmented_base AS MATERIALIZED (
            SELECT
                r.*,
                c.fragment_config,
                stac_links_strip_hrefs(r.orig_content->'links') AS links_template,
                extract_fragment(r.orig_content, c.fragment_config) AS frag_content
            FROM raw r
            JOIN collections c ON c.id = r.collection
        ),
        fragmented AS MATERIALIZED (
            SELECT
                fb.*,
                CASE
                    WHEN fb.frag_content IS NULL
                        AND fb.links_template IS NULL THEN NULL
                    ELSE pgstac_hash_fragment(
                        jsonb_strip_nulls(
                            jsonb_build_object(
                                'content', NULLIF(fb.frag_content, '{}'::jsonb),
                                'links_template', fb.links_template
                            )
                        )
                    )
                END AS frag_hash
            FROM fragmented_base fb
        ),
        fragments AS MATERIALIZED (
            SELECT DISTINCT ON (collection, frag_hash)
                collection,
                frag_hash,
                frag_content,
                fragment_config,
                links_template
            FROM fragmented
            WHERE frag_hash IS NOT NULL
            ORDER BY collection, frag_hash
        ),
        insert_fragments AS (
            INSERT INTO item_fragments (collection, hash, content, links_template)
            SELECT collection, frag_hash, COALESCE(frag_content, '{}'::jsonb), links_template
            FROM fragments
            ON CONFLICT (collection, hash) DO NOTHING
            RETURNING id, collection, hash
        ),
        all_fragments AS (
            SELECT id, collection, hash FROM insert_fragments
            UNION ALL
            SELECT f.id, f.collection, f.hash
            FROM item_fragments f
            JOIN fragments p ON f.collection = p.collection AND f.hash = p.frag_hash
        ),
        enriched AS MATERIALIZED (
            SELECT
                r.id,
                r.geometry,
                r.collection,
                r.datetime,
                r.end_datetime,
                r.datetime_is_range,
                CASE
                    WHEN fragment_path_text(ARRAY['stac_version']) = ANY(f.fragment_config) THEN NULL
                    ELSE r.stac_version
                END AS stac_version,
                CASE
                    WHEN fragment_path_text(ARRAY['stac_extensions']) = ANY(f.fragment_config) THEN '[]'::jsonb
                    ELSE r.stac_extensions
                END AS stac_extensions,
                r.pgstac_updated_at,
                r.item_hash,
                af.id AS fragment_id,
                r.bbox,
                CASE
                    WHEN r.link_hrefs IS NOT NULL AND array_length(r.link_hrefs, 1) > 0 THEN NULL
                    ELSE r.links
                END AS links,
                strip_fragment_col(COALESCE(r.assets, '{}'::jsonb), 'assets', f.fragment_config) AS assets,
                strip_fragment_col(COALESCE(r.properties, '{}'::jsonb), 'properties', f.fragment_config) AS properties,
                r.extra,
                r.created,
                r.updated,
                r.platform,
                r.instruments,
                r.constellation,
                r.mission,
                r.eo_cloud_cover,
                r.bands,
                r.eo_snow_cover,
                r.gsd,
                r.proj_code,
                r.proj_geometry,
                r.proj_wkt2,
                r.proj_projjson,
                r.proj_bbox,
                r.proj_centroid,
                r.proj_shape,
                r.proj_transform,
                r.sci_doi,
                r.sci_citation,
                r.sci_publications,
                r.view_off_nadir,
                r.view_incidence_angle,
                r.view_azimuth,
                r.view_sun_azimuth,
                r.view_sun_elevation,
                r.view_moon_azimuth,
                r.view_moon_elevation,
                r.file_size,
                r.file_header_size,
                r.file_checksum,
                r.file_byte_order,
                r.sat_orbit_state,
                r.sat_relative_orbit,
                r.sat_absolute_orbit,
                r.sat_platform_international_designator,
                r.sat_anx_datetime,
                r.link_hrefs,
                NULL::jsonb AS private
            FROM fragmented r
            LEFT JOIN fragments f ON f.collection = r.collection AND f.frag_hash = r.frag_hash
            LEFT JOIN all_fragments af ON af.collection = f.collection AND af.hash = f.frag_hash
        )
    SELECT * FROM enriched;
$$ LANGUAGE SQL;

-- items_staging_triggerfunc: AFTER INSERT trigger on items_staging /
-- items_staging_ignore / items_staging_upsert. Ensures partitions exist, then
-- runs the shared items_staging_dehydrate() pipeline with the per-table conflict
-- policy (and, for upsert, a pre-DELETE of rows whose stored content changed),
-- and finally clears the staging table.
CREATE OR REPLACE FUNCTION items_staging_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
    batch jsonb[];
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

    batch := ARRAY(SELECT content FROM newdata);

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items SELECT * FROM items_staging_dehydrate(batch);
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items SELECT * FROM items_staging_dehydrate(batch)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        -- Delete existing rows whose stored content actually changed, then insert.
        EXECUTE format(
            $sql$
            DELETE FROM items i USING (
                SELECT d.*
                FROM newdata s
                CROSS JOIN LATERAL content_dehydrate(s.content) d
            ) s
            WHERE
                i.id = s.id
                AND i.collection = s.collection
                AND (
                    %s
                )
            $sql$,
            items_content_distinct_sql('i', 's')
        );
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;
        INSERT INTO items SELECT * FROM items_staging_dehydrate(batch)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    END IF;

    RAISE NOTICE 'Deleting data from staging table. %', clock_timestamp() - ts;
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
-- jsonb_field_rows is defined in 001a_jsonutils.sql (loaded first).
-- ---------------------------------------------------------------------------

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
-- (each element is fragment_path_text(text[]) — a JSON-array serialized root-relative path),
-- extract the sparse overlay JSONB that will be stored in item_fragments for dedup.
-- Returns NULL when fragment_paths is NULL or empty, or when no values are found.
-- Supports paths at any depth:
--   depth-1  (e.g. 'stac_version')                  → extracts the whole top-level key.
--   depth-2  (e.g. 'assets.thumbnail')               → extracts a named sub-key.
--   depth-3+ (e.g. 'assets.thumbnail.type')          → extracts a nested sub-sub-key,
--                                                       building a sparse nested JSONB.
-- Multiple paths sharing intermediate keys are merged correctly so that, for example,
-- 'assets.thumbnail.type' and 'assets.thumbnail.roles' together produce
-- {"assets": {"thumbnail": {"type": ..., "roles": ...}}} rather than overwriting.
CREATE OR REPLACE FUNCTION extract_fragment(
    content jsonb,
    fragment_paths text[]
) RETURNS jsonb AS $$
DECLARE
    result    jsonb := '{}'::jsonb;
    p         text;
    pth       text[];
    val       jsonb;
BEGIN
    IF content IS NULL OR fragment_paths IS NULL OR cardinality(fragment_paths) = 0 THEN
        RETURN NULL;
    END IF;

    FOREACH p IN ARRAY fragment_paths LOOP
        pth := fragment_path_array(p);
        IF pth IS NULL OR cardinality(pth) = 0 THEN CONTINUE; END IF;

        val := content #> pth;
        IF val IS NOT NULL THEN
            -- jsonb_set_nested creates intermediate empty objects as needed, so
            -- depth-3+ paths are handled correctly and multiple paths sharing the
            -- same intermediate keys are merged rather than overwritten.
            result := jsonb_set_nested(result, pth, val);
        END IF;
    END LOOP;

    IF result = '{}'::jsonb THEN RETURN NULL; END IF;
    RETURN result;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

-- pgstac_hash_fragment: Hash a fragment payload for dedup. Returns the raw
-- 32-byte sha256 (bytea), stored directly in item_fragments.hash. The fragment
-- jsonb is normalized to a single canonical text by PostgreSQL (equal fragments
-- always serialize identically), so jsonb::text is sufficient for this
-- internal dedup key; the externally reproducible item digest is pgstac_item_hash.
CREATE OR REPLACE FUNCTION pgstac_hash_fragment(fragment jsonb) RETURNS bytea AS $$
SELECT sha256(convert_to(fragment::text, 'UTF8'));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- gc_fragments: Garbage collect orphaned fragments using a single set-based DELETE.
-- Replaces the previous per-collection FOR LOOP with a single statement that lets
-- the planner choose the optimal join/anti-join strategy across all collections.
-- The NOT EXISTS sub-select is evaluated per fragment; with an index on items.fragment_id
-- this is an efficient anti-join rather than a full seq-scan.
--
-- Operational note: because items.fragment_id is intentionally unmanaged by an FK
-- (partitioned-items incremental NOT VALID FKs are not supported), gc_fragments has a
-- small race window with concurrent inserts. A fragment that is unreferenced at the time
-- the DELETE snapshot is taken but becomes referenced by a later insert could be removed.
-- The retention_interval guard makes this unlikely for normal ingest, but operators should
-- still run gc_fragments during low-ingest periods or with a sufficiently conservative
-- retention interval. This is a documented operational tradeoff, not a silent invariant.
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
-- Supports paths at any depth:
--   depth-1  matching col_name  → zeroes out the entire column (returns '{}').
--   depth-2+ matching col_name  → removes the nested sub-path using the #- operator,
--                                  which handles arbitrary nesting without requiring a loop.
-- Examples with col_name='assets':
--   'assets'                  → returns '{}'  (whole column is in the fragment)
--   'assets.thumbnail'        → removes 'thumbnail' key  (depth-2, old behaviour)
--   'assets.thumbnail.type'   → removes 'type' from within 'thumbnail'  (depth-3, new)
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
    n         int;
BEGIN
    IF col_value IS NULL OR fragment_paths IS NULL THEN RETURN col_value; END IF;

    FOREACH p IN ARRAY fragment_paths LOOP
        pth := fragment_path_array(p);
        n   := cardinality(pth);
        IF pth IS NULL OR n = 0 OR pth[1] <> col_name THEN CONTINUE; END IF;

        IF n = 1 THEN
            RETURN '{}'::jsonb;  -- entire column goes to fragment
        ELSE
            -- Remove the nested sub-path from the column value at any depth.
            -- #- handles depth-2 (removes a top-level key) through depth-N (removes a
            -- nested key) using the path tail pth[2:n].
            result := result #- pth[2:n];
        END IF;
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;
