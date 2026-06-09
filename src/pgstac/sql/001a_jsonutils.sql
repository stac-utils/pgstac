CREATE OR REPLACE FUNCTION to_int(jsonb) RETURNS int AS $$
    SELECT floor(($1->>0)::float)::int;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_float(jsonb) RETURNS float AS $$
    SELECT ($1->>0)::float;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_tstz(jsonb) RETURNS timestamptz AS $$
    SELECT ($1->>0)::timestamptz;
$$ LANGUAGE SQL IMMUTABLE STRICT SET TIME ZONE 'UTC' COST 5000 PARALLEL SAFE;


CREATE OR REPLACE FUNCTION to_text(jsonb) RETURNS text AS $$
    SELECT CASE WHEN jsonb_typeof($1) IN ('array','object') THEN $1::text ELSE $1->>0 END;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_text_array(jsonb) RETURNS text[] AS $$
    SELECT
        CASE jsonb_typeof($1)
            WHEN 'array' THEN ARRAY(SELECT jsonb_array_elements_text($1))
            ELSE ARRAY[$1->>0]
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT COST 5000 PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS geometry AS $$
SELECT CASE jsonb_array_length(_bbox)
    WHEN 4 THEN
        ST_SetSRID(ST_MakeEnvelope(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float,
            (_bbox->>3)::float
        ),4326)
    WHEN 6 THEN
    ST_SetSRID(ST_3DMakeBox(
        ST_MakePoint(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float
        ),
        ST_MakePoint(
            (_bbox->>3)::float,
            (_bbox->>4)::float,
            (_bbox->>5)::float
        )
    ),4326)
    ELSE null END;
;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION geom_bbox(_geom geometry) RETURNS jsonb AS $$
    SELECT jsonb_build_array(
        st_xmin(_geom),
        st_ymin(_geom),
        st_xmax(_geom),
        st_ymax(_geom)
    );
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION flip_jsonb_array(j jsonb) RETURNS jsonb AS $$
    SELECT jsonb_agg(value) FROM (SELECT value FROM jsonb_array_elements(j) WITH ORDINALITY ORDER BY ordinality DESC) as t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION explode_dotpaths(j jsonb) RETURNS SETOF text[] AS $$
    SELECT string_to_array(p, '.') as e FROM jsonb_array_elements_text(j) p;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION explode_dotpaths_recurse(IN j jsonb) RETURNS SETOF text[] AS $$
    WITH RECURSIVE t AS (
        SELECT e FROM explode_dotpaths(j) e
        UNION ALL
        SELECT e[1:cardinality(e)-1]
        FROM t
        WHERE cardinality(e)>1
    ) SELECT e FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


-- jsonb_canonical: RFC 8785 (JSON Canonicalization Scheme)-aligned serialization.
-- Produces a deterministic, key-order-independent text encoding that an external
-- client can reproduce byte-for-byte. NOTE: do NOT use `jsonb::text` for hashing —
-- PostgreSQL re-normalizes object key order to length-then-bytewise and inserts
-- ": " / ", " separators, so `jsonb::text` is neither alphabetical nor compact.
--
-- Canonical rules (must match the external recipe below):
--   * object keys sorted by Unicode code point (== UTF-8 byte order, COLLATE "C"),
--   * compact separators: ',' between members, ':' between key and value,
--   * strings: standard JSON escaping, NON-ASCII left as UTF-8 (no \uXXXX),
--   * numbers: IEEE-754 double, shortest round-trip form (Ryu) — matches
--     PostgreSQL float8 output and ECMAScript Number::toString for in-range
--     values. (STAC numbers are physical quantities; integers beyond 2^53 are
--     out of contract, as in RFC 8785.)
--   * true / false / null as literals.
--
-- External equivalents:
--   Python: an RFC 8785 canonicalizer, or the rule-for-rule reference:
--     def canon(v):
--       if isinstance(v, bool): return 'true' if v else 'false'
--       if v is None: return 'null'
--       if isinstance(v, dict):
--         return '{'+','.join(json.dumps(k,ensure_ascii=False)+':'+canon(v[k])
--                             for k in sorted(v))+'}'
--       if isinstance(v, list): return '['+','.join(canon(x) for x in v)+']'
--       if isinstance(v,(int,float)):
--         f=float(v); return str(int(f)) if f==int(f) and abs(f)<1e16 else repr(f)
--       return json.dumps(v, ensure_ascii=False)
--   Rust: the `rfc8785` crate (serde_jcs) over serde_json::Value.
CREATE OR REPLACE FUNCTION jsonb_canonical(j jsonb) RETURNS text AS $$
    SELECT CASE jsonb_typeof(j)
        WHEN 'object' THEN COALESCE((
            SELECT '{' || string_agg(
                to_json(kv.key)::text || ':' || jsonb_canonical(kv.value),
                ',' ORDER BY kv.key COLLATE "C"
            ) || '}'
            FROM jsonb_each(j) kv
        ), '{}')
        WHEN 'array' THEN COALESCE((
            SELECT '[' || string_agg(jsonb_canonical(e.value), ',' ORDER BY e.ord) || ']'
            FROM jsonb_array_elements(j) WITH ORDINALITY e(value, ord)
        ), '[]')
        WHEN 'number' THEN (j #>> '{}')::float8::text
        ELSE j::text  -- string (JSON-escaped, UTF-8 preserved), 'true' / 'false' / 'null'
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

-- jsonb_hash: raw 32-byte sha256 of the canonical (RFC 8785-aligned) JSON form.
-- Returns bytea so callers store the compact binary digest directly (32 B vs
-- 64-char hex). Use encode(jsonb_hash(j), 'hex') when a printable string is
-- needed for display or external comparison.
-- Externally reproducible: sha256(utf8_bytes(jsonb_canonical(j))).
-- The private jsonb column on items/collections is intentionally excluded — it
-- stores operator metadata outside the STAC item identity contract.
CREATE OR REPLACE FUNCTION jsonb_hash(j jsonb) RETURNS bytea AS $$
    SELECT sha256(convert_to(jsonb_canonical(j), 'UTF8'));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

-- jsonb_field_rows: Recursively walk a JSONB document and emit one row per field path.
-- max_depth guards against runaway recursion on pathologically nested documents.
-- Used by the field registry to track which paths exist in a collection's items.
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

-- jsonb_leaf_rows: Recursively flatten JSONB into dot-path/value rows.
-- Arrays, explicit JSON nulls, and empty objects are treated as atomic values.
-- Non-empty objects recurse into child paths and do not emit their own row.
CREATE OR REPLACE FUNCTION jsonb_leaf_rows(
    data jsonb,
    parent_path text
) RETURNS TABLE (path text, value jsonb) AS $$
DECLARE
    key_text text;
    child jsonb;
    current_path text;
BEGIN
    IF data IS NULL THEN
        RETURN;
    END IF;

    IF jsonb_typeof(data) = 'object' THEN
        IF data = '{}'::jsonb THEN
            IF parent_path <> '' THEN
                RETURN QUERY SELECT parent_path, data;
            END IF;
            RETURN;
        END IF;

        FOR key_text, child IN SELECT * FROM jsonb_each(data) LOOP
            current_path := CASE
                WHEN parent_path = '' THEN key_text
                ELSE parent_path || '.' || key_text
            END;
            RETURN QUERY SELECT * FROM jsonb_leaf_rows(child, current_path);
        END LOOP;
        RETURN;
    END IF;

    IF parent_path <> '' THEN
        RETURN QUERY SELECT parent_path, data;
    END IF;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


-- jsonb_common_values: Keep only leaf values that are identical in both documents.
-- Objects recurse key-by-key; arrays are treated as atomic values and must match exactly.
CREATE OR REPLACE FUNCTION jsonb_common_values(left_doc jsonb, right_doc jsonb) RETURNS jsonb AS $$
    SELECT CASE
        WHEN left_doc IS NULL OR right_doc IS NULL THEN NULL
        WHEN jsonb_typeof(left_doc) = 'object' AND jsonb_typeof(right_doc) = 'object' THEN
            CASE
                WHEN left_doc = '{}'::jsonb AND right_doc = '{}'::jsonb THEN '{}'::jsonb
                ELSE (
                    SELECT CASE
                        WHEN count(*) = 0 THEN NULL
                        ELSE jsonb_object_agg(key, common_value)
                    END
                    FROM (
                        SELECT
                            left_fields.key,
                            jsonb_common_values(left_fields.value, right_fields.value) AS common_value
                        FROM jsonb_each(left_doc) left_fields
                        JOIN jsonb_each(right_doc) right_fields USING (key)
                    ) common_fields
                    WHERE common_value IS NOT NULL
                )
            END
        WHEN left_doc = right_doc THEN left_doc
        ELSE NULL
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


-- jsonb_common_paths_*: aggregate used by Python-side tooling to derive a
-- collection-wide fragment_config from sampled item documents.
--
-- These helpers are NOT called by the runtime SQL ingest/search path. They are
-- kept in the SQL source so Python tooling can execute the same canonical logic
-- inside PostgreSQL when computing candidate fragment paths.
CREATE OR REPLACE FUNCTION jsonb_common_paths_state(state jsonb[], next_doc jsonb) RETURNS jsonb[] AS $$
    SELECT CASE
        WHEN next_doc IS NULL THEN COALESCE(state, '{}'::jsonb[])
        WHEN state IS NULL THEN ARRAY[next_doc]
        ELSE array_append(state, next_doc)
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_common_paths_final(docs jsonb[]) RETURNS text[] AS $$
    WITH normalized_docs AS (
        SELECT content
        FROM unnest(COALESCE(docs, '{}'::jsonb[])) AS doc(content)
        WHERE content IS NOT NULL
    ), doc_count AS (
        SELECT count(*)::bigint AS n
        FROM normalized_docs
    ), flat AS (
        SELECT rows.path, rows.value
        FROM normalized_docs d
        CROSS JOIN LATERAL jsonb_leaf_rows(d.content, '') AS rows(path, value)
    )
    SELECT CASE
        WHEN (SELECT n FROM doc_count) = 0 THEN '{}'::text[]
        ELSE COALESCE(
            (
                SELECT array_agg(path ORDER BY path)
                FROM (
                    SELECT f.path
                    FROM flat f
                    CROSS JOIN doc_count d
                    GROUP BY f.path, d.n
                    HAVING count(*) = d.n AND count(DISTINCT f.value) = 1
                ) same_paths
            ),
            '{}'::text[]
        )
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE AGGREGATE jsonb_common_paths_agg(jsonb) (
    SFUNC = jsonb_common_paths_state,
    STYPE = jsonb[],
    FINALFUNC = jsonb_common_paths_final,
    PARALLEL = SAFE
);


CREATE OR REPLACE FUNCTION jsonb_set_nested(j jsonb, path text[], val jsonb) RETURNS jsonb AS $$
DECLARE
BEGIN
    IF cardinality(path) > 1 THEN
        FOR i IN 1..(cardinality(path)-1) LOOP
            IF j #> path[:i] IS NULL THEN
                j := jsonb_set_lax(j, path[:i], '{}', TRUE);
            END IF;
        END LOOP;
    END IF;
    RETURN jsonb_set_lax(j, path, val, true);

END;
$$ LANGUAGE PLPGSQL IMMUTABLE;



-- jsonb_merge_recursive: Deep-merge two JSONB values with item precedence.
-- Object keys are merged recursively; for non-object collisions the item value wins.
-- Used by hydration so deep fragment paths (depth-3+) and per-item siblings can
-- coexist under shared parent objects without losing data.
--
-- Performance: the split-storage strip removes every fragment-owned key from the
-- per-item column, so at each merge level the fragment sub-object and the per-item
-- sub-object almost always have DISJOINT key sets. When that holds, a shallow
-- concat (`f.value || i.value`, item precedence) is exact and skips a recursive
-- function call. The recursive branch is only taken when keys actually overlap
-- (rare: only for hand-configured fragment paths that split a shared deep object).
-- This disjoint fast-path makes hydrate ~2.5x cheaper on asset-heavy items
-- (e.g. Landsat) versus an unconditional recursive descent, while producing
-- byte-identical output.
CREATE OR REPLACE FUNCTION jsonb_merge_recursive(frag jsonb, item jsonb) RETURNS jsonb AS $$
    SELECT CASE
        WHEN frag IS NULL THEN COALESCE(item, '{}'::jsonb)
        WHEN item IS NULL OR item = '{}'::jsonb THEN frag
        WHEN jsonb_typeof(frag) = 'object' AND jsonb_typeof(item) = 'object' THEN
            COALESCE(
                (
                    SELECT jsonb_object_agg(
                        key,
                        CASE
                            WHEN i.value IS NULL THEN f.value
                            WHEN f.value IS NULL THEN i.value
                            WHEN jsonb_typeof(f.value) = 'object' AND jsonb_typeof(i.value) = 'object' THEN
                                CASE
                                    WHEN NOT EXISTS (
                                        SELECT 1 FROM jsonb_object_keys(f.value) k WHERE i.value ? k
                                    ) THEN f.value || i.value
                                    ELSE jsonb_merge_recursive(f.value, i.value)
                                END
                            ELSE i.value
                        END
                    )
                    FROM jsonb_each(frag) f
                    FULL JOIN jsonb_each(item) i USING (key)
                ),
                '{}'::jsonb
            )
        ELSE item
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION jsonb_include(j jsonb, f jsonb) RETURNS jsonb AS $$
DECLARE
    includes jsonb := f-> 'include';
    outj jsonb := '{}'::jsonb;
    path text[];
BEGIN
    IF
        includes IS NULL
        OR jsonb_array_length(includes) = 0
    THEN
        RETURN j;
    ELSE
        includes := includes || (
            CASE WHEN j ? 'collection' THEN
                '["id","collection"]'
            ELSE
                '["id"]'
            END)::jsonb;
        FOR path IN SELECT explode_dotpaths(includes) LOOP
            outj := jsonb_set_nested(outj, path, j #> path);
        END LOOP;
    END IF;
    RETURN outj;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_exclude(j jsonb, f jsonb) RETURNS jsonb AS $$
DECLARE
    excludes jsonb := f-> 'exclude';
    outj jsonb := j;
    path text[];
BEGIN
    IF
        excludes IS NULL
        OR jsonb_array_length(excludes) = 0
    THEN
        RETURN j;
    ELSE
        FOR path IN SELECT explode_dotpaths(excludes) LOOP
            outj := outj #- path;
        END LOOP;
    END IF;
    RETURN outj;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_fields(j jsonb, f jsonb DEFAULT '{"fields":[]}') RETURNS jsonb AS $$
    SELECT jsonb_exclude(jsonb_include(j, f), f);
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION nullif_jsonbnullempty(j jsonb) RETURNS jsonb AS $$
    SELECT nullif(nullif(nullif(j,'null'::jsonb),'{}'::jsonb),'[]'::jsonb);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION jsonb_array_unique(j jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(jsonb_agg(DISTINCT a)) v FROM jsonb_array_elements(j) a;
$$ LANGUAGE SQL IMMUTABLE;

-- fragment_path_text: Serialize a root-relative path array as a JSON array string
-- suitable for storage in collections.fragment_config text[].
-- This avoids ambiguity for keys that may contain dots.
CREATE OR REPLACE FUNCTION fragment_path_text(_path text[]) RETURNS text AS $$
    SELECT to_jsonb(_path)::text;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

-- fragment_path_array: Convert a serialized fragment path back to a path array
-- suitable for use with the #> operator.
-- Expects JSON-array serialization (e.g. '["assets","B1","type"]').
CREATE OR REPLACE FUNCTION fragment_path_array(_path_text text) RETURNS text[] AS $$
BEGIN
    RETURN ARRAY(
        SELECT jsonb_array_elements_text(_path_text::jsonb)
    );
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION jsonb_concat_ignorenull(a jsonb, b jsonb) RETURNS jsonb AS $$
    SELECT coalesce(a,'[]'::jsonb) || coalesce(b,'[]'::jsonb);
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_least(a jsonb, b jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(least(nullif_jsonbnullempty(a), nullif_jsonbnullempty(b)));
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION jsonb_greatest(a jsonb, b jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(greatest(a, b));
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION first_notnull_sfunc(anyelement, anyelement) RETURNS anyelement AS $$
    SELECT COALESCE($1,$2);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE AGGREGATE first_notnull(anyelement)(
    SFUNC = first_notnull_sfunc,
    STYPE = anyelement
);

CREATE OR REPLACE AGGREGATE jsonb_array_unique_merge(jsonb) (
    STYPE = jsonb,
    SFUNC = jsonb_concat_ignorenull,
    FINALFUNC = jsonb_array_unique
);

CREATE OR REPLACE AGGREGATE jsonb_min(jsonb) (
    STYPE = jsonb,
    SFUNC = jsonb_least
);

CREATE OR REPLACE AGGREGATE jsonb_max(jsonb) (
    STYPE = jsonb,
    SFUNC = jsonb_greatest
);
