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
        IF j ? 'collection' THEN
            includes := includes || '["id","collection"]'::jsonb;
        ELSE
            includes := includes || '["id"]'::jsonb;
        END IF;
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


CREATE OR REPLACE FUNCTION merge_jsonb(_a jsonb, _b jsonb) RETURNS jsonb AS $$
    SELECT
    CASE
        WHEN _a = '"𒍟※"'::jsonb THEN NULL
        WHEN _a IS NULL OR jsonb_typeof(_a) = 'null' THEN _b
        WHEN jsonb_typeof(_a) = 'object' AND jsonb_typeof(_b) = 'object' THEN
            (
                SELECT
                    jsonb_strip_nulls(
                        jsonb_object_agg(
                            key,
                            merge_jsonb(a.value, b.value)
                        )
                    )
                FROM
                    jsonb_each(coalesce(_a,'{}'::jsonb)) as a
                FULL JOIN
                    jsonb_each(coalesce(_b,'{}'::jsonb)) as b
                USING (key)
            )
        WHEN
            jsonb_typeof(_a) = 'array'
            AND jsonb_typeof(_b) = 'array'
            AND jsonb_array_length(_a) = jsonb_array_length(_b)
        THEN
            (
                SELECT jsonb_agg(m) FROM
                    ( SELECT
                        merge_jsonb(
                            jsonb_array_elements(_a),
                            jsonb_array_elements(_b)
                        ) as m
                    ) as l
            )
        ELSE _a
    END
    ;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION strip_jsonb(_a jsonb, _b jsonb) RETURNS jsonb AS $$
    SELECT
    CASE

        WHEN (_a IS NULL OR jsonb_typeof(_a) = 'null') AND _b IS NOT NULL AND jsonb_typeof(_b) != 'null' THEN '"𒍟※"'::jsonb
        WHEN _b IS NULL OR jsonb_typeof(_a) = 'null' THEN _a
        WHEN _a = _b AND jsonb_typeof(_a) = 'object' THEN '{}'::jsonb
        WHEN _a = _b THEN NULL
        WHEN jsonb_typeof(_a) = 'object' AND jsonb_typeof(_b) = 'object' THEN
            (
                SELECT
                    jsonb_strip_nulls(
                        jsonb_object_agg(
                            key,
                            strip_jsonb(a.value, b.value)
                        )
                    )
                FROM
                    jsonb_each(_a) as a
                FULL JOIN
                    jsonb_each(_b) as b
                USING (key)
            )
        WHEN
            jsonb_typeof(_a) = 'array'
            AND jsonb_typeof(_b) = 'array'
            AND jsonb_array_length(_a) = jsonb_array_length(_b)
        THEN
            (
                SELECT jsonb_agg(m) FROM
                    ( SELECT
                        strip_jsonb(
                            jsonb_array_elements(_a),
                            jsonb_array_elements(_b)
                        ) as m
                    ) as l
            )
        ELSE _a
    END
    ;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION nullif_jsonbnullempty(j jsonb) RETURNS jsonb AS $$
    SELECT nullif(nullif(nullif(j,'null'::jsonb),'{}'::jsonb),'[]'::jsonb);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION jsonb_array_unique(j jsonb) RETURNS jsonb AS $$
    SELECT nullif_jsonbnullempty(jsonb_agg(DISTINCT a)) v FROM jsonb_array_elements(j) a;
$$ LANGUAGE SQL IMMUTABLE;

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
