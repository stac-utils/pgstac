CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate jsonb,
    relative_base timestamptz DEFAULT date_trunc('hour', CURRENT_TIMESTAMP)
) RETURNS tstzrange AS $$
DECLARE
    timestrs text[];
    s timestamptz;
    e timestamptz;
    -- STAC intervals are closed; a date ending one is bumped to the next midnight and left open
    bounds text := '[]';
    empty_reason text;
BEGIN
    timestrs :=
    CASE
        WHEN _indate ? 'timestamp' THEN
            ARRAY[_indate->>'timestamp']
        WHEN _indate ? 'interval' THEN
            to_text_array(_indate->'interval')
        WHEN jsonb_typeof(_indate) = 'array' THEN
            to_text_array(_indate)
        ELSE
            regexp_split_to_array(
                _indate->>0,
                '/'
            )
    END;
    RAISE DEBUG 'TIMESTRS %', timestrs;
    -- An explicit timestamp is the instant it names, matching temporal_operand: without this the
    -- same spelling is a day in the datetime parameter and an instant in a filter.
    IF jsonb_typeof(_indate) = 'object' AND _indate ? 'timestamp' THEN
        s := (_indate->>'timestamp')::timestamptz;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) = 1 THEN
        IF timestrs[1] ILIKE 'P%' THEN
            RETURN tstzrange(relative_base - upper(timestrs[1])::interval, relative_base, '[)');
        END IF;
        -- A bare date is the whole of that day, everywhere: alone, at either end of an
        -- interval, and as a temporal operand. A value carrying a time is the instant it names.
        s := timestrs[1]::timestamptz;
        IF timestrs[1] ~ '^\d{4}-\d{2}-\d{2}$' THEN
            RETURN tstzrange(s, (timestrs[1]::date + 1)::timestamptz, '[)');
        END IF;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) != 2 THEN
        RAISE EXCEPTION 'Timestamp cannot have more than 2 values';
    END IF;

    IF timestrs[2] ~ '^\d{4}-\d{2}-\d{2}$' THEN
        timestrs[2] := (timestrs[2]::date + 1)::text;
        bounds := '[)';
    END IF;

    IF timestrs[1] = '..' OR timestrs[1] = '' THEN
        s := '-infinity'::timestamptz;
        e := timestrs[2]::timestamptz;
        RETURN tstzrange(s,e,bounds);
    END IF;

    IF timestrs[2] = '..' OR timestrs[2] = '' THEN
        s := timestrs[1]::timestamptz;
        e := 'infinity'::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[1] ILIKE 'P%' AND timestrs[2] NOT ILIKE 'P%' THEN
        e := timestrs[2]::timestamptz;
        s := e - upper(timestrs[1])::interval;
        empty_reason := 'its duration is zero';
    ELSIF timestrs[2] ILIKE 'P%' AND timestrs[1] NOT ILIKE 'P%' THEN
        s := timestrs[1]::timestamptz;
        e := s + upper(timestrs[2])::interval;
        empty_reason := 'its duration is zero';
    ELSE
        s := timestrs[1]::timestamptz;
        e := timestrs[2]::timestamptz;
        empty_reason := 'it ends before it starts';
    END IF;

    -- A one day inversion bumps the high end onto the low one. tstzrange raises when the start
    -- is after the end, but a '[)' range whose ends are equal is EMPTY, and an empty range
    -- renders as a comparison against NULL: zero rows, silently.
    IF bounds = '[)' AND s >= e THEN
        RAISE EXCEPTION 'Datetime range % is empty: %.', _indate, empty_reason;
    END IF;

    RETURN tstzrange(s,e,bounds);
END;
$$ LANGUAGE PLPGSQL STABLE STRICT PARALLEL SAFE SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate text,
    relative_base timestamptz DEFAULT CURRENT_TIMESTAMP
) RETURNS tstzrange AS $$
    SELECT parse_dtrange(to_jsonb(_indate), relative_base);
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;


-- One operand of a temporal predicate as the closed interval [low, high], each end either SQL
-- text (a column or index expression) or a timestamptz literal. An instant is [t, t], a date
-- runs to the last microsecond of its day.
CREATE OR REPLACE FUNCTION temporal_operand(
    IN j jsonb,
    IN inside_interval boolean DEFAULT false,
    IN _collection_ids text[] DEFAULT NULL,
    OUT low text,
    OUT high text,
    OUT low_ts timestamptz,
    OUT high_ts timestamptz
) AS $$
DECLARE
    prop text;
    col text;
    ppath text;
    wrapper text;
    isdate boolean;
    qdef jsonb;
    rrange tstzrange;
    ends jsonb;
    s text;
    d date;
BEGIN
    -- Not STRICT: NULL collection_ids must not null the result.
    IF j IS NULL THEN
        RETURN;
    END IF;
    IF jsonb_typeof(j->'property') = 'string' THEN
        prop := j->>'property';
        -- Resolved here rather than through cql2_query, so it needs the same guard: an empty or
        -- properties-only name yields to_tstz() and fails in the executor.
        IF btrim(coalesce(strip_properties_prefix(prop), '')) = ''
           OR btrim(prop) = 'properties' THEN
            RAISE EXCEPTION 'A property name is required.'
                USING HINT = format('Got %s.', j);
        END IF;
        col := queryable_column(prop);
        IF col IN ('datetime', 'end_datetime') THEN
            low := col;
        ELSIF col IS NOT NULL THEN
            RAISE EXCEPTION 'Property % is not temporal.', prop;
        ELSE
            SELECT q.path, q.wrapper, q.definition->>'format' = 'date', q.definition
              INTO ppath, wrapper, isdate, qdef
              FROM queryable(prop, _collection_ids) q;
            -- A registered property must declare itself temporal; otherwise to_tstz fails per
            -- row, so the error depends on the plan. An unregistered one declares nothing and is
            -- read as a timestamp. {"type":"string"} with no format is what missing_queryables
            -- emits for every string property, so it stays acceptable.
            IF wrapper <> 'to_tstz' AND (
                   wrapper IN ('to_int', 'to_float', 'to_text_array')
                OR (qdef ? 'format' AND qdef->>'format' NOT IN ('date', 'date-time'))
                OR (qdef ? 'type' AND NOT (
                        qdef->>'type' = 'string'
                     OR (jsonb_typeof(qdef->'type') = 'array' AND qdef->'type' ? 'string')))
            ) THEN
                RAISE EXCEPTION 'Property % is not temporal.', prop
                    USING HINT = 'A queryable used as a temporal operand needs a string type, a date or date-time format, or an explicit to_tstz property_wrapper.';
            END IF;
            -- to_tstz is forced rather than taken from the queryable: an unregistered
            -- property's wrapper is to_text, which does not typecheck against a timestamptz
            low := format('to_tstz(%s)', ppath);
            IF isdate THEN
                -- Through UTC, because timestamptz + interval advances by a calendar day in the
                -- session's timezone: across a DST change that day is 23 or 25 hours long.
                high := format(
                    '((%s AT TIME ZONE ''UTC'' + interval ''1 day'' - interval ''1 microsecond'') AT TIME ZONE ''UTC'')',
                    low);
                RETURN;
            END IF;
        END IF;
        high := low;
        RETURN;
    END IF;

    -- CQL2 Example 19: each end of an interval is itself an operand, resolved here recursively.
    -- {"interval": [a, b]}, "a/b" and [a, b] all spell the same interval.
    ends := CASE
        WHEN j ? 'interval' THEN j->'interval'
        WHEN jsonb_typeof(j) = 'array' THEN j
        WHEN jsonb_typeof(j) = 'string' AND j #>> '{}' LIKE '%/%' THEN to_jsonb(string_to_array(j #>> '{}', '/'))
    END;
    IF ends IS NOT NULL THEN
        IF inside_interval OR (j ? 'interval' AND (ends->>0 ILIKE 'P%' OR ends->>1 ILIKE 'P%')) THEN
            RAISE EXCEPTION 'An interval end must be a timestamp, a date or a property.';
        END IF;
        IF jsonb_typeof(ends) != 'array' OR jsonb_array_length(ends) != 2 THEN
            RAISE EXCEPTION 'Temporal interval % must have exactly two ends.', j;
        END IF;
        -- a duration end is left open until the other end, which it is relative to, is known
        IF ends->>0 IN ('..', '') OR ends->>0 ILIKE 'P%' THEN
            low_ts := '-infinity';
        ELSE
            SELECT t.low, t.low_ts INTO low, low_ts FROM temporal_operand(ends->0, true, _collection_ids) t;
        END IF;
        IF ends->>1 IN ('..', '') OR ends->>1 ILIKE 'P%' THEN
            high_ts := 'infinity';
        ELSE
            SELECT t.high, t.high_ts INTO high, high_ts FROM temporal_operand(ends->1, true, _collection_ids) t;
        END IF;
        IF ends->>0 ILIKE 'P%' THEN
            low_ts := high_ts - upper(ends->>0)::interval;
        ELSIF ends->>1 ILIKE 'P%' THEN
            high_ts := low_ts + upper(ends->>1)::interval;
        END IF;
        IF (ends->>0 ILIKE 'P%' OR ends->>1 ILIKE 'P%') AND (isfinite(low_ts) AND isfinite(high_ts)) IS NOT TRUE THEN
            RAISE EXCEPTION 'A duration must be paired with a timestamp or a date.';
        END IF;
        IF low_ts > high_ts THEN
            RAISE EXCEPTION 'Temporal interval % ends before it starts.', j;
        END IF;
        RETURN;
    END IF;

    IF (jsonb_typeof(j) = 'string' AND j #>> '{}' ~ '^\d{4}-\d{2}-\d{2}$') OR jsonb_typeof(j->'date') = 'string' THEN
        d := COALESCE(j->>'date', j #>> '{}')::date;
        low_ts := d;
        high_ts := (d + 1)::timestamptz - interval '1 microsecond';
        RETURN;
    END IF;

    s := CASE WHEN jsonb_typeof(j) = 'string' THEN j #>> '{}' ELSE j->>'timestamp' END;
    IF s IS NULL THEN
        RAISE EXCEPTION 'Temporal operand % is not a timestamp, a date, an interval or a property.', j;
    END IF;
    -- An explicit timestamp is the instant it names, even spelled as a bare date. Every other
    -- spelling of a bare date is the whole day, which is what parse_dtrange returns, so this one
    -- must not go through it.
    IF j ? 'timestamp' THEN
        low_ts := s::timestamptz;
        high_ts := low_ts;
        RETURN;
    END IF;

    rrange := parse_dtrange(to_jsonb(s));
    low_ts := lower(rrange);
    -- high_ts is inclusive, and parse_dtrange returns a half-open range whenever it expanded a
    -- bare date. upper() as-is would make the operand a day plus one microsecond.
    high_ts := CASE WHEN upper_inc(rrange) THEN upper(rrange)
                    ELSE upper(rrange) - interval '1 microsecond' END;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE SET TIME ZONE 'UTC';

-- SQL text for one end of a temporal operand: a literal stays a literal so the planner sees a constant.
CREATE OR REPLACE FUNCTION temporal_end(expr text, ts timestamptz) RETURNS text AS $$
    SELECT CASE WHEN ts IS NOT NULL THEN format('%L::timestamptz', ts) ELSE expr END;
$$ LANGUAGE SQL STABLE SET TIME ZONE 'UTC';


CREATE OR REPLACE FUNCTION temporal_op_query(op text, args jsonb, _collection_ids text[] DEFAULT NULL) RETURNS text AS $$
DECLARE
    l RECORD;
    r RECORD;
    outq text;
BEGIN
    -- Not STRICT: NULL collection_ids must not null the result.
    IF op IS NULL OR args IS NULL THEN
        RETURN NULL;
    END IF;
    RAISE DEBUG 'Constructing temporal query OP: %, ARGS: %', op, args;
    op := lower(op);
    -- every comparison has the first operand's end on its left
    outq := CASE op
        WHEN 't_before'       THEN 'lh < rl'
        WHEN 't_after'        THEN 'll > rh'
        WHEN 't_meets'        THEN 'lh = rl'
        WHEN 't_metby'        THEN 'll = rh'
        WHEN 't_overlaps'     THEN 'll < rl AND lh > rl AND lh < rh'
        WHEN 't_overlappedby' THEN 'll > rl AND ll < rh AND lh > rh'
        WHEN 't_starts'       THEN 'll = rl AND lh < rh'
        WHEN 't_startedby'    THEN 'll = rl AND lh > rh'
        WHEN 't_during'       THEN 'll > rl AND lh < rh'
        WHEN 't_contains'     THEN 'll < rl AND lh > rh'
        WHEN 't_finishes'     THEN 'll > rl AND lh = rh'
        WHEN 't_finishedby'   THEN 'll < rl AND lh = rh'
        WHEN 't_equals'       THEN 'll = rl AND lh = rh'
        WHEN 't_disjoint'     THEN 'NOT (ll <= rh AND lh >= rl)'
        WHEN 't_intersects'   THEN 'll <= rh AND lh >= rl'
        WHEN 'anyinteracts'   THEN 'll <= rh AND lh >= rl'
    END;
    IF outq IS NULL THEN
        RAISE EXCEPTION 'Temporal operator % is not supported.', op;
    END IF;
    IF args->0 IS NULL OR args->1 IS NULL THEN
        RAISE EXCEPTION 'Temporal operator % requires two operands.', op;
    END IF;
    SELECT * INTO l FROM temporal_operand(args->0, false, _collection_ids);
    SELECT * INTO r FROM temporal_operand(args->1, false, _collection_ids);
    -- Placeholders first, then one format(): an operand that itself contains the text 'rl' can
    -- never be rescanned as a placeholder. The templates above hold nothing but these four names
    -- and operators, so a plain replace cannot match part of anything else.
    outq := replace(replace(replace(replace(
        outq, 'll', '%1$s'), 'lh', '%2$s'), 'rl', '%3$s'), 'rh', '%4$s');
    RETURN format('(' || outq || ')',
        temporal_end(l.low, l.low_ts), temporal_end(l.high, l.high_ts),
        temporal_end(r.low, r.low_ts), temporal_end(r.high, r.high_ts)
    );
END;
$$ LANGUAGE PLPGSQL STABLE;



CREATE OR REPLACE FUNCTION spatial_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    geom text;
    j jsonb := args->1;
BEGIN
    op := lower(op);
    RAISE DEBUG 'Constructing spatial query OP: %, ARGS: %', op, args;
    IF op NOT IN ('s_equals','s_disjoint','s_touches','s_within','s_overlaps','s_crosses','s_intersects','intersects','s_contains') THEN
        RAISE EXCEPTION 'Spatial Operator % Not Supported', op;
    END IF;
    op := regexp_replace(op, '^s_', 'st_');
    IF op = 'intersects' THEN
        op := 'st_intersects';
    END IF;
    -- Convert geometry to WKB string
    IF j ? 'type' AND j ? 'coordinates' THEN
        geom := st_geomfromgeojson(j)::text;
    ELSIF jsonb_typeof(j) = 'array' THEN
        geom := bbox_geom(j)::text;
    END IF;
    IF geom IS NULL THEN
        RAISE EXCEPTION 'Spatial operand % is not a GeoJSON geometry or a bbox.', j;
    END IF;

    RETURN format('%s(geometry, %L::geometry)', op, geom);
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION query_to_cql2(q jsonb) RETURNS jsonb AS $$
-- Translates anything passed in through the deprecated "query" into equivalent CQL2
WITH t AS (
    SELECT key as property, value as ops
        FROM jsonb_each(q)
), t2 AS (
    SELECT property, (jsonb_each(ops)).*
        FROM t WHERE jsonb_typeof(ops) = 'object'
    UNION ALL
    SELECT property, 'eq', ops
        FROM t WHERE jsonb_typeof(ops) != 'object'
)
SELECT
    jsonb_strip_nulls(jsonb_build_object(
        'op', 'and',
        'args', jsonb_agg(
            jsonb_build_object(
                'op', key,
                'args', jsonb_build_array(
                    jsonb_build_object('property',property),
                    value
                )
            )
        )
    )
) as qcql FROM t2
;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION cql1_to_cql2(j jsonb) RETURNS jsonb AS $$
DECLARE
    ret jsonb;
BEGIN
    RAISE DEBUG 'CQL1_TO_CQL2: %', j;
    IF j ? 'filter' THEN
        RETURN cql1_to_cql2(j->'filter');
    END IF;
    IF jsonb_typeof(j) = 'array' THEN
        SELECT jsonb_agg(cql1_to_cql2(el)) INTO ret FROM jsonb_array_elements(j) el;
        RETURN ret;
    END IF;
    -- scalars, property references, GeoJSON geometries and temporal blocks are literals, not operators
    IF jsonb_typeof(j) != 'object' OR j ?| '{property,type,timestamp,interval}'::text[] THEN
        RETURN j;
    END IF;
    -- every key is an operator whose value is its args; several keys are an implicit AND
    SELECT jsonb_agg(jsonb_build_object(
        'op', key,
        'args', CASE WHEN jsonb_typeof(value) = 'array' THEN cql1_to_cql2(value) ELSE jsonb_build_array(cql1_to_cql2(value)) END
    )) INTO ret FROM jsonb_each(j);
    IF coalesce(jsonb_array_length(ret), 0) <= 1 THEN
        RETURN ret->0;
    END IF;
    RETURN jsonb_build_object('op', 'and', 'args', ret);
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT;

CREATE TABLE cql2_ops (
    op text PRIMARY KEY,
    template text
);



CREATE OR REPLACE FUNCTION cql2_query(j jsonb, wrapper text DEFAULT NULL,
    _collection_ids text[] DEFAULT NULL, _terms_checked boolean DEFAULT false) RETURNS text AS $$
#variable_conflict use_variable
DECLARE
    args jsonb := j->'args';
    arg jsonb;
    op text := lower(j->>'op');
    cql2op RECORD;
    leftarg text;
    rightarg text;
    prop text;
    argdef jsonb;
    declared_wrapper text;
    extra_props bool := pgstac.additional_properties();
BEGIN
    IF j IS NULL THEN
        RETURN NULL;
    END IF;
    IF op IS NOT NULL AND jsonb_typeof(args) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'The % operator requires an array of args.', op;
    END IF;
    RAISE DEBUG 'CQL2_QUERY: %', j;

    -- Once, at the node the caller handed in: $.**.property walks the whole tree, so every
    -- deeper node is already covered and re-running it there only repeats the lookups.
    IF NOT extra_props AND NOT _terms_checked THEN
        FOR prop IN
            SELECT DISTINCT p->>0
            FROM jsonb_path_query(j, 'strict $.**.property') p
        LOOP
            IF NOT (queryable(prop, _collection_ids)).registered THEN
                RAISE EXCEPTION 'Term % is not found in queryables.', prop;
            END IF;
        END LOOP;
    END IF;

    IF j ? 'filter' THEN
        RETURN cql2_query(j->'filter', NULL, _collection_ids, true);
    END IF;

    IF j ? 'upper' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'upper', 'args', jsonb_build_array(j->'upper')), NULL, _collection_ids, true);
    END IF;

    IF j ? 'lower' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'lower', 'args', jsonb_build_array(j->'lower')), NULL, _collection_ids, true);
    END IF;

    -- Temporal Query
    IF op ilike 't_%' or op = 'anyinteracts' THEN
        RETURN temporal_op_query(op, args, _collection_ids);
    END IF;

    -- If property is a timestamp convert it to text to use with
    -- general operators
    IF j ? 'timestamp' THEN
        RETURN format('%L::timestamptz', to_tstz(j->'timestamp'));
    END IF;
    IF j ? 'interval' THEN
        RAISE EXCEPTION 'Please use temporal operators when using intervals.';
    END IF;

    -- Spatial Query
    IF op ilike 's_%' or op = 'intersects' THEN
        RETURN spatial_op_query(op, args);
    END IF;

    IF op IN ('a_equals','a_contains','a_contained_by','a_overlaps') THEN
        IF args->0 ? 'property' THEN
            leftarg := format('to_text_array(%s)', (queryable(args->0->>'property', _collection_ids)).path);
        END IF;
        IF args->1 ? 'property' THEN
            rightarg := format('to_text_array(%s)', (queryable(args->1->>'property', _collection_ids)).path);
        END IF;
        RETURN FORMAT(
            '%s %s %s',
            COALESCE(leftarg, quote_literal(to_text_array(args->0))),
            CASE op
                WHEN 'a_equals' THEN '='
                WHEN 'a_contains' THEN '@>'
                WHEN 'a_contained_by' THEN '<@'
                WHEN 'a_overlaps' THEN '&&'
            END,
            COALESCE(rightarg, quote_literal(to_text_array(args->1)))
        );
    END IF;

    IF op = 'in' THEN
        RAISE DEBUG 'IN : % % %', args, jsonb_build_array(args->0), args->1;
        IF jsonb_typeof(args->1) IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'The in operator takes a value and an array of values.';
        END IF;
        args := jsonb_build_array(args->0) || (args->1);
        RAISE DEBUG 'IN2 : %', args;
    END IF;



    -- Rebuilding args from its own first three elements cannot change it once the length is
    -- known, so only the check remains.
    IF op = 'between' AND jsonb_array_length(args) <> 3 THEN
        RAISE EXCEPTION 'The between operator takes a value, a lower bound and an upper bound.';
    END IF;

    RAISE DEBUG 'ARGS PRE: %', args;
    IF j ? 'args' THEN
        IF EXISTS (
            SELECT FROM jsonb_array_elements(args) a
            WHERE queryable_column(a->>'property') IN ('datetime', 'end_datetime')
        ) THEN
            -- to_tstz reads its argument as UTC when it carries no offset, and is immutable, so
            -- the literal folds to a fixed instant at plan time instead of being cast at
            -- execution in whatever timezone the session happens to have.
            wrapper := 'to_tstz';
        ELSIF EXISTS (
            SELECT FROM jsonb_array_elements(args) a WHERE queryable_column(a->>'property') IS NOT NULL
        ) THEN
            wrapper := NULL;
        ELSE
            -- if any of the arguments are a property, try to get the property_wrapper
            FOR arg IN SELECT jsonb_path_query(args, '$[*] ? (@.property != null)') LOOP
                RAISE DEBUG 'Arg: %', arg;
                SELECT q.nulled_wrapper, q.definition, q.wrapper
                  INTO wrapper, argdef, declared_wrapper
                  FROM queryable(arg->>'property', _collection_ids) q;
                IF wrapper IS NULL AND (argdef ? 'type' OR argdef ? 'format') THEN
                    -- Declared, so its own type decides rather than the literal's: the number
                    -- heuristic below would otherwise read a declared string as a float. A
                    -- definition that is only a $ref or a title declares nothing, so it is left
                    -- to the heuristic exactly as an unregistered property is.
                    wrapper := declared_wrapper;
                END IF;
                RAISE DEBUG 'Property: %, Wrapper: %', arg, wrapper;
                IF wrapper IS NOT NULL THEN
                    EXIT;
                END IF;
            END LOOP;

            -- if the property was not in queryables, see if any args were numbers
            IF
                wrapper IS NULL
                AND jsonb_path_exists(args, '$[*] ? (@.type()=="number")')
            THEN
                wrapper := 'to_float';
            END IF;
            wrapper := coalesce(wrapper, 'to_text');
        END IF;

        SELECT jsonb_agg(cql2_query(a, wrapper, _collection_ids, true))
            INTO args
        FROM jsonb_array_elements(args) a;
    END IF;
    RAISE DEBUG 'ARGS: %', args;

    IF op IN ('and', 'or') THEN
        RETURN
            format(
                '(%s)',
                array_to_string(to_text_array(args), format(' %s ', upper(op)))
            );
    END IF;

    IF op = 'in' THEN
        RAISE DEBUG 'IN --  % %', args->0, to_text(args->0);
        RETURN format(
            '%s IN (%s)',
            to_text(args->0),
            array_to_string((to_text_array(args))[2:], ',')
        );
    END IF;

    IF op IN ('like', 'ilike', 'not_like', 'not_ilike') THEN
        IF wrapper IS NOT NULL AND wrapper NOT IN ('to_text') THEN
            RAISE EXCEPTION 'The % operator compares text, but its operand is read with %.', op, wrapper
                USING HINT = 'Give the queryable a to_text property_wrapper to match it as text.';
        END IF;
        -- A column operand carries no wrapper, so the check above never sees it. Only id and
        -- collection are text columns; the rest fail in the executor.
        IF EXISTS (
            SELECT FROM jsonb_array_elements(j->'args') a
            WHERE queryable_column(a->>'property') IS NOT NULL
              AND queryable_column(a->>'property') NOT IN ('id', 'collection')
        ) THEN
            RAISE EXCEPTION 'The % operator compares text, but its operand is not a text column.', op;
        END IF;
    END IF;

    -- Look up template from cql2_ops
    IF j ? 'op' THEN
        SELECT * INTO cql2op FROM cql2_ops WHERE cql2_ops.op = op;
        IF FOUND THEN
            IF jsonb_array_length(args) >
               (length(cql2op.template) - length(replace(cql2op.template, '%s', ''))) / 2 THEN
                RAISE EXCEPTION 'The % operator was given % arguments, more than it takes.',
                    op, jsonb_array_length(args);
            END IF;
            RETURN format(
                cql2op.template,
                VARIADIC (to_text_array(args))
            );
        ELSE
            RAISE EXCEPTION 'Operator % Not Supported.', op;
        END IF;
    END IF;


    -- A property with no name cannot be resolved; emitting it anyway yields SQL like to_text(),
    -- which fails in the executor rather than here, where the cause is visible.
    IF j ? 'property' AND (
           btrim(coalesce(strip_properties_prefix(j->>'property'), '')) = ''
        OR btrim(coalesce(j->>'property', '')) = 'properties') THEN
        RAISE EXCEPTION 'A property name is required.'
            USING HINT = format('Got %s.', j);
    END IF;

    IF j ? 'property' THEN
        -- A column of items is already the right type; only the literals beside it need the
        -- wrapper that fixes how they are read.
        IF wrapper IS NULL OR queryable_column(j->>'property') IS NOT NULL THEN
            RETURN (queryable(j->>'property', _collection_ids)).path;
        END IF;
        RETURN format('%I(%s)', wrapper, (queryable(j->>'property', _collection_ids)).path);
    ELSIF wrapper IS NOT NULL THEN
        RETURN format('%I(%L)', wrapper, j);
    END IF;

    RETURN quote_literal(to_text(j));
END;
$$ LANGUAGE PLPGSQL STABLE;
