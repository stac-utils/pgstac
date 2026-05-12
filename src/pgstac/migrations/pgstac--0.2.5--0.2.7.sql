SET SEARCH_PATH TO pgstac, public;
BEGIN;
CREATE OR REPLACE FUNCTION stac_query_op(att text, _op text, val jsonb) RETURNS text AS $$
DECLARE
ret text := '';
op text;
jp text;
att_parts RECORD;
val_str text;
prop_path text;
BEGIN
val_str := lower(jsonb_build_object('a',val)->>'a');
RAISE NOTICE 'val_str %', val_str;

att_parts := split_stac_path(att);
prop_path := replace(att_parts.dotpath, 'properties.', '');

op := CASE _op
    WHEN 'eq' THEN '='
    WHEN 'gte' THEN '>='
    WHEN 'gt' THEN '>'
    WHEN 'lte' THEN '<='
    WHEN 'lt' THEN '<'
    WHEN 'ne' THEN '!='
    WHEN 'neq' THEN '!='
    WHEN 'startsWith' THEN 'LIKE'
    WHEN 'endsWith' THEN 'LIKE'
    WHEN 'contains' THEN 'LIKE'
    ELSE _op
END;

val_str := CASE _op
    WHEN 'startsWith' THEN concat(val_str, '%')
    WHEN 'endsWith' THEN concat('%', val_str)
    WHEN 'contains' THEN concat('%',val_str,'%')
    ELSE val_str
END;


RAISE NOTICE 'att_parts: % %', att_parts, count_by_delim(att_parts.dotpath,'\.');
IF
    op = '='
    AND att_parts.col = 'properties'
    --AND count_by_delim(att_parts.dotpath,'\.') = 2
THEN
    -- use jsonpath query to leverage index for eqaulity tests on single level deep properties
    jp := btrim(format($jp$ $.%I[*] ? ( @ == %s ) $jp$, replace(att_parts.dotpath, 'properties.',''), lower(val::text)::jsonb));
    raise notice 'jp: %', jp;
    ret := format($q$ properties @? %L $q$, jp);
ELSIF jsonb_typeof(val) = 'number' THEN
    ret := format('properties ? %L AND (%s)::numeric %s %s', prop_path, att_parts.jspathtext, op, val);
ELSE
    ret := format('properties ? %L AND %s %s %L', prop_path ,att_parts.jspathtext, op, val_str);
END IF;
RAISE NOTICE 'Op Query: %', ret;

return ret;
END;
$$ LANGUAGE PLPGSQL;
INSERT INTO migrations (version) VALUES ('0.2.7');

COMMIT;
