-- collections_asitems: Exposes collections as pseudo-items for CQL2 filtering.
-- The 'properties' column is the collection content with the standard top-level
-- STAC fields stripped, so CQL2 expressions like {"property":"title"} resolve
-- correctly when filtering collections via /collections?filter=... endpoints.
CREATE OR REPLACE VIEW collections_asitems AS
SELECT
    id,
    geometry,
    'collections' AS collection,
    datetime,
    end_datetime,
    -- Expose collection metadata as properties so CQL2 {"property":"title"} etc. work.
    content - '{links,assets,stac_version,stac_extensions}' AS properties,
    jsonb_build_object(
        'properties', content - '{links,assets,stac_version,stac_extensions}',
        'links', content->'links',
        'assets', content->'assets',
        'stac_version', content->'stac_version',
        'stac_extensions', content->'stac_extensions'
    ) AS content,
    content as collectionjson
FROM collections;


-- collection_search_plan: the collection counterpart of search_plan -- the CLIENT-STREAMING entry
-- for collections. Returns the data query (collection content + keyset keys) the client PREPAREs and
-- the always-on numberMatched query. Built from the SAME building blocks as search_plan/search_page
-- (search_to_cql2 -> cql2_query, keyset_*). No datetime bands: collections aren't partitioned by
-- time. Unlike items, collections aren't split-storage, so the query yields the collection content
-- jsonb directly (no client-side assembly) -- which is why collection_search() can run it as-is.
--   query     : SELECT jsonb_fields(collectionjson, <fields>) AS content, <keys> AS keys
--               FROM collections_asitems WHERE <where + keyset seek> ORDER BY <orderby> LIMIT $1
--   ctx_query : SELECT count(*) FROM collections_asitems WHERE <where>   (numberMatched, always)
CREATE OR REPLACE FUNCTION collection_search_plan(
    _search jsonb DEFAULT '{}'::jsonb,
    _token  text  DEFAULT NULL,
    OUT query     text,
    OUT ctx_query text
) RETURNS record AS $$
DECLARE
    is_prev boolean := _token LIKE 'prev:%';
    keyset text := nullif(regexp_replace(coalesce(_token, ''), '^(next|prev):', ''), '');
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    -- collections default to id ASC unless the caller supplied an explicit sortby.
    _eff jsonb := CASE WHEN _search ? 'sortby' THEN _search
                       ELSE _search || '{"sortby":[{"field":"id","direction":"asc"}]}'::jsonb END;
    _cql2 jsonb := search_to_cql2(_search);
    _where text := coalesce(nullif(btrim(cql2_query(_cql2)), ''), 'TRUE');
    orderby_str text; keys_proj text; keyset_w text; full_where text;
BEGIN
    -- effective ORDER BY (reversed for prev) + the key projection, from the keyset keys.
    SELECT string_agg(expr || ' ' || CASE WHEN is_prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END, ', ' ORDER BY ord),
           'ARRAY[' || string_agg(format('(%s)::text', expr), ',' ORDER BY ord) || ']::text[]'
    INTO orderby_str, keys_proj
    FROM keyset_sortkeys(_eff);

    IF keyset IS NOT NULL THEN
        keyset_w := keyset_where(_eff, keyset_decode(keyset), is_prev);
    END IF;
    full_where := concat_ws(' AND ', _where, keyset_w);
    IF full_where IS NULL OR btrim(full_where) = '' THEN full_where := 'TRUE'; END IF;

    query := format(
        $q$ SELECT jsonb_fields(collectionjson, %L) AS content, %s AS keys
            FROM collections_asitems WHERE %s ORDER BY %s LIMIT $1 $q$,
        _fields, keys_proj, full_where, orderby_str);
    ctx_query := format('SELECT count(*) FROM collections_asitems WHERE %s', _where);
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path TO pgstac, public;


-- collection_search: keyset-paginated collection listing.
-- Builds its data + numberMatched queries via collection_search_plan (one source of truth, shared
-- with the client-streaming path), fetches _limit+1 to detect a further page, and links next/prev as
-- opaque keyset tokens. Collections default to id ASC. numberMatched is ALWAYS returned (small table).
CREATE OR REPLACE FUNCTION collection_search(
    _search jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    _limit int := coalesce((_search->>'limit')::float::int, 10);
    _token text := _search->>'token';
    is_prev boolean := _token LIKE 'prev:%';
    keyset text := nullif(regexp_replace(coalesce(_token, ''), '^(next|prev):', ''), '');
    _q text; _ctx text;
    number_matched bigint;
    number_returned bigint;
    acc jsonb := '[]'::jsonb; cnt int := 0;
    first_k text[]; last_k text[]; fwd_first_k text[]; fwd_last_k text[];
    has_more boolean; next_present boolean; prev_present boolean;
    next_tok text; prev_tok text;
    links jsonb := '[]'::jsonb;
    burl text := concat(rtrim(base_url(_search->'conf'), '/'), '/collections');
    -- typed loop targets (not a `record`) so plpgsql_check can analyze the dynamic EXECUTE.
    _content jsonb; _keys text[];
BEGIN
    SELECT query, ctx_query INTO _q, _ctx FROM collection_search_plan(_search, _token);
    EXECUTE _ctx INTO number_matched;   -- numberMatched, always

    -- the plan query returns (content, keys); +1 over _limit detects a further page.
    FOR _content, _keys IN EXECUTE _q USING (_limit + 1)
    LOOP
        cnt := cnt + 1;
        IF cnt = 1 THEN first_k := _keys; END IF;
        IF cnt <= _limit THEN acc := acc || _content; last_k := _keys; END IF;
    END LOOP;

    has_more := cnt > _limit;
    IF is_prev THEN
        acc := flip_jsonb_array(acc);
        fwd_first_k := last_k; fwd_last_k := first_k;
        next_present := (keyset IS NOT NULL);   -- a prev page always has the origin ahead
        prev_present := has_more;                -- ... and a further-back page iff more remain
    ELSE
        fwd_first_k := first_k; fwd_last_k := last_k;
        next_present := has_more;
        prev_present := (keyset IS NOT NULL);
    END IF;
    IF cnt > 0 AND next_present THEN next_tok := keyset_encode(fwd_last_k); END IF;
    IF cnt > 0 AND prev_present THEN prev_tok := keyset_encode(fwd_first_k); END IF;

    number_returned := jsonb_array_length(acc);

    IF next_tok IS NOT NULL THEN
        links := links || jsonb_build_object('rel', 'next', 'type', 'application/json',
            'method', 'GET', 'href', burl || '?token=next:' || next_tok);
    END IF;
    IF prev_tok IS NOT NULL THEN
        links := links || jsonb_build_object('rel', 'prev', 'type', 'application/json',
            'method', 'GET', 'href', burl || '?token=prev:' || prev_tok);
    END IF;

    RETURN jsonb_build_object(
        'collections', acc,
        'numberMatched', number_matched,
        'numberReturned', number_returned,
        'links', links
    );
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
