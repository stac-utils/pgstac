CREATE OR REPLACE VIEW collections_asitems AS
SELECT
    id,
    geometry,
    'collections' AS collection,
    datetime,
    end_datetime,
    jsonb_build_object(
        'properties', content - '{links,assets,stac_version,stac_extensions}',
        'links', content->'links',
        'assets', content->'assets',
        'stac_version', content->'stac_version',
        'stac_extensions', content->'stac_extensions'
    ) AS content,
    content as collectionjson
FROM collections;


CREATE OR REPLACE FUNCTION collection_search_matched(
    IN _search jsonb DEFAULT '{}'::jsonb,
    OUT matched bigint
) RETURNS bigint AS $$
DECLARE
    _where text := stac_search_to_where(_search);
BEGIN
    EXECUTE format(
        $query$
            SELECT
                count(*)
            FROM
                collections_asitems
            WHERE %s
            ;
        $query$,
        _where
    ) INTO matched;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION collection_search_rows(
    _search jsonb DEFAULT '{}'::jsonb
) RETURNS SETOF jsonb AS $$
DECLARE
    _where text := stac_search_to_where(_search);
    _limit int := coalesce((_search->>'limit')::int, 10);
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    _orderby text;
    _offset int := COALESCE((_search->>'offset')::int, 0);
BEGIN
    _orderby := sort_sqlorderby(
        jsonb_build_object(
            'sortby',
            coalesce(
                _search->'sortby',
                '[{"field": "id", "direction": "asc"}]'::jsonb
            )
        )
    );
    RETURN QUERY EXECUTE format(
        $query$
            SELECT
                jsonb_fields(collectionjson, %L) as c
            FROM
                collections_asitems
            WHERE %s
            ORDER BY %s
            LIMIT %L
            OFFSET %L
            ;
        $query$,
        _fields,
        _where,
        _orderby,
        _limit,
        _offset
    );
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION collection_search(
    _search jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    out_records jsonb;
    number_matched bigint := collection_search_matched(_search);
    number_returned bigint;
    _limit int := coalesce((_search->>'limit')::float::int, 10);
    _offset int := coalesce((_search->>'offset')::float::int, 0);
    links jsonb := '[]';
    ret jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'), '/collections');
    prevoffset int;
    nextoffset int;
BEGIN
    SELECT
        coalesce(jsonb_agg(c), '[]')
    INTO out_records
    FROM collection_search_rows(_search) c;

    number_returned := jsonb_array_length(out_records);



    IF _limit <= number_matched AND number_matched > 0 THEN --need to have paging links
        nextoffset := least(_offset + _limit, number_matched - 1);
        prevoffset := greatest(_offset - _limit, 0);

        IF _offset > 0 THEN
            links := links || jsonb_build_object(
                    'rel', 'prev',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', prevoffset),
                    'merge', TRUE
                );
        END IF;

        IF (_offset + _limit < number_matched)  THEN
            links := links || jsonb_build_object(
                    'rel', 'next',
                    'type', 'application/json',
                    'method', 'GET' ,
                    'href', base_url,
                    'body', jsonb_build_object('offset', nextoffset),
                    'merge', TRUE
                );
        END IF;

    END IF;

    ret := jsonb_build_object(
        'collections', out_records,
        'numberMatched', number_matched,
        'numberReturned', number_returned,
        'links', links
    );
    RETURN ret;

END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
