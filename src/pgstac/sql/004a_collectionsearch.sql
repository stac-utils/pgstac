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
                collections
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
                jsonb_fields(content, %L) as c
            FROM
                collections
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
    _limit int := coalesce((_search->>'limit')::int, 10);
BEGIN
    SELECT
        coalesce(jsonb_agg(c), '[]')
    INTO out_records
    FROM collection_search_rows(_search) c;
    RETURN jsonb_build_object(
        'type', 'FeatureCollection',
        'features', out_records,
        'context', jsonb_build_object(
            'limit', _search->'limit',
            'matched', number_matched,
            'returned', jsonb_array_length(out_records)
        )
    );
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
