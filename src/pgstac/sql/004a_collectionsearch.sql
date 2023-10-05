CREATE OR REPLACE FUNCTION collection_search(
    _search jsonb = '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    _where text := stac_search_to_where(_search);
    _limit int := coalesce((_search->>'limit')::int, 10);
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    query text;
    number_matched bigint;
    out_records jsonb;
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

    query := format(
        $query$
            SELECT
                count(*)
            FROM
                collections
            WHERE %s
            ;
        $query$,
        _where
    );
    EXECUTE query INTO number_matched;
    RAISE NOTICE '% MATCHED %', query, number_matched;

    query := format(
        $query$
            WITH t AS (
            SELECT
                jsonb_fields(content, %L) as c
            FROM
                collections
            WHERE %s
            ORDER BY %s
            LIMIT %L
            OFFSET %L
            )
            SELECT jsonb_agg(c) FROM t
            ;
        $query$,
        _fields,
        _where,
        _orderby,
        _limit,
        _offset
    );
    RAISE NOTICE '%', query;
    EXECUTE query INTO out_records;

    RETURN jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb),
        'context', jsonb_build_object(
            'limit', _limit,
            'matched', number_matched,
            'returned', coalesce(jsonb_array_length(out_records), 0)
        )
    );


END;
$$ LANGUAGE PLPGSQL;
