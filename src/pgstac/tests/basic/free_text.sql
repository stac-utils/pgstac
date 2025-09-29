
SET pgstac.context TO 'on';
SET pgstac."default_filter_lang" TO 'cql2-json';

CREATE TEMP TABLE temp_collections (
    id SERIAL PRIMARY KEY,
    title TEXT,
    description TEXT,
    keywords TEXT,
    minx NUMERIC,
    miny NUMERIC,
    maxx NUMERIC,
    maxy NUMERIC,
    sdt TIMESTAMPTZ,
    edt TIMESTAMPTZ
);

INSERT INTO temp_collections (
  title, description, keywords, minx, miny, maxx, maxy, sdt, edt
) VALUES
    -- no keywords
    (
        'Stranger Things',
        'Some teenagers drop out of school to fight scary monsters',
        null,
        -180, -90, 180, 90,
        '2016-01-01T00:00:00Z',
        '2025-12-31T23:59:59Z'
    ),
    (
        'The Bear',
        'Another story about why you should not start a restaurant',
        'restaurant, funny, sad, great',
        -180, -90, 180, 90,
        '2022-01-01T00:00:00Z',
        '2025-12-31T23:59:59Z'
    ),
    (
        'Godzilla',
        'A large lizard takes its revenge',
        'scary, lizard, monster',
        -180, -90, 180, 90,
        '1954-01-01T00:00:00Z',
        null
    ),
    (
        'Chefs Table',
        'Another great story that make you wonder if you should go to a restaurant',
        'restaurant, food, michelin',
        -180, -90, 180, 90,
        '2019-01-01T00:00:00Z',
        '2025-12-31T23:59:59Z'
    ),
    -- no title
    (
        null,
        'A humoristic portrayal of office life',
        'Scranton, paper',
        -180, -90, 180, 90,
        '2005-01-01T00:00:00Z',
        '2013-12-31T23:59:59Z'
    );

SELECT
    create_collection(jsonb_build_object(
        'id', format('testcollection_%s', id),
        'type', 'Collection',
        'title', title,
        'description', description,
        'extent', jsonb_build_object(
            'spatial', jsonb_build_array(jsonb_build_array(minx, miny, maxx, maxy)),
            'temporal', jsonb_build_array(jsonb_build_array(sdt, edt))
        ),
        'stac_extensions', jsonb_build_array(),
        'keywords', string_to_array(keywords, ', ')
    ))
FROM temp_collections;

select collection_search('{"q": "monsters"}');

select collection_search('{"q": "lizard"}');

select collection_search('{"q": "scary OR funny"}');

select collection_search('{"q": "(scary AND revenge) OR (funny AND sad)"}');

select collection_search('{"q": "\"great story\""}');

select collection_search('{"q": "monster -school"}');

select collection_search('{"q": "+restaurant -sad"}');

select collection_search('{"q": "+restaurant"}');

select collection_search('{"q": "bear or stranger"}');

select collection_search('{"q": "bear OR stranger"}');

select collection_search('{"q": "bear, stranger"}');

select collection_search('{"q": "bear AND stranger"}');

select collection_search('{"q": "bear and stranger"}');

select collection_search('{"q": "\"bear or stranger\""}');

select collection_search('{"q": "office"}');

select collection_search('{"q": ["bear", "stranger"]}');

select collection_search('{"q": "large lizard"}');

select collection_search('{"q": "teenagers fight monsters"}');
