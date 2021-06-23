CREATE TABLE unlogged searches (
    searches_id bigint generated always as identity primary key,
    _where text,
    _dtrange tstzrange,
    _orderby text
);


CREATE TABLE search_quadkey_cache(
    search_quadkeys_id bigint generated always as identity primary key,
    searches_id bigint references searches (searches_id),
    quadkey text NOT NULL,
    meta_quadkey text NOT NULL
    UNIQUE (searches_id, quadkey)
