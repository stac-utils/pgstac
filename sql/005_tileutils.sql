SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION quadkey(IN tx int, ty int, zoom int) RETURNS text AS $$
DECLARE
i int;
digit int;
quadkey text := '';
mask int;
BEGIN
--ty := (ty ^ zoom) - ty;
FOR i IN REVERSE zoom..1 LOOP
    digit := 0;
    mask := 1 << (i-1);
    IF (tx & mask) != 0 THEN
        digit :=  digit + 1;
    END IF;
    IF (ty & mask) != 0 THEN
        digit :=  digit + 2;
    END IF;
    quadkey := concat(quadkey, digit::text);
END LOOP;
RETURN quadkey;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION quadkeytotile(IN quadkey text, OUT zoom int, OUT x int, OUT y int) RETURNS RECORD AS $$
DECLARE
mask int;
c text;
BEGIN
x := 0;
y:= 0;
zoom := length(quadkey);
FOR i IN REVERSE zoom..1 LOOP
    mask := 1 << (i-1);
    c := substr(quadkey, zoom - i + 1, 1);
    IF c = '1' THEN
        x := x | mask;
    END IF;
    IF c = '2' THEN
        y := y | mask;
    END IF;
    IF c = '3' THEN
        x := x | mask;
        y := y | mask;
    END IF;
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tileenvelope(zoom int, x int, y int) RETURNS geometry AS $$
WITH t AS (
    SELECT
        20037508.3427892 as merc_max,
        -20037508.3427892 as merc_min,
        (2 * 20037508.3427892) / (2 ^ zoom) as tile_size
)
SELECT st_makeenvelope(
    merc_min + (tile_size * x),
    merc_max - (tile_size * (y + 1)),
    merc_min + (tile_size * (x + 1)),
    merc_max - (tile_size * y),
    3857
) FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;DROP FUNCTION IF EXISTS mercgrid;


CREATE OR REPLACE FUNCTION mercgrid(IN _zoom int, IN _x int, IN _y int, IN levels int DEFAULT 4, OUT quadkey text, OUT geom geometry) RETURNS SETOF record AS $$
DECLARE
quadkey text;
x int := _x;
y int := _y;
zoom int := _zoom;
merc_max float := 20037508.3427892;
merc_min float := - merc_max;
tile_size float;
initx float;
finalx float;
inity float;
finaly float;
BEGIN
RAISE NOTICE 'x: % y: % zoom: % levels: % quadkey: %', x, y, zoom, levels, quadkey;
IF levels < 0 THEN
    quadkey := quadkey(x, y, zoom);
    RAISE NOTICE 'quadkey: %', quadkey;
    quadkey := substr(quadkey,1,length(quadkey)+levels);
    RAISE NOTICE 'quadkey adjusted: %', quadkey;
    SELECT * INTO zoom, x, y FROM quadkeytotile(quadkey);
    levels := - levels;
END IF;
RAISE NOTICE 'x: % y: % zoom: % levels: % quadkey: %', x, y, zoom, levels, quadkey;
tile_size := (2 * merc_max) / (2 ^ (zoom + levels));
initx := 2^levels * x;
finalx := (2^levels * (x+1))-1;
inity := 2^levels * y;
finaly := (2^levels * (y+1))-1;
RAISE NOTICE 'initx: %, inity: %, finalx: %, finaly: %', initx, inity, finalx, finaly;
RETURN QUERY
SELECT
    quadkey(dx, dy, zoom + levels),
    st_makeenvelope(
        merc_min + (tile_size * dx),
        merc_max - (tile_size * (dy + 1)),
        merc_min + (tile_size * (dx + 1)),
        merc_max - (tile_size * dy),
        3857
    )
FROM
    generate_series(initx::int, finalx::int, 1) dx,
    generate_series(inity::int, finaly::int, 1) dy
;

RETURN;

END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION mercgrid_fromgeom(IN _zoom int, IN _geom geometry, OUT quadkey text, OUT geom geometry) RETURNS SETOF record AS $$
DECLARE
quadkey text;
-- x int := _x;
-- y int := _y;
zoom int := _zoom;
merc_max float := 20037508.3427892;
merc_min float := - merc_max;
tile_size float;
initx float;
finalx float;
inity float;
finaly float;
geom geometry := st_transform(_geom, 3857);
BEGIN
tile_size := (2 * merc_max) / (2 ^ zoom);
initx := floor( (st_xmin(geom) + merc_max) / tile_size );
finalx := ceil( (st_xmax(geom) + merc_max) / tile_size );
inity := floor( (st_ymin(geom) + merc_max) / tile_size );
finaly := ceil( (st_ymax(geom) + merc_max) / tile_size );
-- RAISE NOTICE 'tile_size: %, initx: %, inity: %, finalx: %, finaly: %', tile_size, initx, inity, finalx, finaly;
RETURN QUERY
SELECT
    quadkey(dx, dy, zoom),
    st_makeenvelope(
        merc_min + (tile_size * initx),
        merc_min + (tile_size * inity),
        merc_min + (tile_size * (initx + 1 )),
        merc_min + (tile_size * (inity + 1 )),
        3857
    )
FROM
    generate_series(initx::int, finalx::int, 1) dx,
    generate_series(inity::int, finaly::int, 1) dy
;

RETURN;

END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION ftime() RETURNS interval as $$
SELECT age(clock_timestamp(), transaction_timestamp());
$$ LANGUAGE SQL;
