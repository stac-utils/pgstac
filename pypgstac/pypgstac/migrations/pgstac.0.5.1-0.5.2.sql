SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.delete_item(_id text, _collection text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id AND (_collection IS NULL OR collection=_collection) RETURNING * INTO STRICT out;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.stac_daterange(value jsonb)
 RETURNS tstzrange
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE
 SET "TimeZone" TO 'UTC'
AS $function$
DECLARE
    props jsonb := value;
    dt timestamptz;
    edt timestamptz;
BEGIN
    IF props ? 'properties' THEN
        props := props->'properties';
    END IF;
    IF props ? 'start_datetime' AND props ? 'end_datetime' THEN
        dt := props->>'start_datetime';
        edt := props->>'end_datetime';
        IF dt > edt THEN
            RAISE EXCEPTION 'start_datetime must be < end_datetime';
        END IF;
    ELSE
        dt := props->>'datetime';
        edt := props->>'datetime';
    END IF;
    IF dt is NULL OR edt IS NULL THEN
        RAISE EXCEPTION 'Either datetime or both start_datetime and end_datetime must be set.';
    END IF;
    RETURN tstzrange(dt, edt, '[]');
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.update_item(content jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    old items %ROWTYPE;
    out items%ROWTYPE;
BEGIN
    PERFORM delete_item(content->>'id', content->>'collection');
    PERFORM create_item(content);
END;
$function$
;



SELECT set_version('0.5.2');
