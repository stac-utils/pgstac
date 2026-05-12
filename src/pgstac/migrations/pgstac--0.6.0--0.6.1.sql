SET SEARCH_PATH to pgstac, public;
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.to_text(jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
    SELECT CASE WHEN jsonb_typeof($1) IN ('array','object') THEN $1::text ELSE $1->>0 END;
$function$
;



SELECT set_version('0.6.1');
