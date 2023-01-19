SET SEARCH_PATH to pgstac, public;
drop function if exists "pgstac"."content_hydrate"(_base_item jsonb, _item jsonb, fields jsonb);

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.content_hydrate(_item jsonb, _base_item jsonb, fields jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT merge_jsonb(
            jsonb_fields(_item, fields),
            jsonb_fields(_base_item, fields)
    );
$function$
;



SELECT set_version('0.6.3');
