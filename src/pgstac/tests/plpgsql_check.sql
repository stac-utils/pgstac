-- plpgsql_check gate: static-analyze every pgstac PL/pgSQL function and fail on any
-- real error. Run against a fully-installed pgstac database, e.g.
--   psql -d pgstac_test_db_template -f src/pgstac/tests/plpgsql_check.sql
--
-- Known false positives are excluded: plpgsql_check cannot see tables created at
-- runtime, so functions that CREATE TEMP TABLE (search_page -> _search_page_rows,
-- geometrysearch -> pgstac_results) report a spurious "relation ... does not exist".
-- Trigger functions need a relation context and are checked separately (TODO: wire
-- relids); this gate covers all non-trigger PL/pgSQL functions.
CREATE EXTENSION IF NOT EXISTS plpgsql_check;

DO $$
DECLARE
    rec record;
    n_error int := 0;
    n_warning int := 0;
BEGIN
    FOR rec IN
        SELECT (p.oid::regprocedure)::text AS func, c.lineno, c.level, c.message
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL plpgsql_check_function_tb(p.oid) c
        WHERE n.nspname = 'pgstac'
          AND p.prolang = (SELECT oid FROM pg_language WHERE lanname = 'plpgsql')
          AND p.prorettype <> 'trigger'::regtype
          -- only check pgstac's own functions, not extension functions (e.g. pgtap, which
          -- CREATE EXTENSION installs into the pgstac schema) that have their own pg-version quirks.
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend dep
              WHERE dep.classid = 'pg_proc'::regclass AND dep.objid = p.oid AND dep.deptype = 'e'
          )
          -- exclude the runtime-temp-table false positives (plpgsql_check cannot see
          -- a CREATE TEMP TABLE that happens at runtime: search_page -> _search_page_rows,
          -- geometrysearch -> pgstac_results)
          AND NOT (c.level = 'error' AND c.message LIKE 'relation "%" does not exist')
        ORDER BY 3, 1, 2
    LOOP
        IF rec.level = 'error' THEN
            n_error := n_error + 1;
            RAISE WARNING 'plpgsql_check ERROR  %:% %', rec.func, rec.lineno, rec.message;
        ELSE
            n_warning := n_warning + 1;
            RAISE NOTICE 'plpgsql_check %  %:% %', rec.level, rec.func, rec.lineno, rec.message;
        END IF;
    END LOOP;

    RAISE NOTICE 'plpgsql_check: % error(s), % warning(s)', n_error, n_warning;
    IF n_error > 0 THEN
        RAISE EXCEPTION 'plpgsql_check found % error(s)', n_error;
    END IF;
END;
$$;
