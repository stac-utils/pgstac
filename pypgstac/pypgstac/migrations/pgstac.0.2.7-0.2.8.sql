SET SEARCH_PATH TO pgstac, public;
BEGIN;

INSERT INTO migrations (version) VALUES ('0.2.8');

COMMIT;
