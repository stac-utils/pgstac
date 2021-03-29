BEGIN;
SET SEARCH_PATH TO pgstac, public;

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Change Primary Key to include datetime column for timescale partitioning
ALTER TABLE items DROP CONSTRAINT items_pkey;
ALTER TABLE items ADD PRIMARY KEY (id, datetime);

SELECT create_hypertable('items', 'datetime');
COMMIT;