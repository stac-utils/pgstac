psql -X -q -v ON_ERROR_STOP=1 <<EOSQL
ALTER SYSTEM SET shared_preload_libraries=plrust;
ALTER SYSTEM SET plrust.work_dir='/tmp';
CREATE EXTENSION plrust;
EOSQL
