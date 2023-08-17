psql -X -q -v ON_ERROR_STOP=1 <<EOSQL
ALTER SYSTEM SET shared_preload_libraries=plrust;
EOSQL
