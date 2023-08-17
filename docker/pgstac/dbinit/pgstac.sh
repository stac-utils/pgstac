SYSMEM=$(cat /proc/meminfo | grep -i 'memtotal' | grep -o '[[:digit:]]*')
SHARED_BUFFERS=$(( $SYSMEM/4 ))
EFFECTIVE_CACHE_SIZE=$(( $SYSMEM*3/4 ))
MAINTENANCE_WORK_MEM=$(( $SYSMEM/8 ))
WORK_MEM=$(( $SHARED_BUFFERS/50 ))

psql -X -q -v ON_ERROR_STOP=1 <<EOSQL
ALTER SYSTEM SET search_path TO pgstac, public;
ALTER SYSTEM SET client_min_messages TO WARNING;
ALTER SYSTEM SET shared_buffers='${SHARED_BUFFERS}kB';
ALTER SYSTEM SET effective_cache_size='${EFFECTIVE_CACHE_SIZE}kB';
ALTER SYSTEM SET maintenance_work_mem='${MAINTENANCE_WORK_MEM}kB';
ALTER SYSTEM SET work_mem='${WORK_MEM}kB';
ALTER SYSTEM SET effective_io_concurrency=200;
ALTER SYSTEM SET random_page_cost=1.1;
EOSQL