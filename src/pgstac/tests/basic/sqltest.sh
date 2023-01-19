#!/bin/bash
SCRIPTDIR=$(dirname "$0")
cd $SCRIPTDIR
SQLFILE=$(pwd)/$1
TESTDB=$2
SQLOUTFILE=${SQLFILE}.out
PGDATABASE_OLD=$PGDATABASE

echo $SQLFILE
echo $SQLOUTFILE

if [ -z "{$TESTDB}" ]; then
psql <<EOSQL
DROP DATABASE IF EXISTS pgstac_basic_tests WITH (force);
CREATE DATABASE pgstac_basic_tests;
EOSQL
trap 'echo "trap"; psql -c "DROP DATABASE IF EXISTS pgstac_basic_tests WITH (force);"'  0 2 3 15

export PGDATABASE=pgstac_basic_tests
echo $PGDATABASE
pypgstac migrate
else
    export PGDATABASE=$TESTDB
    echo $PGDATABASE
    psql -l
fi

TMPFILE=$(mktemp)
trap 'rm "$TMPFILE"' 0 2 3 15

echo "Running tests in $SQLFILE"
psql -X $PGDATABASE <<EOSQL >"$TMPFILE"
\set QUIET 1
\set ON_ERROR_STOP 1
\set ON_ERROR_ROLLBACK 1

BEGIN;
SET SEARCH_PATH TO pgstac, public;
SET client_min_messages TO 'warning';
SET pgstac.context TO 'on';
SET pgstac."default_filter_lang" TO 'cql-json';

DELETE FROM collections WHERE id = 'pgstac-test-collection';
\copy collections (content) FROM '../testdata/collections.ndjson';
\copy items_staging (content) FROM '../testdata/items.ndjson'

\t

\set QUIET 0
\set ECHO all
$(cat $SQLFILE)
\set QUIET 1
\set ECHO none
ROLLBACK;
EOSQL

if [ "$2" == "generateout" ]; then
    echo "Creating $SQLOUTFILE"
    cat $TMPFILE >$SQLOUTFILE
else
    diff -Z -b -w -B --strip-trailing-cr "$TMPFILE" $SQLOUTFILE
    error=$?
fi

export PGDATABASE=$PGDATABASE_OLD
psql $PGDATABASE <<EOSQL
DROP DATABASE IF EXISTS pgstac_basic_tests WITH (force);
EOSQL

exit $error
