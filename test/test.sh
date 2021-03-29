#!/bin/bash
tempid=$(cat /dev/urandom | tr -dc 'a-zA-Z' | fold -w 8 | head -n 1)
tempdb="pgstac_tst_${tmpid}"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"


createdb $tempdb;
export PGDATABASE=$tempdb

cd $DIR/..
psql -f pgstac.sql


cat test/testdata/collections.ndjson | psql -c "copy pgstac.collections_staging FROM stdin"
cat test/testdata/items.ndjson | psql -c "copy pgstac.items_staging FROM stdin"

psql -X -f test/test.sql >/tmp/$tempdb.out

cmp --silent /tmp/$tempdb.out test/test.out && echo '### SUCCESS: Files Tests Pass! ###' || echo '### WARNING: Tests did not pass! ###' && diff /tmp/$tempdb.out test/test.out

dropdb $tempdb;
rm /tmp/$tempdb.out