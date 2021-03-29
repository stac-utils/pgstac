#!/bin/bash

# set -e

FROMDB=$1

cd /workspaces

TODBURL=postgresql://postgres@localhost:5432/migra_to
GITURL=https://github.com/stac-utils/pgstac.git

# wait for pg_isready
RETRIES=10

until pg_isready || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

psql <<-'EOSQL'
    DROP DATABASE IF EXISTS migra_from;
    CREATE DATABASE migra_from;
    DROP DATABASE IF EXISTS migra_to;
    CREATE DATABASE migra_to;
EOSQL

# Load current workspace into TODB
psql -f pgstac.sql $TODBURL

if [[ $FROMDB = postgresql* ]]
then
    echo "Comparing schema to existing PG instance $FROMDB"
    FROMDBURL=$FROMDB
else
    echo "Comparing schema to $FROMDB branch on github"
    FROMDBURL=postgresql://postgres@localhost:5432/migra_from
    BRANCH=${1:-main}
    rm -fr /tmp/fromdb
    mkdir -p /tmp/fromdb
    cd /tmp/fromdb
    echo "$(pwd) $FROMDBURL $BRANCH $GITURL"
    git clone $GITURL --branch $BRANCH --single-branch
    cd pgstac
    psql $FROMDBURL -f pgstac.sql
fi
mkdir -p /tmp
migra --schema pgstac --unsafe $FROMDBURL $TODBURL >/tmp/migration.sql

# Test migration
echo "testing migration"
psql $FROMDBURL -f /tmp/migration.sql

echo "If there is anything between the ****************** there was a problem with the migration."
echo "***************************"
migra --schema pgstac --unsafe $FROMDBURL $TODBURL
echo "***************************"
