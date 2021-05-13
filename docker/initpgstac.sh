#!/bin/sh

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"
export PGDATABASE="$POSTGRES_DB"

cd /workspaces
# psql -f pgstac.sql
