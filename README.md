# PGStac
PGDatabase Schema and Functions for Storing and Accessing STAC collections and items in PostgreSQL

Stac Client that uses PGStac available in [Stac FastAPI](https://github.com/stac-utils/stac-fastapi)

PGStac requires Postgresql>=12, PostGIS>=3, and PG_Partman. Best performance will be had using PostgreSQL>=13 and PostGIS>=3.1.


## PyPGStac
PGStac includes a Python utility for bulk data loading and managing migrations.

PyPGStac is available on PyPI
```
pip install pypgstac
```
Or can be built locally using Poetry
```
cd pypgstac
poetry build pypgstac
pip install dist/pypgstac-[version]-py3-none-any.whl
```

## Migrations
To install the latest version of pgstac on an empty database, you can directly load the primary source code.
```
psql -f pgstac.sql
```
Which calls the main SQL files in the sql directory.

All development should take place on these files.

For each new version of PGStac, two migrations should be added to pypgstac/pypgstac/migrations/
 - pgstac.[version].sql (equivalent to `cat sql/*.sql > migration.sql && echo "insert into migrations (versions) VALUES ('[version]')" >> migration.sql)
 - pgstac.[version].[fromversion].sql (Migration to move from existing version to new version, can be created by hand or using the makemigration.sh tool below)

### Running Migrations
Migrations can be installed by either directly running the appropriate migration sql file for from and target PGStac versions:
`psql -f pypgstac/pypgstac/migrations/pgstac.0.1.8.sql`

Or by using pypgstac:
`pypgstac migrate`

### Creating Migrations Using Schema Diff
To create a migration from a previous version of pgstac you can calculate the migration from the running instance of pgstac using the makemigration.sh command. This will use docker to copy the schema of the existing database and the new sql into new docker databases and create/test the migration between the two.
```
makemigration.sh postgresql://myuser:mypassword@myhost:myport/mydatabase
```

## Bulk Data Loading
A python utility is included which allows to load data from any source openable by smart-open using python in a memory efficient streaming manner using PostgreSQL copy. There are options for collections and items and can be used either as a command line or a library.

To load an ndjson of items directly using copy (will fail on any duplicate ids but is the fastest option to load new data you know will not conflict)
```
pypgstac load items
```

To load skipping any records that conflict with existing data
```
pypgstac load items --method insert_ignore
```

To upsert any records, adding anything new and replacing anything with the same id
```
pypgstac load items --method upsert
```

## Development

PGStac uses a dockerized development environment. You can set this up using:

```bash
scripts/setup
```

To bring up the development database:
```
scripts/server
```

To run tests, use:
```bash
scripts/test
```

To rebuild docker images:
```bash
scripts/update
```

To drop into a console, use
```bash
scripts/console
```

To drop into a psql console on the database container, use:
```bash
scripts/console --db
```

To run migrations on the development database, use
```bash
scripts/migrate
```