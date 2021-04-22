# PGStac
*** work in progress, not feature complete or tested yet ***

PGDatabase Schema and Functions for Storing and Accessing STAC collections and items in PostgreSQL

PGStac requires Postgresql>=12, PostGIS>=3 and PG_Partman

Basic install:
```
psql -f pgstac.sql
```

PG_PartMan requires maintenance functions in order to keep the partition structure up to date.



To create a migration from a previous version of pgstac you can calculate the migration from the running instance of pgstac using the makemigration.sh command. This will use docker to copy the schema of the existing database and the new sql into new docker databases and create/test the migration between the two.
```
makemigration.sh postgresql://myuser:mypassword@myhost:myport/mydatabase
```

To load data using psql fromo ndjson collections or items
```
cat items.ndjson | psql -c "copy items_staging from stdin"
cat collections.ndjson | psql -c "copy collections_staging from stdin"
```

A python utility is included which allows to load data from any source openable by smart-open using python in a memory efficient streaming manner using PostgreSQL copy.

```
from pypgstac import loader
loader.items(file='path/to/file', dsn='postgresql://myuser:mypassword@myhost:myport/mydatabase')
```