

PgSTAC includes a Python utility for bulk data loading and managing migrations.

pyPgSTAC is available on PyPI
```
python -m pip install pypgstac
```

By default, pyPgSTAC does not install the `psycopg` dependency. If you want the database driver installed, use:

```
python -m pip install pypgstac[psycopg]
```

Or can be built locally
```
git clone https://github.com/stac-utils/pgstac
cd pgstac/pypgstac
python -m pip install .
```

```
pypgstac --help
Usage: pypgstac [OPTIONS] COMMAND [ARGS]...

Options:
  --install-completion  Install completion for the current shell.
  --show-completion     Show completion for the current shell, to copy it or
                        customize the installation.

  --help                Show this message and exit.

Commands:
  initversion  Get initial version.
  load         Load STAC data into a pgstac database.
  migrate      Migrate a pgstac database.
  pgready      Wait for a pgstac database to accept connections.
  version      Get version from a pgstac database.
```

pyPgSTAC will get the database connection settings from the **standard PG environment variables**:

- PGHOST=0.0.0.0
- PGPORT=5432
- PGUSER=username
- PGDATABASE=postgis
- PGPASSWORD=asupersecretpassword

It can also take a DSN database url "postgresql://..." via the **--dsn** flag.

### Migrations
pyPgSTAC has a utility to help apply migrations to an existing PgSTAC instance to bring it up to date.

There are two types of migrations:

 - **Base migrations** install PgSTAC into a database with no current PgSTAC installation. These migrations follow the file pattern `"pgstac.[version].sql"`
 - **Incremental migrations** are used to move PgSTAC from one version to the next. These migrations follow the file pattern `"pgstac.[version].[fromversion].sql"`

Migrations are stored in ```pypgstac/pypgstac/migration`s``` and are distributed with the pyPgSTAC package.

### Running Migrations
pyPgSTAC has a utility for checking the version of an existing PgSTAC database and applying the appropriate migrations in the correct order. It can also be used to setup a database from scratch.

To create an initial PgSTAC database or bring an existing one up to date, check you have the pypgstac version installed you want to migrate to and run:
```
pypgstac migrate
```

### Bulk Data Loading
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

### Automated Collection Extent Updates

By setting `pgstac.update_collection_extent` to `true`, a trigger is enabled to automatically adjust the spatial and temporal extents in collections when new items are ingested. This feature, while helpful, may increase overhead within data load transactions. To alleviate performance impact, combining this setting with `pgstac.use_queue` is beneficial. This approach necessitates a separate process, such as a scheduled task via the `pg_cron` extension, to periodically invoke `CALL run_queued_queries();`. Such asynchronous processing ensures efficient transactional performance and updated collection extents.

*Note: The `pg_cron` extension must be properly installed and configured to manage the scheduling of the `run_queued_queries()` function.*
