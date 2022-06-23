

PgSTAC includes a Python utility for bulk data loading and managing migrations.

PyPGStac is available on PyPI
```
pip install pypgstac
```

By default, PyPGStac does not install the `psycopg` dependency. If you want the database driver installed, use:

```
pip install pypgstac[psycopg]
```

Or can be built locally
```
git clone https://github.com/stac-utils/pgstac
cd pgstac/pypgstac
pip install .
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

PyPGStac will get the database connection settings from the **standard PG environment variables**:

- PGHOST=0.0.0.0
- PGPORT=5432
- PGUSER=username
- PGDATABASE=postgis
- PGPASSWORD=asupersecretpassword

It can also take a DSN database url "postgresql://..." via the **--dsn** flag.

### Migrations
PyPGStac has a utility to help apply migrations to an existing PGStac instance to bring it up to date.

There are two types of migrations:
 - **Base migrations** install PGStac into a database with no current PGStac installation. These migrations follow the file pattern `"pgstac.[version].sql"`
 - **Incremental migrations** are used to move PGStac from one version to the next. These migrations follow the file pattern `"pgstac.[version].[fromversion].sql"`

Migrations are stored in ```pypgstac/pypgstac/migration`s``` and are distributed with the PyPGStac package.

### Running Migrations
PyPGStac has a utility for checking the version of an existing PGStac database and applying the appropriate migrations in the correct order. It can also be used to setup a database from scratch.

To create an initial PGStac database or bring an existing one up to date, check you have the pypgstac version installed you want to migrate to and run:
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
