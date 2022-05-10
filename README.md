<p align="center">
  <p align="center">PostgreSQL schema and functions for Spatio Temporal Asset Catalog (STAC)</p>
</p>

<p align="center">
  <a href="https://github.com/stac-utils/pgstac/actions?query=workflow%3ACI" target="_blank">
      <img src="https://github.com/stac-utils/pgstac/workflows/CI/badge.svg" alt="Test">
  </a>

  <a href="https://pypi.org/project/pypgstac" target="_blank">
      <img src="https://img.shields.io/pypi/v/pypgstac?color=%2334D058&label=pypi%20package" alt="Package version">
  </a>
</p>

---

## PGStac

PGDatabase Schema and Functions for Storing and Accessing STAC collections and items in PostgreSQL

STAC Client that uses PGStac available in [STAC-FastAPI](https://github.com/stac-utils/stac-fastapi)

PGStac requires **Postgresql>=13** and **PostGIS>=3**. Best performance will be had using PostGIS>=3.1.

### PGStac Settings
PGStac installs everything into the pgstac schema in the database. This schema must be in the search_path in the postgresql session while using pgstac.


#### PGStac Users
The pgstac_admin role is the owner of all the objects within pgstac and should be used when running things such as migrations.

The pgstac_ingest role has read/write priviliges on all tables and should be used for data ingest or if using the transactions extension with stac-fastapi-pgstac.

The pgstac_read role has read only access to the items and collections, but will still be able to write to the logging tables.

You can use the roles either directly and adding a password to them or by granting them to a role you are already using.

To use directly:
```sql
ALTER ROLE pgstac_read LOGIN PASSWORD '<password>';
```

To grant pgstac permissions to a current postgresql user:
```sql
GRANT pgstac_read TO <user>;
```

#### PGStac Search Path
The search_path can be set at the database level or role level or by setting within the current session. The search_path is already set if you are directly using one of the pgstac users. If you are not logging in directly as one of the pgstac users, you will need to set the search_path by adding it to the search_path of the user you are using:
```sql
ALTER ROLE <user> SET SEARCH_PATH TO pgstac, public;
```
setting the search_path on the database:
```sql
ALTER DATABASE <database> set search_path to pgstac, public;
```

In psycopg the search_path can be set by passing it as a configuration when creating your connection:
```python
kwargs={
    "options": "-c search_path=pgstac,public"
}
```

#### PGStac Settings Variables
There are additional variables that control the settings used for calculating and displaying context (total row count) for a search, as well as a variable to set the filter language (cql-json or cql-json2).
The context is "off" by default, and the default filter language is set to "cql2-json".

Variables can be set either by passing them in via the connection options using your connection library, setting them in the pgstac_settings table or by setting them on the Role that is used to log in to the database.

Turning "context" on can be **very** expensive on larger databases. Much of what PGStac does is to optimize the search of items sorted by time where only fewer than 10,000 records are returned at a time. It does this by searching for the data in chunks and is able to "short circuit" and return as soon as it has the number of records requested. Calculating the context (the total count for a query) requires a scan of all records that match the query parameters and can take a very long time. Settting "context" to auto will use database statistics to estimate the number of rows much more quickly, but for some queries, the estimate may be quite a bit off.

Example for updating the pgstac_settings table with a new value:
```sql
INSERT INTO pgstac_settings (name, value)
VALUES
    ('default-filter-lang', 'cql-json'),
    ('context', 'on')

ON CONFLICT ON CONSTRAINT pgstac_settings_pkey DO UPDATE SET value = excluded.value;
```

Alternatively, update the role:
```sql
ALTER ROLE <username> SET SEARCH_PATH to pgstac, public;
ALTER ROLE <username> SET pgstac.context TO <'on','off','auto'>;
ALTER ROLE <username> SET pgstac.context_estimated_count TO '<number of estimated rows when in auto mode that when an estimated count is less than will trigger a full count>';
ALTER ROLE <username> SET pgstac.context_estimated_cost TO '<estimated query cost from explain when in auto mode that when an estimated cost is less than will trigger a full count>';
ALTER ROLE <username> SET pgstac.context_stats_ttl TO '<an interval string ie "1 day" after which pgstac search will force recalculation of it's estimates>>';
```

#### Runtime Configurations

Runtime configuration of variables can be made with search by passing in configuration in the search json "conf" item.

Runtime configuration is available for context, context_estimated_count, context_estimated_cost, context_stats_ttl, and nohydrate.

The nohydrate conf item returns an unhydrated item bypassing the CPU intensive step of rehydrating data with data from the collection metadata. When using the nohydrate conf, the only fields that are respected in the fields extension are geometry and bbox.
```sql
SELECT search('{"conf":{"nohydrate"=true}}');
```

#### PGStac Partitioning
By default PGStac partitions data by collection (note: this is a change starting with version 0.5.0). Each collection can further be partitioned by either year or month. **Partitioning must be set up prior to loading any data!** Partitioning can be configured by setting the partition_trunc flag on a collection in the database.
```sql
UPDATE collections set partition_trunc='month' WHERE id='<collection id>';
```

In general, you should aim to keep each partition less than a few hundred thousand rows. Further partitioning (ie setting everything to 'month' when not needed to keep the partitions below a few hundred thousand rows) can be detrimental.

#### PGStac Indexes / Queryables
By default, PGStac includes indexes on the id, datetime, collection, geometry, and the eo:cloud_cover property. Further indexing can be added for additional properties globally or only on particular collections by modifications to the queryables table.

Currently indexing is the only place the queryables table is used, but in future versions, it will be extended to provide a queryables backend api.

To add a new global index across all partitions:
```sql
INSERT INTO pgstac.queryables (name, property_wrapper, property_index_type)
VALUES (<property name>, <property wrapper>, <index type>);
```
Property wrapper should be one of to_int, to_float, to_tstz, or to_text. The index type should almost always be 'BTREE', but can be any PostgreSQL index type valid for the data type.

**More indexes is note necessarily better.** You should only index the primary fields that are actively being used to search. Adding too many indexes can be very detrimental to performance and ingest speed. If your primary use case is delivering items sorted by datetime and you do not use the context extension, you likely will not need any further indexes.

## PyPGStac
PGStac includes a Python utility for bulk data loading and managing migrations.

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

## Contribution & Development

PGStac uses a dockerized development environment. However,
it still needs a local install of pypgstac to allow an editable
install inside the docker container. This is installed automatically
if you have set up a virtual environment for the project. Otherwise
you'll need to install a local copy yourself by running `scripts/install`.

To build the docker images and set up the test database, use:

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

To stage code and configurations and create template migrations for a version release, use
```bash
scripts/stageversion [version]
```

Examples:
```
scripts/stageversion 0.2.8
```

This will create a base migration for the new version and will create incremental migrations between any existing base migrations. The incremental migrations that are automatically generated by this script will have the extension ".staged" on the file. You must manually review (and make any modifications necessary) this file and remove the ".staged" extension to enable the migration.

### Making Changes to SQL
All changes to SQL should only be made in the `/sql` directory. SQL Files will be run in alphabetical order.

### Adding Tests
PGStac uses PGTap to test SQL. Tests can be found in tests/pgtap.sql and are run using `scripts/test`


### Release Process
1) Make sure all your code is added and committed
2) Create a PR against the main branch
3) Once the PR has been merged, start the release process.
4) Upate the version in `pypgstac/pypgstac/version.py`
5) Use `scripts/stagerelease` as documented in migrations section above making sure to rename any files ending in ".staged" in the migrations section
6) Add details for release to the CHANGELOG
7) Add/Commit any changes
8) Run tests `scripts/test`
9) Create a git tag `git tag v0.2.8` using new version number
10) Push the git tag `git push origin v0.2.8`
11) The CI process will push pypgstac to PyPi, create a docker image on ghcr.io, and create a release on github.
