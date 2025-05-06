

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

Migrations are stored in ```pypgstac/pypgstac/migrations``` and are distributed with the pyPgSTAC package.

### Running Migrations
pyPgSTAC has a utility for checking the version of an existing PgSTAC database and applying the appropriate migrations in the correct order. It can also be used to setup a database from scratch.

To create an initial PgSTAC database or bring an existing one up to date, check you have the pypgstac version installed you want to migrate to and run:
```
pypgstac migrate
```

### Bootstrapping an Empty Database

When starting with an empty database, you have two options for initializing PgSTAC:

#### Option 1: Execute as Power User

This approach uses a database user with administrative privileges (such as 'postgres') to run the migration, which will automatically create all necessary extensions and roles:

```bash
# Set environment variables for database connection
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=yourdatabase
export PGUSER=postgres  # A user with admin privileges
export PGPASSWORD=yourpassword

# Run the migration
pypgstac migrate
```

The migration process will automatically:
- Create required extensions (postgis, btree_gist, unaccent)
- Create necessary roles (pgstac_admin, pgstac_read, pgstac_ingest)
- Set up the pgstac schema and tables

In production environments, you should assign these roles to your application database user rather than continuing to use the postgres user:

```sql
-- Grant appropriate roles to your application user
GRANT pgstac_read TO your_app_user;
GRANT pgstac_ingest TO your_app_user;
GRANT pgstac_admin TO your_app_user;

-- Set the search path for your application user
ALTER USER your_app_user SET search_path TO pgstac, public;
```

#### Option 2: Create User with Initial Grants

If you don't have administrative privileges or prefer more control over the setup process, you can manually prepare the database before running migrations.

Connect to your database as an administrator and execute:

```sql
\c [database]

-- Create required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Create required roles
CREATE ROLE pgstac_admin;
CREATE ROLE pgstac_read;
CREATE ROLE pgstac_ingest;

-- Grant appropriate permissions
ALTER DATABASE [database] OWNER TO [user];
ALTER USER [user] SET search_path TO pgstac, public;
ALTER DATABASE [database] set search_path to pgstac, public;
GRANT CONNECT ON DATABASE [database] TO [user];
GRANT ALL PRIVILEGES ON TABLES TO [user];
GRANT ALL PRIVILEGES ON SEQUENCES TO [user];
GRANT pgstac_read TO [user] WITH ADMIN OPTION;
GRANT pgstac_ingest TO [user] WITH ADMIN OPTION;
GRANT pgstac_admin TO [user] WITH ADMIN OPTION;
```

Then run the migration as your non-admin user:

```bash
# Set environment variables for database connection
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=yourdatabase
export PGUSER=[user]  # Your non-admin user
export PGPASSWORD=yourpassword

# Run the migration
pypgstac migrate
```

### Verifying Migration

To verify that PgSTAC was installed correctly:

```bash
# Check the PgSTAC version
pypgstac version
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

### Loading Queryables

Queryables are a mechanism that allows clients to discover what terms are available for use when writing filter expressions in a STAC API. The Filter Extension enables clients to filter collections and items based on their properties using the Common Query Language (CQL2).

To load queryables from a JSON file:

```
pypgstac load_queryables queryables.json
```

To load queryables for specific collections:

```
pypgstac load_queryables queryables.json --collection_ids [collection1,collection2]
```

To load queryables and delete properties not present in the file:

```
pypgstac load_queryables queryables.json --delete_missing
```

To load queryables and create indexes only for specific fields:

```
pypgstac load_queryables queryables.json --index_fields [field1,field2]
```

By default, no indexes are created when loading queryables. Using the `--index_fields` parameter allows you to selectively create indexes only for fields that require them. Creating too many indexes can degrade database performance, especially for write operations, so it's recommended to only index fields that are frequently used in queries.

When using `--delete_missing` with specific collections, only properties for those collections will be deleted:

```
pypgstac load_queryables queryables.json --collection_ids [collection1,collection2] --delete_missing
```

You can combine all parameters as needed:

```
pypgstac load_queryables queryables.json --collection_ids [collection1,collection2] --delete_missing --index_fields [field1,field2]
```

The JSON file should follow the queryables schema as described in the [STAC API - Filter Extension](https://github.com/stac-api-extensions/filter#queryables). Here's an example:

```json
{
  "$schema": "https://json-schema.org/draft/2019-09/schema",
  "$id": "https://example.com/stac/queryables",
  "type": "object",
  "title": "Queryables for Example STAC API",
  "description": "Queryable names for the Example STAC API",
  "properties": {
    "id": {
      "description": "Item identifier",
      "type": "string"
    },
    "datetime": {
      "description": "Datetime",
      "type": "string",
      "format": "date-time"
    },
    "eo:cloud_cover": {
      "description": "Cloud cover percentage",
      "type": "number",
      "minimum": 0,
      "maximum": 100
    }
  },
  "additionalProperties": true
}
```

The command will extract the properties from the JSON file and create queryables in the database. It will also determine the appropriate property wrapper based on the type of each property and create the necessary indexes.

### Automated Collection Extent Updates

By setting `pgstac.update_collection_extent` to `true`, a trigger is enabled to automatically adjust the spatial and temporal extents in collections when new items are ingested. This feature, while helpful, may increase overhead within data load transactions. To alleviate performance impact, combining this setting with `pgstac.use_queue` is beneficial. This approach necessitates a separate process, such as a scheduled task via the `pg_cron` extension, to periodically invoke `CALL run_queued_queries();`. Such asynchronous processing ensures efficient transactional performance and updated collection extents.

*Note: The `pg_cron` extension must be properly installed and configured to manage the scheduling of the `run_queued_queries()` function.*
