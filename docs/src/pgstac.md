
PGDatabase Schema and Functions for Storing and Accessing STAC collections and items in PostgreSQL

STAC Client that uses PgSTAC available in [STAC-FastAPI](https://github.com/stac-utils/stac-fastapi)

PgSTAC requires **Postgresql>=13** and **PostGIS>=3**. Best performance will be had using PostGIS>=3.1.

### PgSTAC Settings
PgSTAC installs everything into the pgstac schema in the database. This schema must be in the search_path in the postgresql session while using pgstac.


#### PgSTAC Users
The `pgstac_admin` role is the owner of all the objects within pgstac and should be used when running things such as migrations.

The `pgstac_ingest` role has read/write privileges on all tables and should be used for data ingest or if using the transactions extension with stac-fastapi-pgstac.

The `pgstac_read` role has read only access to the items and collections, but will still be able to write to the logging tables.

You can use the roles either directly and adding a password to them or by granting them to a role you are already using.

To use directly:
```sql
ALTER ROLE pgstac_read LOGIN PASSWORD '<password>';
```

To grant pgstac permissions to a current postgresql user:
```sql
GRANT pgstac_read TO <user>;
```

#### PgSTAC Search Path
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

#### PgSTAC Settings Variables
There are additional variables that control the settings used for calculating and displaying context (total row count) for a search, as well as a variable to set the filter language (cql-json or cql2-json).
The context is "off" by default, and the default filter language is set to "cql2-json".

Variables can be set either by passing them in via the connection options using your connection library, setting them in the pgstac_settings table or by setting them on the Role that is used to log in to the database.

Turning "context" on can be **very** expensive on larger databases. Much of what PgSTAC does is to optimize the search of items sorted by time where only fewer than 10,000 records are returned at a time. It does this by searching for the data in chunks and is able to "short circuit" and return as soon as it has the number of records requested. Calculating the context (the total count for a query) requires a scan of all records that match the query parameters and can take a very long time. Setting "context" to auto will use database statistics to estimate the number of rows much more quickly, but for some queries, the estimate may be quite a bit off.

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

The check_pgstac_settings function can be used to check what pgstac settings are being used and to check recommendations for system settings. It takes a single parameter which should be the amount of memory available on the database system.
```sql
SELECT check_pgstac_settings('16GB');
```

##### Read Only Mode
The pgstac.readonly setting can be used when using pgstac with a read replica.
Note that when pgstac.readonly is set to TRUE that pgstac is unable to use a cache for calculating the total count for context which can make use of the context extension very expensive (see notes above). In readonly mode, pgstac is also unable to register the hash that is used to store queries that can be used with geometry_search (used by titiler-pgstac). A registered has will still be readable, but new hashes cannot be created on the read only replica, they must be registered on the main database.

#### Runtime Configurations

Runtime configuration of variables can be made with search by passing in configuration in the search json "conf" item.

Runtime configuration is available for **context**, **context_estimated_count**, **context_estimated_cost**, **context_stats_ttl**, and **nohydrate**.

The nohydrate conf item returns an unhydrated item bypassing the CPU intensive step of rehydrating data with data from the collection metadata. When using the nohydrate conf, the only fields that are respected in the fields extension are geometry and bbox.
```sql
SELECT search('{"conf":{"nohydrate"=true}}');
```

#### PgSTAC Partitioning
By default PgSTAC partitions data by collection (note: this is a change starting with version 0.5.0). Each collection can further be partitioned by either year or month. **Partitioning must be set up prior to loading any data!** Partitioning can be configured by setting the partition_trunc flag on a collection in the database.
```sql
UPDATE collections set partition_trunc='month' WHERE id='<collection id>';
```

In general, you should aim to keep each partition less than a few hundred thousand rows. Further partitioning (ie setting everything to 'month' when not needed to keep the partitions below a few hundred thousand rows) can be detrimental.

#### PgSTAC Indexes / Queryables

By default, PgSTAC includes indexes on the id, datetime, collection, and geometry. Further indexing can be added for additional properties globally or only on particular collections by modifications to the queryables table.

The `queryables` table controls the indexes that PgSTAC will build as well as the metadata that is returned from a [STAC Queryables endpoint](https://github.com/stac-api-extensions/filter#queryables).

| Column                | Description                                                              | Type       | Example                                                                                                            |
|-----------------------|--------------------------------------------------------------------------|------------|--------------------------------------------------------------------------------------------------------------------|
| `id`                  | The id of the queryable                                                  | bigint[pk] | -                                                                                                                  |
| `name`                | The name of the property                                                 | text       | `eo:cloud_cover`                                                                                                   |
| `collection_ids`      | The collection ids that this queryable applies to                        | text[]     | `{sentinel-2-l2a,landsat-c2-l2,aster-l1t}` or `NULL`                                                               |
| `definition`          | The queryable definition of the property                                 | jsonb      | `{"title": "Cloud Cover", "type": "number", "minimum": 0, "maximum": 100}`                                         |
| `property_wrapper`    | The wrapper function to use to convert the property to a searchable type | text       | One of `to_int`, `to_float`, `to_tstz`, `to_text` or `NULL`                                                        |
| `property_index_type` | The index type to use for the property                                   | text       | `BTREE`, `NULL` or other valid [PostgreSQL index type](https://www.postgresql.org/docs/current/indexes-types.html) |

Each record in the queryables table references a single property but can apply to any number of collections. If the `collection_ids` field is left as NULL, then that queryable will apply to all collections. There are constraints that allow only a single queryable record to be active per collection. If there is a queryable already set for a property field with collection_ids set to NULL, you will not be able to create a separate queryable entry that applies to that property with a specific collection as pgstac would not then be able to determine which queryable entry to use.

By default, any property may be used in filter expressions. If you wish to restrict it and only allow the queryables, you should either set the additional_properties setting variable to False or make the corresponding adjustment in the pgstac_settings table.

##### Queryable Metadata

When used with [stac-fastapi](https://stac-utils.github.io/stac-fastapi/), the metadata returned in the queryables endpoint is determined using the definition field on the `queryables` table. This is a jsonb field that will be returned as-is in the queryables response. The full queryable response for a collection will be determined by all the `queryables` records that have a match in `collection_ids` or have a NULL `collection_ids`.

If two or more collections in your catalog share a property name, but have different definitions (e.g., `platform` with different enum values), be sure to repeat the property for each collection id, each with a unique `definition`.

There is a utility SQL function that can be used to help populate the `queryables` table by looking at a sample of data for each collection. This utility can also look to the json schema for STAC extensions defined in the `stac_extensions` table.

The `stac_extensions` table contains a `url` field and a `content` field for each extension that should be introspected to compare for fields. This can either be filled in manually or by using the `pypgstac loadextensions` command included with pypgstac. This command will look at the `stac_extensions` attribute in all collections to populate the `stac_extensions` table, fetching the json content of each extension. If any urls were added manually to the stac_extensions table, it will also populate any records where the content is NULL.

Once the `stac_extensions` table has been filled in, you can run the `missing_queryables` function either for a single collection:

```sql
SELECT * FROM missing_queryables('mycollection', 5);
```

or for all collections:

```sql
SELECT * FROM missing_queryables(5);
```

The numeric argument is the approximate percent of items that should be sampled to look for fields to include. This function will look for fields in the properties of items that do not already exist in the queryables table for each collection. It will then look to see if there is a field in any definition in the stac_extensions table to populate the definition for the queryable. If no definition was found, it will use the data type of the values for that field in the sample of items to fill in a generic definition with just the field type.

In order to populate the queryables table, you can then run the following query. Note we're casting the collection id to a text array:

```sql
INSERT INTO queryables (collection_ids, name, definition, property_wrapper)
    SELECT array[collection]::text[] as collection_ids, name, definition, property_wrapper
        FROM missing_queryables('mycollection', 5)
```

If you run into conflicts due to the unique constraints on collection/name, you may need to create a temp table, make any changes to remove the conflicts, and then INSERT.

```sql
CREATE TEMP TABLE draft_queryables AS SELECT * FROM missing_queryables(5);
```

Make any edits to that table or the existing queryables, then:

```sql
INSERT INTO queryables (collection_ids, name, definition, property_wrapper) SELECT * FROM draft_queryables;
```

##### Indexing

The `queryables` table is also used to specify which item `properties` attributes to add indexes on.

To add a new global index across all collection partitions:

```sql
INSERT INTO pgstac.queryables (name, property_wrapper, property_index_type)
VALUES (<property name>, <property wrapper>, <index type>);
```

Property wrapper should be one of `to_int`, `to_float`, `to_tstz`, or `to_text`. The index type should almost always be `BTREE`, but can be any PostgreSQL index type valid for the data type.

**More indexes is not necessarily better.** You should only index the primary fields that are actively being used to search. Adding too many indexes can be very detrimental to performance and ingest speed. If your primary use case is delivering items sorted by datetime and you do not use the context extension, you likely will not need any further indexes.

Leave `property_index_type` set to NULL if you do not want an index set for a property.

### Maintenance Procedures

These are procedures that should be run periodically to make sure that statistics and constraints are kept up-to-date and validated. These can be made to run regularly using the pg_cron extension if available.
```sql
SELECT cron.schedule('0 * * * *', 'CALL validate_constraints();');
SELECT cron.schedule('10, * * * *', 'CALL analyze_items();');
```
