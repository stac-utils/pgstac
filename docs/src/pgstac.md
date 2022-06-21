
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
