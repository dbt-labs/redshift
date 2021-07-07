# Redshift data models and utilities

[dbt](https://www.getdbt.com) models for [Redshift](https://aws.amazon.com/redshift/) warehouses.

## Installation instructions

1. Include this package in your `packages.yml` -- check [here](https://hub.getdbt.com/dbt-labs/redshift/latest/)
for installation instructions.
2. Run `dbt deps`

### Models

This package provides a number of base models for Redshift system tables, as well as a few utility views that usefully combine the base models.

__Base Models__

Each of these base models maps 1-to-1 with the underlying Redshift table. Some renaming has been performed to make the field names grokable.

- pg_attribute
- pg_class
- pg_namespace
- pg_user
- [stl_explain](http://docs.aws.amazon.com/redshift/latest/dg/r_STL_EXPLAIN.html)
- [stl_query](http://docs.aws.amazon.com/redshift/latest/dg/r_STL_QUERY.html)
- [stl_wlm_query](http://docs.aws.amazon.com/redshift/latest/dg/r_STL_WLM_QUERY.html)
- [stv_blocklist](http://docs.aws.amazon.com/redshift/latest/dg/r_STV_BLOCKLIST.html)
- [stv_tbl_perm](http://docs.aws.amazon.com/redshift/latest/dg/r_STV_TBL_PERM.html)
- [svv_diskusage](http://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DISKUSAGE.html)

__Ephemeral Models__

These ephemeral models simplify some of Redshift's field naming and logic, to make the data more usable.

- redshift_cost: transforms the start and max explain cost values from stl_explain into floating point values

__View Models__

These views are designed to make debugging your Redshift cluster more straightforward. They are, in effect, materializations of the [Diagnostic Queries for Query Tuning](http://docs.aws.amazon.com/redshift/latest/dg/diagnostic-queries-for-query-tuning.html) from Redshift's documentation.

- [redshift_admin_queries](models/views/redshift_admin_queries.sql): Simplified view of queries, including explain cost, execution times, and queue times.
- [redshift_admin_table_stats](models/views/redshift_admin_table_stats.sql): Gives insight on tables in your warehouse. Includes information on sort and dist keys, table size on disk, and more.
- [redshift_admin_dependencies](models/views/redshift_admin_dependencies.sql): Simplified view of pg_depend, showing any dependent objects (views) for a given source object

These views are designed to make user privilege management more straightforward.
- [users_table_view_privileges](models/views/users_table_view_privileges.sql): Gives insights into which [privileges](https://docs.aws.amazon.com/redshift/latest/dg/r_HAS_TABLE_PRIVILEGE.html) each user has on each table/view.
- [users_schema_privileges](models/views/users_schema_privileges.sql): Gives insights into which [privileges](https://docs.aws.amazon.com/redshift/latest/dg/r_HAS_SCHEMA_PRIVILEGE.html) each user has on each schema.

__Introspection Models__

These models (default ephemeral) make it possible to inspect tables, columns, constraints, and sort/dist keys of the Redshift cluster. These models are used to build column compression queries, but may also be generally useful.

- [redshift_tables](models/introspection/redshift_tables.sql)
- [redshift_columns](models/introspection/redshift_columns.sql)
- [redshift_constraints](models/introspection/redshift_constraints.sql)
- [redshift_sort_dist_keys](models/introspection/redshift_sort_dist_keys.sql)


## Macros

#### compress_table ([source](macros/compression.sql))

This macro returns the SQL required to auto-compress a table using the results of an `analyze compression` query. All comments, constraints, keys, and indexes are copied to the newly compressed table by this macro. Additionally, sort and dist keys can be provided to override the settings from the source table. By default, a backup table is made which is _not_ deleted. To delete this backup table after a successful copy, use `drop_backup` flag.

Macro signature:
```
{{ compress_table(schema, table,
                  drop_backup=False,
                  comprows=none|Integer,
                  sort_style=none|compound|interleaved,
                  sort_keys=none|List<String>,
                  dist_style=none|all|even,
                  dist_key=none|String) }}
```

Example usage:
```
{{
  config({
    "materialized":"table",
    "sort": "id",
    "dist": "id",
    "post-hook": [
      "{{ redshift.compress_table(this.schema, this.table, drop_backup=False) }}"
    ]
  })
}}
```

#### unload_table ([source](macros/unload.sql))

This macro returns the SQL required to unload a Redshift table to one or more files on S3. The macro replicates all functionality provided by Redshift's [UNLOAD](http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) command.

Macro signature:
```
{{ unload_table(schema,
                table,
                s3_path,
                iam_role=None|String,
                aws_key=None|String,
                aws_secret=None|String,
                aws_token=None|String,
                aws_region=None|String,
                manifest=Boolean,
                header=Boolean,
                format=None|String,
                delimiter=String,
                null_as=String,
                max_file_size=String,
                escape=Boolean,
                compression=None|GZIP|BZIP2,
                add_quotes=Boolean,
                encrypted=Boolean,
                overwrite=Boolean,
                parallel=Boolean,
                partition_by=none|List<String>
) }}
```

Example usage:
```
{{
  config({
    "materialized":"table",
    "sort": "id",
    "dist": "id",
    "post-hook": [
      "{{ redshift.unload_table(this.schema,
                                this.table,
                                s3_path='s3://bucket/folder',
                                aws_key='abcdef',
                                aws_secret='ghijklm',
                                header=True,
                                delimiter='|') }}"
    ]
  })
}}
```

#### redshift_maintenance_operation ([source](macros/redshift_maintenance_operation.sql))

This macro is intended to be run as an [operation](https://docs.getdbt.com/docs/using-operations). It vacuums and analyzes each table, with verbose logging.

The user who runs this operation must be a super user.
```
$ dbt run-operation redshift_maintenance
Running with dbt=0.14.2
06:35:33 + 1 of 478 Vacuuming "analytics"."customer_orders"
06:35:33 + 1 of 478 Analyzing "analytics"."customer_orders"
06:35:33 + 1 of 478 Finished "analytics"."customer_orders" in 0.29s
06:35:33 + 2 of 478 Vacuuming "analytics"."customer_payments"
06:35:33 + 2 of 478 Analyzing "analytics"."customer_payments"
06:35:33 + 2 of 478 Finished "analytics"."customer_payments" in 0.28s
```

The command can also be run with optional parameters to exclude schemas, either with exact or regex matching. This can be useful for cases where the models (or the schemas themselves) tend to be short-lived and don't require vacuuming. For example:
```
$ dbt run-operation redshift_maintenance --args '{exclude_schemas: ["looker_scratch"], exclude_schemas_like: ["sinter\\_pr\\_%"]}'
```
You can also implement your own query to choose which tables to vacuum. To do so,
create a macro in your **own project** named `vacuumable_tables_sql`, following
the same pattern as the macro in [this package](macros/redshift_maintenance_operation.sql).
Here's an example:
```sql
-- my_project/macros/redshift_maintenance_operation.sql
{% macro vacuumable_tables_sql() %}
{%- set limit=kwargs.get('limit') -%}
select
    current_database() as table_database,
    table_schema,
    table_name
from information_schema.tables
where table_type = 'BASE TABLE'

order by table_schema, table_name
{% if limit %}
limit ~ {{ limit }}
{% endif %}
{% endmacro %}
```
When you run the `redshift_maintenance` macro, your version of `vacuumable_tables_sql`
will be respected. You can also add arguments to your version of `vacuumable_tables_sql`
by following the pattern in the `vacuumable_tables_sql` macro in this package.

Note: This macro will skip any relations that are dropped in the time betwen running
the initial query, and the point at which you try to vacuum it. This results in
a message like so:
```
13:18:22 + 1 of 157 Skipping relation "analytics"."dbt_claire"."amazon_orders" as it does not exist
```

### Contributing
Additional contributions to this repo are very welcome! Check out [this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package. All PRs should only include functionality that is contained within all Redshift deployments; no implementation-specific details should be included.
