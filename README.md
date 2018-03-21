# Redshift data models and utilities

[dbt](https://www.getdbt.com) models for [Redshift](https://aws.amazon.com/redshift/) warehouses.

## Models

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

- [queries](models/views/queries.sql): Simplified view of queries, including explain cost, execution times, and queue times.
- [table_stats](models/views/table_stats.sql): Gives insight on tables in your warehouse. Includes information on sort and dist keys, table size on disk, and more.

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
                manifest=Boolean,
                delimiter=String,
                null_as=String,
                max_file_size=String,
                escape=Boolean,
                compression=None|GZIP|BZIP2,
                add_quotes=Boolean,
                encrypted=Boolean,
                overwrite=Boolean,
                parallel=Boolean) }}
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
                                delimiter='|') }}"
    ]
  })
}}
```

#### analyze ([source](macros/analyze.sql))
Use this macro to analyze tables. The arguments to this macro map to the arguments of the Redshift [analyze](http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html) command. This macro should be used in `on-run-start`, `on-run-end`, `pre-hook`, or `post-hook` configs.

__Arguments:__
 - `table_ref`: `ref`, or `<schema>.<table>` qualified name of the table to analyze. If not provided, all tables in the database will be analyzed.
 - `columns`: list of columns to analyze. If provided, `table_ref` must also be provided (default: none)
 - `column_spec`: One of: `predicate columns` | `all columns` (default: `all columns`)
 - `analyze_threshold_percent`: sets `analyze_threshold_percent` before running `analyze` (default: 10)
 
 __Usage:__
```sql
-- models/my_model.sql
{{
  config({
    'materialized':'incremental',
    'sql_where': 'TRUE',
    'sort':'id',
    'post-hook': [
      analyze(this.final_name())
    ]
  })
}}
    
select 1 as id  

```
 
__Detailed Usage:__
```sql
-- Analyze all tables in the database
{{ analyze() }}

-- Analyze the public.raw_data table
{{ analyze('public.raw_data') }}

-- Analyze {{ this }} model
{{ analyze(this.final_name()) }}

-- Analyze the "id" column of {{ this }} model
{{ analyze(this.final_name(), ['id']) }}

-- Analyze the predicate columns of {{ this }} model
{{ analyze(this.final_name(), column_spec='predicate columns') }}

-- Analyze the predicate columns of {{ this }} model. Skip if "stats off" is less than 5%
{{ analyze(this.final_name(), column_spec='predicate columns', analyze_threshold_percent=5) }}

-- Analyze the predicate columns of all tables in the database. Skip if "stats off" is less than 20%
{{ analyze(column_spec='predicate columns', analyze_threshold_percent=20) }}
```

#### vacuum ([source](macros/vacuum.sql))
Use this macro to vacuum tables. The arguments to this macro map to the arguments of the Redshift [vacuum](http://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html) command. This macro should be used in `on-run-start`, `on-run-end`, `pre-hook`, or `post-hook` configs. Note that `vacuum` cannot run inside of a transaction. To indicate to dbt that `vacuum` hooks should run outside of the model transaction, use `after_commit()` as shown below.

__Arguments:__
 - `table_ref`: `ref`, or `<schema>.<table>` qualified name of the table to vacuum. If not provided, all tables in the database will be vacuumed.
 - `type`: One of: `full` | `sort only` | `delete only` | `reindex` (default: `full`)
 - `to`: An integer threshold for the vacuum command between 0 and 100 (default: 95)

__Usage:__
```sql
-- models/my_model.sql
{{
  config({
    'materialized':'incremental',
    'sql_where': 'TRUE',
    'sort':'id',
    'post-hook': [
      after_commit(vacuum(this.final_name(), type='full', to=95))
    ]
  })
}}
    
select 1 as id  

```

__Detailed Usage:__
```sql
-- Vacuum all tables in the database
{{ after_commit(vacuum()) }}

-- Vacuum the public.raw_data table
{{ after_commit(vacuum('public.raw_data)) }}

-- Vacuum {{ this }} model with in 'sort only' mode
{{ after_commit(vacuum(this.final_name(), 'sort only')) }}

-- Vacuum {{ this }} model with in 'delete only' mode
{{ after_commit(vacuum(this.final_name(), 'delete only')) }}

-- Vacuum reindex {{ this }} model. Only applicable for tables with interleaved sort keys
{{ after_commit(vacuum(this.final_name(), 'reindex')) }}

-- Vacuum all tables in the database in 'full' mode, the default
{{ after_commit(vacuum(type='full')) }}

-- Vacuum all tables in the database in 'full' mode. Skip tables that are less than 1% unsorted
{{ after_commit(vacuum(type='full', to=99)) }}

-- Vacuum all tables in the database in 'sort only' mode. Skip tables that are less than 1% unsorted
{{ after_commit(vacuum(type='sort only', to=99)) }}

-- Vacuum {{ this }} model in 'reindex' mode. Skip if the table is less than 50% unsorted
{{ after_commit(vacuum(this.final_name(), type='reindex', to=50)) }}

-- Vacuum {{ this }} model in 'sort only' mode. Skip if the table is less than 25% unsorted
{{ after_commit(vacuum(this.final_name(), type='sort only', to=75)) }}

```
