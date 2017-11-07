<p align="center">
  <img src="etc/dbt-logo.png" alt="dbt logo" />
</p>

----

# Redshift data models and utilities

Current version: 0.0.1

[dbt](https://www.getdbt.com) models for [Redshift](https://aws.amazon.com/redshift/) warehouses.

### Models

This package provides a number of base models for Redshift system tables, as well as a few utility views that usefully combine the base models.

__Base Models__

Each of these base models maps 1-to-1 with the underlying Redshift table.

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
- users: transforms pg_users to make the field names grokable

__View Models__

These views are designed to make debugging your Redshift cluster more straightforward. They are, in effect, materializations of the [Diagnostic Queries for Query Tuning](http://docs.aws.amazon.com/redshift/latest/dg/diagnostic-queries-for-query-tuning.html) from Redshift's documentation.

- queries: Simplified view of queries, including explain cost, execution times, and queue times.
- table_stats: Gives insight on tables in your warehouse. Includes information on sort and dist keys, table size on disk, and more.

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
