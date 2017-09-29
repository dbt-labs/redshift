<p align="center">
  <img src="etc/dbt-logo.png" alt="dbt logo" />
</p>

----

## Redshift data models
Current Version: 0.0.1

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
