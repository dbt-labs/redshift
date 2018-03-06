with unsorted_by_table as (

  select
    db_id
  , id as table_id
  , name as table_name
  , sum(rows) as rows
  , sum(unsorted_rows) as unsorted_rows
  from {{ref('stv_tbl_perm')}}
  group by 1, 2, 3

), pg_class as (

  select * from {{ref('pg_class')}}

), pg_namespace as (

  select * from {{ref('pg_namespace')}}

), table_sizes as (

  select
    tbl as table_id
  , count(*) as size_in_megabytes
  from {{ref('stv_blocklist')}}
  group by 1

), table_attributes as (

  select
    attrelid as table_id
  , min(dist_key) as dist_key
  , min(sort_key) as sort_key
  , max(attsortkeyord) as num_sort_keys
  , (max(attencodingtype) > 0) as is_encoded
  , max(attnum) as num_columns
  from {{ref('pg_attribute')}}
  group by 1

), slice_distribution as (

  select
    tbl as table_id
  , trim(name) as name
  , slice
  , count(*) as size_in_megabytes

  from {{ref('svv_diskusage')}}
  group by 1, 2, 3

), capacity as (

  select
    sum(capacity) as total_megabytes
  from
  stv_partitions
  where part_begin=0

), table_distribution_ratio as (

  select
    table_id
  , (max(size_in_megabytes)::float / min(size_in_megabytes)::float)
      as ratio
  from slice_distribution
  group by 1

)

select

  trim(pg_namespace.nspname) as schema
, trim(unsorted_by_table.table_name) as table
, unsorted_by_table.rows
, unsorted_by_table.unsorted_rows
, {{percentage('unsorted_by_table.unsorted_rows',
               'unsorted_by_table.rows')}}
    as percent_rows_unsorted
, unsorted_by_table.table_id

, {{decode_reldiststyle('pg_class.reldiststyle',
                        'table_attributes.dist_key')}} as dist_style
, table_distribution_ratio.ratio as dist_skew

, (table_attributes.sort_key is not null) as is_sorted
, table_attributes.sort_key
, table_attributes.num_sort_keys
, table_attributes.num_columns

, table_sizes.size_in_megabytes
, {{percentage('table_sizes.size_in_megabytes',
               'capacity.total_megabytes')}}
    as disk_used_percent_of_total
, table_attributes.is_encoded

from unsorted_by_table

inner join pg_class
  on pg_class.oid = unsorted_by_table.table_id

inner join pg_namespace
  on pg_namespace.oid = pg_class.relnamespace

inner join capacity
  on 1=1

left join table_sizes
  on unsorted_by_table.table_id = table_sizes.table_id

inner join table_attributes
  on table_attributes.table_id = unsorted_by_table.table_id

inner join table_distribution_ratio
  on table_distribution_ratio.table_id = unsorted_by_table.table_id
