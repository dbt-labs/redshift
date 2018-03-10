with tables as (

  select * from {{ref('pg_tables')}}

), views as (

  select * from {{ref('pg_views')}}

), users as (

  select * from {{ref('pg_user')}}

), objects as (
  
  select
    schema_name
  , 'table' as object_type
  , table_name as object_name
  , '"' || schema_name || '"."' || table_name || '"' as full_object_name
  from tables
  where schema_name not in ('pg_catalog', 'information_schema')
  
  union
  
  select
    schema_name
  , 'view' as object_type
  , view_name as object_name
  , '"' || schema_name || '"."' || view_name || '"' as full_object_name
  from views
  where schema_name not in ('pg_catalog', 'information_schema')
  
)

select 
  objects.schema_name
, objects.object_name
, users.username
, has_table_privilege(users.username, objects.full_object_name, 'select') as has_select_privilege
, has_table_privilege(users.username, objects.full_object_name, 'insert') as has_insert_privilege
, has_table_privilege(users.username, objects.full_object_name, 'update') as has_update_privilege
, has_table_privilege(users.username, objects.full_object_name, 'delete') as has_delete_privilege
, has_table_privilege(users.username, objects.full_object_name, 'references') as has_references_privilege
from objects
cross join users
order by objects.full_object_name, users.username
