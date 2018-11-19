with tables as (

  select * from {{ref('pg_tables')}}

), views as (

  select * from {{ref('pg_views')}}

), users as (

  select * from {{ref('pg_user')}}
  
), schemas as (
  
  select
  distinct(schema_name)
  from tables
  where schema_name not in ('pg_catalog', 'information_schema')
        
  union
        
  select
  distinct(schema_name)
  from views
        
  where schema_name not in ('pg_catalog', 'information_schema')
  
)


select 
  schemas.schema_name
, users.username
, has_schema_privilege(users.username, schemas.schema_name, 'usage') AS has_usage_privilege
, has_schema_privilege(users.username, schemas.schema_name, 'create') AS has_create_privilege
from schemas
cross join users
order by schemas.schema_name, users.username
