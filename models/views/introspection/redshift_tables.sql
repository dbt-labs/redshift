
select
    n.nspname AS schemaname,
    c.relname AS tablename,
    d.description,
    case
        when c.relkind = 'v' then 'view'
        when c.relkind = 'r' then 'table'
        else null
    end as relation_type

from pg_catalog.pg_namespace n
join pg_catalog.pg_class c on n.oid = c.relnamespace
left outer join pg_description d ON (d.objoid = c.oid AND d.objsubid = 0)

where schemaname not like 'pg_%'
  and schemaname != 'information_schema'
  and relkind in ('v', 'r')
