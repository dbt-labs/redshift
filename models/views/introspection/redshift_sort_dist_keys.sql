
with dist_config as (

    -- gets distyle and distkey (if there is one)
    select distinct
        n.nspname as schemaname,
        c.relname as tablename,

        case
            when c.reldiststyle = 0 then 'even'
            when c.reldiststyle = 1 then 'key'
            when c.reldiststyle = 8 then 'all'
            else null
        end as diststyle,

        max(case when c.reldiststyle = 1 and a.attisdistkey IS TRUE and a.attnum > 0 then a.attname else null end) over (partition by n.nspname, c.relname) as dist_key

    from pg_namespace as n
    inner join pg_class as c on n.oid = c.relnamespace
    inner join pg_attribute as a on c.oid = a.attrelid
    where c.relkind = 'r'

),

sort_config as (

    -- get sortstyle and sortkeys
    select distinct
        n.nspname as schemaname,
        c.relname as tablename,
        case
            when min(a.attsortkeyord) over (partition by n.nspname, c.relname) = -1 then 'interleaved'
            else 'compound'
        end as sort_style,
        listagg(a.attname, '|') within group (order by a.attsortkeyord) over (partition by n.nspname, c.relname) as sort_keys

    from  pg_namespace as n
    inner join pg_class as c on n.oid = c.relnamespace
    inner join pg_attribute as a on c.oid = a.attrelid
    where c.relkind = 'r'
      and abs(a.attsortkeyord) > 0
      and a.attnum > 0
)

select *
from sort_config
join dist_config using (schemaname, tablename)
