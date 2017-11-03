
select
    c.nspname as schemaname,
    b.relname as tablename,
    case
        when a.contype = 'p' then 'primary key'
        when a.contype = 'u' then 'unique'
        when a.contype = 'f' then 'foreign key'
        else null
    end as constraint_type,
    pg_get_constraintdef(a.oid) as col_constraint

from pg_catalog.pg_constraint a
join pg_catalog.pg_class b on(a.conrelid=b.oid)
join pg_catalog.pg_namespace c on(a.connamespace=c.oid)
where a.contype in ('p', 'u', 'f')
