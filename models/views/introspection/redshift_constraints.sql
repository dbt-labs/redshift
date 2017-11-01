
SELECT
    c.nspname as schemaname,
    b.relname as tablename,
    case
        when a.contype = 'p' then 'primary key'
        when a.contype = 'u' then 'unique'
        else null
    end as constraint_type,
    pg_get_constraintdef(a.oid) as col_constraint

FROM pg_catalog.pg_constraint a
JOIN pg_catalog.pg_class b ON(a.conrelid=b.oid)
JOIN pg_catalog.pg_namespace c ON(a.connamespace=c.oid)
WHERE a.contype in ('p', 'u')
