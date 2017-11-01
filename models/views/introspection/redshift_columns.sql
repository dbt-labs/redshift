
with columns as (
    select
        n.nspname as schemaname,
        c.relname as tablename,
        a.attnum as col_index,
        a.attname as col_name,
        d.description,

        case
            when strpos(upper(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
            then replace(upper(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')

            when strpos(upper(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
            then replace(upper(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')

            else upper(format_type(a.atttypid, a.atttypmod))
        end as col_datatype,

        format_encoding((a.attencodingtype)::integer) as col_encoding,

        case
            when a.atthasdef is true then adef.adsrc
            else null
        end as col_default,

        a.attnotnull as col_not_null

    from pg_namespace as n
    inner join pg_class as c on n.oid = c.relnamespace
    inner join pg_attribute as a on c.oid = a.attrelid
    left outer join pg_description as d ON (d.objoid = a.attrelid AND d.objsubid = a.attnum)
    left outer join pg_attrdef as adef on a.attrelid = adef.adrelid and a.attnum = adef.adnum
    where c.relkind = 'r'
      and a.attnum > 0

)
select *
from columns
