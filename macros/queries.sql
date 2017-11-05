
{#
    -- These macros should be models, but limitations in the 0.9.0 implementation
    -- of `ref` make this infeasible. TODO - move this logic directly into models
#}

{% macro fetch_table_data_sql() %}

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

{% endmacro %}

{% macro fetch_column_data_sql() %}

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

            case
                when format_encoding((a.attencodingtype)::integer) = 'none' then 'raw'
                else format_encoding((a.attencodingtype)::integer)
            end as col_encoding,

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

{% endmacro %}

{% macro fetch_constraint_data_sql() %}

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

{% endmacro %}



{% macro fetch_sort_dist_key_data_sql() %}

    with dist_config as (

        -- gets distyle and distkey (if there is one)
        select distinct
            trim(n.nspname) as schemaname,
            trim(c.relname) as tablename,

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
            trim(n.nspname) as schemaname,
            trim(c.relname) as tablename,
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

{% endmacro %}
