{% macro vacuumable_tables_sql(exclude_schemas, exclude_schemas_like) %}
    select
        current_database() as table_database,
        table_schema,
        table_name
    from information_schema.tables
    where table_type = 'BASE TABLE'
        and table_schema not in ('information_schema', 'pg_catalog')
        {% if exclude_schemas %}
        and table_schema not in ('{{exclude_schemas | join("', '")}}')
        {% endif %}
        {% for exclude_schema_like in exclude_schemas_like %}
        and table_schema not like '{{ exclude_schema_like }}'
        {% endfor %}
    order by table_schema, table_name
{% endmacro %}

{% macro get_vacuumable_tables(exclude_schemas, exclude_schemas_like) %}

    {% set vacuumable_tables=run_query(vacuumable_tables_sql()) %}
    {{ return(vacuumable_tables.columns[0].values()) }}

{% endmacro %}

{% macro redshift_maintenance(exclude_schemas=[], exclude_schemas_like=[]) %}

    {#-
    This logic means that if you add your own macro named `vacuumable_tables_sql`
    to your project, that will be used, giving you the flexibility of defining
    your own query.
    -#}
    {% if context.get(ref.config.project_name, {}).get('vacuumable_tables_sql')  %}
        {% set vacuumable_tables_sql=context[ref.config.project_name].vacuumable_tables_sql(exclude_schemas, exclude_schemas_like) %}
    {% else %}
        {% set vacuumable_tables_sql=redshift.vacuumable_tables_sql(exclude_schemas, exclude_schemas_like) %}
    {% endif %}

    {% set vacuumable_tables=run_query(vacuumable_tables_sql) %}

    {% for row in vacuumable_tables %}
        {% set message_prefix=loop.index ~ " of " ~ loop.length %}

        {%- set relation_to_vacuum = adapter.get_relation(
                                                database=row['table_database'],
                                                schema=row['table_schema'],
                                                identifier=row['table_name']
                                    ) -%}
        {% do run_query("commit") %}

        {% if relation_to_vacuum %}
            {% set start=modules.datetime.datetime.now() %}
            {{ dbt_utils.log_info(message_prefix ~ " Vacuuming " ~ relation_to_vacuum) }}
            {% do run_query("vacuum " ~ relation_to_vacuum) %}
            {{ dbt_utils.log_info(message_prefix ~ " Analyzing " ~ relation_to_vacuum) }}
            {% do run_query("analyze " ~ relation_to_vacuum) %}
            {% set end=modules.datetime.datetime.now() %}
            {% set total_seconds = (end - start).total_seconds() | round(2)  %}
            {{ dbt_utils.log_info(message_prefix ~ " Finished " ~ relation_to_vacuum ~ " in " ~ total_seconds ~ "s") }}
        {% else %}
            {{ dbt_utils.log_info(message_prefix ~ ' Relation "' ~ row.values() | join ('"."') ~ '" does not exist') }}
        {% endif %}

    {% endfor %}

{% endmacro %}
