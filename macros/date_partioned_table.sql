
{% macro get_periods(period, date_field, source) %}

    with periods as (

        select distinct
            date_trunc('{{ period }}', {{ date_field }}) as period

        from {{ source }}

    )

    select period from periods order by period


{% endmacro %}

{% macro sql_for_date(date, date_field, sql) %}

    with data as (

        {{ sql }}

    )

    select *
    from data
    where {{ date_field }} = '{{ date }}'

{% endmacro %}

{% macro sql_for_unioned_table(schema, identifier, dates) %}

    {% for date in dates %}

        select * from {{ adapter.quote(identifier ~ '_' ~ date.isoformat()) }}

        {% if not loop.last %} union all {% endif %}

    {% endfor %}

{% endmacro %}

{% materialization date_partitioned_table, default %}

    {%- set identifier = model['name'] %}
    {%- set period = config.get('period') -%}
    {%- set date_field = config.get('date_field') -%}
    {%- set source = config.get('source') -%}

    {% call statement('periods', fetch_result=True) %}
        {{ get_periods(period, date_field, source) }}
    {% endcall %}

    {% set dates = load_result('periods')['data'] | map(attribute='period') | list %}

    {% for date in dates %}

        {% call statement() %}

            {% set period_identifier = identifier ~ '_' ~ date.isoformat() %}
            {% set create_sql = sql_for_date(date, date_field, sql) %}

            {{ create_table_as(True, period_identifier, create_sql) }}

        {% endcall %}

    {% endfor %}

    {% call statement('main') %}

        {% set unioned_sql = sql_for_unioned_table(schema, identifier, dates) %}
        {{ create_table_as(False, identifier, unioned_sql) }}

    {% endcall %}

    {{ adapter.commit() }}

{% endmaterialization %}




