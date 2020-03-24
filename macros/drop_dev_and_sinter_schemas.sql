{% macro drop_dev_schemas(schema_ilike = ['dbt%', 'sinter%', 'dev%'], dry_run = true, exclude = '') %} 

{% set get_dev_sql %}

    select 
        distinct schemaname
    from pg_catalog.pg_tables 
    where 
    (
        (
        
        {% for schema in schema_ilike %} 

        schemaname ilike '{{schema}}' {{'or' if not loop.last}} 

        {% endfor %}
        
    )
        and 
        (
            
        
        {% for schema in schema_ilike %} 
        
        schemaname not ilike '{{ exclude }}' {{'or' if not loop.last}} 
        
        {% endfor %}
    
    )
        
    )
{% endset %}


{% set dev_schemas = dbt_utils.get_query_results_as_dict(get_dev_sql) %}

{% for schema_name in dev_schemas['schemaname'] %} 

    {% set drop_schema -%} 
    
        drop schema {{schema_name}} cascade;

    {%- endset %}
    
    {% if dry_run %} 
    
        {{ log(drop_schema, info=True) }} 

    {% else %}

        {% do run_query(drop_schema) %}

    {% endif %}

{% endfor %}

{% endmacro %}