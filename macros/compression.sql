{% macro find_analyze_recommendations(table, comprows=none) %}

  {% set comprows_s = '' if comprows is none else 'comprows ' ~ comprows %}
  {% call statement('compression_recommendation', fetch_result=True) %}
    analyze compression {{ table }} {{ comprows_s }}
  {% endcall %}
{% endmacro %}

{% macro find_existing_columns(schema, table) %}

  {% call statement('existing_columns', fetch_result=True) %}
    set search_path to {{ schema }};
    -- ignore the sort and dist keys -- we don't want to drop those
    select "column", "type", "encoding"
    from pg_table_def
    where tablename = '{{ table }}'
      and sortkey != 1
      and distkey = FALSE;
  {% endcall %}

{% endmacro %}


{% macro get_compression_sql(table, existing_cols, compression_recommendation) -%}

  {%- set recs = compression_recommendation['data'] | sort(attribute=1) -%}
  {%- set cols = existing_cols['data'] | sort(attribute=0) -%}

  {%- set updates = [] -%}
  {%- set drops = [] -%}
  {%- for i in range(recs | length) -%}

    {%- set column_name = cols[i][0] -%}
    {%- set column_type = cols[i][1] -%}
    {%- set old_column_encoding = cols[i][2] -%}
    {%- set new_column_encoding = recs[i][2] -%}
    {%- set estimated_reduction = recs[i][3] | float -%}

    {%- set old_column_name = column_name ~ "_old" -%}

    {%- if new_column_encoding != old_column_encoding and estimated_reduction > 0 %}

        -- Column: {{ column_name }}, Estimated reduction: {{ estimated_reduction }}
        alter table {{ table }} rename {{ column_name }} to {{ old_column_name }};
        alter table {{ table }} add column {{ column_name }} {{ column_type }} encode {{ new_column_encoding }};

        {%- set _ = updates.append(column_name ~ ' = ' ~ old_column_name) -%}
        {%- set _ = drops.append(old_column_name) %}

    {%- endif -%}
  {%- endfor -%}

  {%- if (updates | length) > 0 -%}
    update {{ table }} set
    {{ updates | join(",\n ") }};
  {% endif -%}

  {%- for drop in drops -%}
    alter table {{ table }} drop column {{ drop }} cascade;
  {% endfor -%}

{%- endmacro %}

{%- macro compress_table(table, comprows=none, column_override=none) -%}

  {% set _ = find_analyze_recommendations(table, comprows) %}
  {% set _ = find_existing_columns(table.schema, table.table) %}
  {%- set compression_recommendation = load_result('compression_recommendation') -%}
  {%- set existing_cols = load_result('existing_columns') -%}

  {% set query = get_compression_sql(table, existing_cols, compression_recommendation) %}

  {% if (query | trim | length) > 0 %} 
    {{ query }}
  {% else %}
    select 'nothing to do' as status
  {% endif %}

{%- endmacro %}
