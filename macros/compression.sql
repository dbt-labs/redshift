{% macro find_analyze_recommendations(schema, table, comprows=none) %}

  {% set comprows_s = '' if comprows is none else 'comprows ' ~ comprows %}
  {% set query %}
    analyze compression "{{ schema }}"."{{ table }}" {{ comprows_s }}
  {% endset %}

  {% set columns = redshift.get_data(query, ['table', 'column', 'encoding', 'reduction_pct']) %}

  {% set ret = {} %}
  {% for column in columns %}
      {%- set _ = ret.update({column.column: column}) -%}
  {% endfor %}

  {{ return(ret) }}

{% endmacro %}

{% macro build_optimized_definition(definition, recommendation) -%}

    {% set optimized = {} %}
    {% set _ = optimized.update(definition) %}
    {% for name, column in definition['columns'].items() %}
        {% set recommended_encoding = recommendation[name] %}

        {% if recommended_encoding['encoding'] != column['encoding'] %}
            {{ log("    Changing " ~ name ~ ": " ~ column['encoding'] ~ " -> " ~ recommended_encoding['encoding'] ~ " (" ~ recommended_encoding['reduction_pct'] ~ "%)") }}
        {% else %}
            {{ log("Not Changing " ~ name ~ ": " ~ column['encoding']) }}
        {% endif %}

        {% set _ = optimized['columns'][name].update({"encoding": recommended_encoding['encoding']}) %}
    {% endfor %}

    {{ return(optimized) }}

{%- endmacro %}

{%- macro insert_into_sql(from_schema, from_table, to_schema, to_table) -%}

    insert into "{{ to_schema }}"."{{ to_table }}" (
        select * from "{{ from_schema }}"."{{ from_table }}"
    );

{%- endmacro -%}

{%- macro atomic_swap_sql(schema, from_table, to_table, drop_backup) -%}

    begin;
    -- drop table if exists "{{ schema }}"."{{ from_table }}__backup" cascade;
    alter table "{{ schema }}"."{{ from_table }}" rename to "{{ from_table }}__backup";
    alter table "{{ schema }}"."{{ to_table }}" rename to "{{ from_table }}";
    {% if drop_backup %}
        drop table "{{ schema }}"."{{ from_table }}__backup" cascade;
    {% else %}
        {{ log('drop_backup is False -- not dropping ' ~ from_table ~ "__backup") }}
    {% endif %}
    commit;

{%- endmacro -%}

{%- macro compress_table(schema, table, drop_backup=False,
                         comprows=none, sort_style=none, sort_keys=none,
                         dist_style=none, dist_key=none) -%}

  {% if not execute %}
    {{ return(none) }}
  {% endif %}

  {% set recommendation = redshift.find_analyze_recommendations(schema, table, comprows) %}
  {% set definition = redshift.fetch_table_definition(schema, table) %}

  {% if definition is none %}
    {{ return(none) }}
  {% endif %}

  {% set optimized = redshift.build_optimized_definition(definition, recommendation) %}

  {% set _ = optimized.update({"keys": optimized.get('keys', {}) | default({})}) %}
  {% if sort_style %} {% set _ = optimized['keys'].update({"sort_style": sort_style}) %} {% endif %}
  {% if sort_keys %}  {% set _ = optimized['keys'].update({"sort_keys": sort_keys}) %} {% endif %}
  {% if dist_style %} {% set _ = optimized['keys'].update({"dist_style": dist_style}) %} {% endif %}
  {% if dist_key %}   {% set _ = optimized['keys'].update({"dist_key": dist_key}) %} {% endif %}

  {% set new_table = table ~ "__compressed" %}
  {% set _ = optimized.update({'name': new_table}) %}

  {# Build the DDL #}
  {{ redshift.build_ddl_sql(optimized) }}
  {{ redshift.insert_into_sql(schema, table, schema, new_table) }}
  {{ redshift.atomic_swap_sql(schema, table, new_table, drop_backup) }}

{%- endmacro %}
