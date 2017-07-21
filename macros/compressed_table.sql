{% macro create_and_analyze(identifier, sql, comprows=none) %}

  {% call statement('main') %}
      {{ create_table_as(temporary=True, identifier=identifier, sql=sql) }}
  {% endcall %}

  -- run the analyze query
  {% set comprows_s = '' if comprows is none else 'comprows ' ~ comprows %}

  {% call statement('compression_recommendation', fetch_result=True) %}
    analyze compression {{ identifier }} {{ comprows_s }}
  {% endcall %}

{% endmacro %}


{% macro create_compressed_table(analyzed_table, identifier, compression_recommendation) %}

  {% set recs = compression_recommendation['data'] | sort(attribute='Column') %}
  {% set cols = adapter.get_columns_in_table(None, analyzed_table) | sort(attribute='column') %}

  {% set dest_cols_csv = cols | map(attribute='quoted') | join(', ') %}
  {% set num_cols = cols | length %}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get('sort_type', validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get('sort', validator=validation.any[list, basestring]) -%}

  create table "{{ schema }}"."{{ identifier }}" (

    {% for i in range(num_cols) %}
      {{ cols[i].name }} {{ cols[i].data_type }} encode {{ recs[i]['Encoding'] | default('zstd') }} {% if not loop.last %},{% endif %}
    {% endfor %}

  ) {{ dist(_dist) }} {{ sort(_sort_type, _sort) }};

  insert into "{{ schema }}"."{{ identifier }}" ({{ dest_cols_csv }})
  select {{ dest_cols_csv }} from {{ analyzed_table }}

{% endmacro %}

{% materialization compressed_table, default %}

  {%- set identifier = model['name'] %}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}
  {%- set tmp_analyze_identifier = "analyze_compression_" + tmp_identifier -%}
  {%- set comprows = config.get('comprows', default=None) -%}

  {{ run_hooks(pre_hooks) }}

  -- setup
  {% if existing_type in [none, 'view'] or full_refresh_mode -%}

      {% set should_create_table = True %}

  {% elif existing_type == 'table' %}

      {{ adapter.truncate(identifier) }}
      {% set should_create_table = False %}

  {%- endif %}

  {% if should_create_table %}

      {{ create_and_analyze(tmp_analyze_identifier, sql, comprows) }}

      {% set compression_recommendation = load_result('compression_recommendation') %}

      {% call statement('main') %}
        {{ create_compressed_table(tmp_analyze_identifier, tmp_identifier, compression_recommendation) }}
      {% endcall %}

     -- {{ adapter.drop(identifier, existing_type) }}
     {{ adapter.rename(tmp_identifier, identifier) }}

  {% else %}

      {% set cols = adapter.get_columns_in_table(schema, identifier) %}
      {% set dest_cols_csv = cols | map(attribute='quoted') | join(', ') %}

      {% call statement() %}
          {{ create_table_as(temporary=True, identifier=tmp_analyze_identifier, sql=sql) }}
      {% endcall %}

      {{ adapter.expand_target_column_types(temp_table=tmp_analyze_identifier, to_schema=schema, to_table=identifier) }}

      {% call statement('main') %}
        insert into "{{ schema }}"."{{ identifier }}" ({{ dest_cols_csv }})
        select {{ dest_cols_csv }} from {{ tmp_analyze_identifier }}
      {% endcall %}

      {{ truncate + insert }}

  {% endif %}

  {%- if should_create_table and existing_type in ['table', 'view'] -%}


  {% elif should_create_table %}


  {% endif %}


  {{ run_hooks(post_hooks) }}

  {{ adapter.commit() }}

{% endmaterialization %}
