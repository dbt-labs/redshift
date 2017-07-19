
{% materialization compressed_table, default %}
  {%- set identifier = model['name'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {%- set existing = adapter.query_for_existing(schema) -%}
  {%- set existing_type = existing.get(identifier) -%}
  {%- set tmp_analyze_identifier = "analyze_compression_" + tmp_identifier -%}

  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}

  {% statement main %}
      create temp table {{ tmp_analyze_identifier }} as (

          with all_data as (

              {{ sql }}

          )

          select *
          from all_data
          order by random()
          limit 10000

      )
  {% endstatement %}

  {% statement compression_recommendation %}

    analyze compression {{ tmp_analyze_identifier }}

  {% endstatement %}


  {% set compression_recommendation = load_result('compression_recommendation') %}

  {% statement %}

      {% set recs = compression_recommendation['data'] | sort(attribute='Column') %}
      {% set cols = adapter.get_columns_in_table(None, tmp_analyze_identifier) | sort(attribute='column') %}
      {% set num_cols = cols | length %}

      {% set dest_cols_csv = cols | map(attribute='quoted') | join(', ') %}

      create table "{{ schema }}"."{{ tmp_identifier }}" (

        {% for i in range(num_cols) %}
          {{ cols[i].name }} {{ cols[i].dtype }} encode {{ recs[i]['Encoding'] }} {% if not loop.last %},{% endif %}
        {% endfor %}

      ) {{ dist(_dist) }} {{ sort(_sort_type, _sort) }};

      insert into "{{ schema }}"."{{ tmp_identifier }}" ({{ dest_cols_csv }})
      with all_data as (
          {{ sql }}
      )
      select {{ dest_cols_csv }} from all_data

  {% endstatement %}

  {{ adapter.commit() }}


{% endmaterialization %}
