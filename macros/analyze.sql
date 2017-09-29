
{#

    Spec: http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html

    ANALYZE [ VERBOSE ]
    [ [ table_name [ ( column_name [, ...] ) ] ]
    [ PREDICATE COLUMNS | ALL  COLUMNS ]

#}

{% macro get_column_spec(column_spec) -%}

    {{ 'all columns' if column_spec is none else column_spec }}

{%- endmacro %}


{% macro get_column_list(column_spec) -%}

    ({{ column_spec | join(",") }})

{%- endmacro %}


{% macro analyze_all(column_spec) -%}

    analyze verbose {{ get_column_spec(column_spec) }}

{%- endmacro %}


{% macro analyze_table(table_ref, columns, column_spec) -%}

    {%- set col_spec = get_column_spec(column_spec) if columns is none else '' -%}
    {%- set col_list = get_column_list(columns) if columns is not none else '' -%}

    analyze verbose {{ table_ref }} {{ col_list }} {{ col_spec }}

{%- endmacro %}


{#
    table_ref: ref, or schema.table qualified name of table
    columns: list of columns to analyze (default: none)
    column_spec: predicate columns | all columns
    analyze_threshold_percent: sets the "analyze_threshold_percent" variable before running analyze (default: 10)
#}
{% macro analyze(table_ref=none, columns=none, column_spec=none, analyze_threshold_percent=none) -%}

    {% if analyze_threshold_percent is not none -%}
        set analyze_threshold_percent to {{ analyze_threshold_percent }} {{ ";\n" }}
    {%- endif -%}

    {%- if table_ref is none -%}
        {% set cmd = analyze_all(column_spec) %}
    {%- else -%}
        {% set cmd = analyze_table(table_ref, columns, column_spec) %}
    {%- endif -%}

    {{ cmd }}

{%- endmacro %}
