
{#

    Spec: http://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html

    VACUUM [ FULL | SORT ONLY | DELETE ONLY | REINDEX ]
    [ [ table_name ] [ TO threshold PERCENT ] ]

#}

{% macro vacuum_all(type, to) -%}

    {%- if type == 'reindex' -%}
        {{ exceptions.raise_compiler_error("vacuum reindex called without a `table_ref`") }}
    {%- endif -%}

    vacuum {{ type }}

{%- endmacro %}


{# -- vacuum reindex does not work with 'to threshold percent' syntax #}
{% macro vacuum_reindex(table_ref) -%}

    vacuum reindex {{ table_ref }}

{%- endmacro %}


{% macro vacuum_table(table_ref, type, to) -%}

    vacuum {{ type }} {{ table_ref }} to {{ to }} percent

{%- endmacro %}


{#
    table_ref: ref, or schema.table qualified name of table
    type: full | sort only | delete only | reindex
    to: threshold for the vacuum command. In range [0, 100]
#}
{% macro vacuum(table_ref=none, type='full', to=95) -%}

    {% if table_ref is none -%}
        {% set cmd = vacuum_all(type, to) %}
    {%- elif type == 'reindex' -%}
        {% set cmd = vacuum_reindex(table_ref) %}
    {%- else -%}
        {% set cmd = vacuum_table(table_ref, type, to) %}
    {%- endif -%}

    {{ cmd }}

{%- endmacro %}
