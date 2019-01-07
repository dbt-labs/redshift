{%- macro try_cast(str, datatype) -%}

{% if not datatype %}
{{ exceptions.raise_compiler_error(
        "Missing datatype for try_cast macro") }}

{%- elif datatype in ['bigint', 'int'] -%}
    {#-
    Matches:
    * 1
    * -1
    Non-matches:
    * 1.1
    * -1.1
    -#}

    case
        when trim({{str}}) ~ '^(\-){0,1}[0-9]+$' then trim({{str}})
        else null
    end::{{datatype}}

{% elif datatype in ['bool', 'boolean'] %}

    case
        when lower(trim({{str}})) in ('t', 'true') then true
        when lower(trim({{str}})) in ('f', 'false') then false
    end::{{datatype}}

{% elif datatype in ['float', 'decimal'] %}
    {#-
    Matches:
    * 12
    * -12
    * 12.2
    * -12.2
    Non-matches:
    * 0.12.2
    -#}
    case
        when trim({{str}}) ~ '^(\-){0,1}[0-9]+(\.[0-9]+){0,1}$'
            then trim({{str}})
    end::{{datatype}}

{% else %}

    {{ exceptions.raise_compiler_error(
            "this datatype is not currently supported") }}

{% endif %}

{%- endmacro -%}