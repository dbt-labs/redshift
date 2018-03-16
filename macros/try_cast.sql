{%- macro try_cast_bigint(str) -%}

case
    when trim({{str}}) ~ '^[0-9]+$' then trim({{str}})
    else null
end::bigint as amount

{%- endmacro -%}
