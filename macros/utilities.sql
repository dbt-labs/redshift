-- decodes pg_class.reldiststyle into 'even', 'all', or the distkey
{% macro decode_reldiststyle(diststyle_field, distkey_field) -%}
  decode({{diststyle_field}}, 0, 'even',
                              1, {{distkey_field}},
                              'all')
{%- endmacro %}


-- take percentage (including type casting)
{% macro percentage(num, denom) -%}
  (case {{denom}}
        when 0 then 0
        else ({{num}}::float / {{denom}}::float) * 100.0 end)
{%- endmacro %}
