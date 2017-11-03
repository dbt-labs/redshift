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

-- unload a table to S3 (see: http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)
{% macro unload_table(table,
                      s3_path,
                      aws_key,
                      aws_secret,
                      delimiter=',',
                      compression=None,
                      escape=True,
                      overwrite=True,
                      parallel=False,
                      encrypted=False,
                      addquotes=False,
                      null_as='',
                      max_file_size='6 GB') %}

  UNLOAD ('SELECT * FROM {{ table }}')
  TO '{{ s3_path }}'
  ACCESS_KEY_ID '{{ aws_key }}'
  SECRET_ACCESS_KEY '{{ aws_secret }}'
  DELIMITER AS '{{ delimiter }}'
  NULL AS '{{ null_as }}'
  MAXFILESIZE AS {{max_file_size}}
  {% if escape %}
  ESCAPE
  {% endif %}
  {% if compression %}
  {{compression|upper}}
  {% endif %}
  {% if addquotes %}
  ADDQUOTES
  {% endif %}
  {% if encrypted %}
  ENCRYPTED
  {% endif %}
  {% if overwrite %}
  ALLOWOVERWRITE
  {% endif %}
  {% if not parallel %}
  PARALLEL OFF
  {% endif %}
  ;
{% endmacro %}