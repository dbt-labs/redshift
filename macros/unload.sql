-- Redshift UNLOAD grammar (see: http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)
{#
UNLOAD ('select-statement')
TO 's3://object-path/name-prefix'
authorization
[ option [ ... ] ]

where option is
{ [ FORMAT [ AS ] ] CSV | PARQUET
| PARTITION BY ( column_name [, ... ] ) [ INCLUDE ]
| MANIFEST [ VERBOSE ] 
| HEADER           
| DELIMITER [ AS ] 'delimiter-char' 
| FIXEDWIDTH [ AS ] 'fixedwidth-spec'   
| ENCRYPTED [ AUTO ]
| BZIP2  
| GZIP 
| ZSTD
| ADDQUOTES 
| NULL [ AS ] 'null-string'
| ESCAPE
| ALLOWOVERWRITE
| PARALLEL [ { ON | TRUE } | { OFF | FALSE } ]
| MAXFILESIZE [AS] max-size [ MB | GB ] 
| REGION [AS] 'aws-region' }

#}
-- Unloads a Redshift table to S3
{% macro unload_table(schema,
                table,
                s3_path,
                iam_role=None,
                aws_key=None,
                aws_secret=None,
                aws_region=None,
                aws_token=None,
                manifest=False,
                header=False,
                format=None,
                delimiter=",",
                null_as="",
                max_file_size='6 GB',
                escape=True,
                compression=None,
                add_quotes=False,
                encrypted=False,
                overwrite=False,
                parallel=False,
                partition_by=None
                ) %}

  -- compile UNLOAD statement
  UNLOAD ('SELECT * FROM "{{ schema }}"."{{ table }}"')
  TO '{{ s3_path }}'
  {% if iam_role %}
  IAM_ROLE '{{ iam_role }}'
  {% elif aws_key and aws_secret %}
  ACCESS_KEY_ID '{{ aws_key }}'
  SECRET_ACCESS_KEY '{{ aws_secret }}'
  {% if aws_token %}
    SESSION_TOKEN '{{ aws_token }}'
  {% endif %}
  {% else %}
  -- Raise an error if authorization args are not present
  {{ exceptions.raise_compiler_error("You must provide AWS authorization parameters via 'iam_role' or 'aws_key' and 'aws_secret'.") }}
  {% endif %}
  {% if manifest %}
  MANIFEST
  {% endif %}
  {% if header %}
  HEADER
  {% endif %}
  {% if format %}
  FORMAT AS {{format|upper}}
  {% endif %}
  {% if not format %}
  DELIMITER AS '{{ delimiter }}'
  {% endif %}
  NULL AS '{{ null_as }}'
  MAXFILESIZE AS {{ max_file_size }}
  {% if escape %}
  ESCAPE
  {% endif %}
  {% if compression %}
  {{ compression|upper }}
  {% endif %}
  {% if add_quotes %}
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
  {% if aws_region %}
  REGION '{{ aws_region }}'
  {% endif %}
  {% if partition_by %}
  PARTITION BY ( {{ partition_by | join(', ') }} )
  {% endif %}
{% endmacro %}
