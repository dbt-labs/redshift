
{#
    CREATE [ [LOCAL ] { TEMPORARY | TEMP } ] TABLE 
    [ IF NOT EXISTS ] table_name
    ( { column_name data_type [column_attributes] [ column_constraints ] 
      | table_constraints
      | LIKE parent_table [ { INCLUDING | EXCLUDING } DEFAULTS ] } 
      [, ... ]  )
    [ BACKUP { YES | NO } ]
    [table_attribute]

    where column_attributes are:
      [ DEFAULT default_expr ]
      [ IDENTITY ( seed, step ) ] 
      [ ENCODE encoding ] 
      [ DISTKEY ]
      [ SORTKEY ]

    and column_constraints are:
      [ { NOT NULL | NULL } ]
      [ { UNIQUE  |  PRIMARY KEY } ]
      [ REFERENCES reftable [ ( refcolumn ) ] ] 

    and table_constraints  are:
      [ UNIQUE ( column_name [, ... ] ) ]
      [ PRIMARY KEY ( column_name [, ... ] )  ]
      [ FOREIGN KEY (column_name [, ... ] ) REFERENCES reftable [ ( refcolumn ) ] 

    and table_attributes are:
      [ DISTSTYLE { EVEN | KEY | ALL } ] 
      [ DISTKEY ( column_name ) ]
      [ [COMPOUND | INTERLEAVED ] SORTKEY ( column_name [, ...] ) ]
#}

{% macro build_ddl_sql(def) %}

    -- DROP
    drop table if exists "{{ def['schema'] }}"."{{ def['name'] }}";
    -- CREATE
    create table "{{ def['schema'] }}"."{{ def['name'] }}" (
        -- COLUMNS
        {% for column in def['columns'].values() | sort(attribute='position') -%}
            "{{ column['name'] }}" {{ column['type'] }}
            {%- if column['encoding'] is not none %} encode {{ column['encoding'] }} {% endif -%}
            {%- if column['default'] is not none %} default {{ column['default'] }} {% endif -%}
            {%- if column['not_null'] %} not null {% endif -%}
            {%- if not loop.last %}, {% endif %}
        {% endfor %}

        -- CONSTRAINTS
        {% for constraint in def['constraints'] -%}
            , {{ constraint['col_constraint'] }}
        {% endfor %}
    )

    --KEYS
    {% if def['keys'] %}
        {% set dist_style = def['keys']['dist_style'] %}
        {% set dist_key = def['keys']['dist_key'] %}
        -- DIST
        {% if dist_style %} diststyle {{ dist_style }} {% endif %}
        {% if dist_key %} distkey("{{ dist_key }}") {% endif %}

        -- SORT
        {% set sort_style = def['keys']['sort_style'] %}
        {% set sort_keys = def['keys']['sort_keys'] %}
        {% set sort_keys_s = sort_keys | join('", "') %}
        {% if sort_keys %} {{ sort_style }} sortkey("{{ sort_keys_s }}") {% endif %}
    {% endif %}
    ;

    -- TABLE COMMENT
    {% if def['description'] %}
        comment on table "{{ def['schema'] }}"."{{ def['name'] }}" is '{{ def["description"] }}';
    {% endif %}

    -- COLUMN COMMENTS
    {% for column in def['columns'].values() -%}
        {%- if column['description'] -%}
            comment on column "{{ def['schema'] }}"."{{ def['name'] }}"."{{ column['name'] }}" is '{{ column["description"] }}';

        {% endif -%}
    {% endfor %}

{% endmacro %}
