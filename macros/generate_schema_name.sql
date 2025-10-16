{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# Check if the target environment name is one of our known development names #}
    {%- if target.name in ['dev', 'default'] -%}

        {# If it is 'dev', ALWAYS use the default schema, ignoring any custom schema #}
        {{ default_schema }}

    {%- else -%}

        {# If it's not 'dev' (e.g., 'prod', 'ci'), then use the original logic #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
