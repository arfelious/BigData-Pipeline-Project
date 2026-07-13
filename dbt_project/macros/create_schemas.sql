-- macros/create_schemas.sql
-- Creates the silver and gold databases if they don't already exist.
-- IDEMPOTENT: IF NOT EXISTS means this is always safe to re-run.
{% macro create_schemas() %}
    {% set schemas = ['silver', 'gold'] %}
    {% for schema in schemas %}
        {% set sql %}
            CREATE DATABASE IF NOT EXISTS {{ schema }}
        {% endset %}
        {% do run_query(sql) %}
        {{ log("Created database: " ~ schema, info=True) }}
    {% endfor %}
{% endmacro %}
