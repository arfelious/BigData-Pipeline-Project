-- macros/register_bronze_sources.sql
-- Registers all 9 Bronze Parquet directories as external tables in the
-- `bronze` database so dbt source() references resolve.
--
-- IDEMPOTENT: uses IF NOT EXISTS on both the database and every table.
-- First run  → creates everything from scratch.
-- Subsequent → all statements are no-ops; no data is touched or re-read.
{% macro register_bronze_sources() %}
    {% set tables = [
        'olist_customers_dataset',
        'olist_geolocation_dataset',
        'olist_order_items_dataset',
        'olist_order_payments_dataset',
        'olist_order_reviews_dataset',
        'olist_orders_dataset',
        'olist_products_dataset',
        'olist_sellers_dataset',
        'product_category_name_translation',
    ] %}

    {%- set create_db -%}
        CREATE DATABASE IF NOT EXISTS bronze
    {%- endset -%}
    {% do run_query(create_db) %}

    {% for table in tables %}
        {%- set create_sql -%}
            CREATE TABLE IF NOT EXISTS bronze.{{ table }}
            USING PARQUET
            LOCATION 'hdfs://namenode:9000/olist/{{ table }}'
        {%- endset -%}
        {% do run_query(create_sql) %}
        {%- set refresh_sql -%}
            REFRESH TABLE bronze.{{ table }}
        {%- endset -%}
        {% do run_query(refresh_sql) %}
        {{ log("Ensured bronze source exists and is refreshed: " ~ table, info=True) }}
    {% endfor %}
{% endmacro %}
