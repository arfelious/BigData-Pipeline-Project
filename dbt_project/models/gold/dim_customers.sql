-- dim_customers.sql
-- One distinct row per customer_id from the Silver customers table.
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ ref('silver_customers') }}
