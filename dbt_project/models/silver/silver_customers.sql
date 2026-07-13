-- silver_customers.sql
-- No structural issues found in Bronze. Preserves all columns as-is.
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('bronze', 'olist_customers_dataset') }}
