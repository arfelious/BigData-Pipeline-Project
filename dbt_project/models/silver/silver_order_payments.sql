-- silver_order_payments.sql
-- No date columns; schema is preserved. Numeric types already inferred by Spark.
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ source('bronze', 'olist_order_payments_dataset') }}
