-- fact_order_payments.sql
-- Payment grain. References dim_date via order_purchase_date_key.
WITH orders_lookup AS (
    SELECT
        order_id,
        CAST(DATE_FORMAT(order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_purchase_date_key
    FROM {{ ref('silver_orders') }}
)
SELECT
    p.order_id,
    p.payment_sequential,
    o.order_purchase_date_key,
    p.payment_type,
    p.payment_installments,
    p.payment_value
FROM {{ ref('silver_order_payments') }} p
INNER JOIN orders_lookup o ON p.order_id = o.order_id
