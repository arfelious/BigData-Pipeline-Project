-- fact_orders.sql
-- Order-level fact with pre-calculated delivery_time_days metric.
-- Joins dim_date via order_purchase_date_key (YYYYMMDD integer).
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    CAST(DATE_FORMAT(order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_purchase_date_key,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    -- Pre-calculated metric: elapsed days from purchase to customer delivery
    (CAST(order_delivered_customer_date AS LONG) - CAST(order_purchase_timestamp AS LONG))
        / (24.0 * 3600.0) AS delivery_time_days
FROM {{ ref('silver_orders') }}
