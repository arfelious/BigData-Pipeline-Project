-- fact_order_items.sql
-- Line-item grain. References dim_products, dim_sellers, dim_customers, dim_date.
-- customer_id and order_purchase_date_key are pulled from fact_orders (Silver orders).
WITH orders_lookup AS (
    SELECT
        order_id,
        customer_id,
        CAST(DATE_FORMAT(order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_purchase_date_key
    FROM {{ ref('silver_orders') }}
)
SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    o.order_purchase_date_key,
    i.price,
    i.freight_value
FROM {{ ref('silver_order_items') }} i
INNER JOIN orders_lookup o ON i.order_id = o.order_id
