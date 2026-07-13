-- silver_order_items.sql
-- shipping_limit_date arrives as a string from Bronze; cast to TIMESTAMP.
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    price,
    freight_value
FROM {{ source('bronze', 'olist_order_items_dataset') }}
