-- dim_products.sql
-- Slim product dimension exposing only the columns needed for analytical queries.
-- product_category_name_english already resolved in the Silver layer via LEFT JOIN.
SELECT DISTINCT
    product_id,
    product_category_name,
    product_category_name_english
FROM {{ ref('silver_products') }}
