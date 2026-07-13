-- silver_products.sql
-- Left-join the English category translation so every product row carries
-- product_category_name_english. Products with no translation keep their
-- Portuguese name via COALESCE.
SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english, p.product_category_name)
        AS product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM {{ source('bronze', 'olist_products_dataset') }} p
LEFT JOIN {{ source('bronze', 'product_category_name_translation') }} t
    ON p.product_category_name = t.product_category_name
