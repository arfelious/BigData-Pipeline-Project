-- silver_sellers.sql
-- No transformations required; schema is clean from Bronze.
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('bronze', 'olist_sellers_dataset') }}
