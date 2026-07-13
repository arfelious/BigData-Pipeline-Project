-- dim_sellers.sql
SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ ref('silver_sellers') }}
