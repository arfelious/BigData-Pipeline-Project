-- fact_order_reviews.sql
-- Review grain. Links to fact_order_items via order_id for category-level scoring.
SELECT
    review_id,
    order_id,
    review_score,
    review_creation_date
FROM {{ ref('silver_order_reviews') }}
