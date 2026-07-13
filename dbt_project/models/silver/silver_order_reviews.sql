-- silver_order_reviews.sql
-- Two date/time columns stored as strings in Bronze are cast to TIMESTAMP.
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date    AS TIMESTAMP) AS review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
FROM {{ source('bronze', 'olist_order_reviews_dataset') }}
