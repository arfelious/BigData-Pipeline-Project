-- dim_date.sql
-- Generates one row per calendar day spanning the full range of order dates.
-- date_key (YYYYMMDD integer) is the foreign key used by all fact tables.
WITH date_bounds AS (
    SELECT
        MIN(DATE(order_purchase_timestamp)) AS min_date,
        MAX(DATE(order_purchase_timestamp)) AS max_date
    FROM {{ ref('silver_orders') }}
),
date_series AS (
    SELECT
        EXPLODE(SEQUENCE(min_date, max_date, INTERVAL 1 DAY)) AS date
    FROM date_bounds
)
SELECT
    date,
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT)  AS date_key,
    YEAR(date)                                   AS year,
    MONTH(date)                                  AS month,
    DATE_FORMAT(date, 'MMMM')                    AS month_name,
    QUARTER(date)                                AS quarter,
    DAYOFWEEK(date)                              AS day_of_week,
    DAYOFMONTH(date)                             AS day_of_month
FROM date_series
