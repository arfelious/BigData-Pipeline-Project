-- silver_geolocation.sql
-- The Bronze geolocation table contains exact row-level duplicates
-- (same ZIP / lat / lng / city / state recorded multiple times).
-- DISTINCT removes them; no single PK exists for this table.
SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM {{ source('bronze', 'olist_geolocation_dataset') }}
