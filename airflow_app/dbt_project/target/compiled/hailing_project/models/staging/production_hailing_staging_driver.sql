

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_source`.`production_hailing_source_driver`
),

cleaned AS (
    SELECT
        driver_id,
        name,
        email,
        -- Standardizing phone numbers
        REGEXP_REPLACE(phone_number, r'[\s\-\(\)]', '') AS phone_number,
        created_at
    FROM source
)

SELECT * FROM cleaned
