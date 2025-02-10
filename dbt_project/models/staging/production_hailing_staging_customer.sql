{{
    config(
        materialized='incremental',
        unique_key='cust_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('source_table', 'customer_data') }}
),

cleaned AS (
    SELECT
        cust_id,
        name,
        -- Standardizing phone numbers (removing parentheses, dashes, spaces)
        REGEXP_REPLACE(phone_number, r'[\s\-\(\)]', '') AS phone_number,
        email,
        created_at
    FROM source
)

SELECT * FROM cleaned
{% if check_if_incremental() %}
    WHERE created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}