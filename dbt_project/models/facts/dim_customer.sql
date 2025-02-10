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
    FROM {{ ref('production_hailing_staging_customer') }}
)

SELECT * FROM source
{% if check_if_incremental() %}
    WHERE created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}