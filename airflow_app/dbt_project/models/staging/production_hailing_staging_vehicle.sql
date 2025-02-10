{{
    config(
        materialized='incremental',
        unique_key='vehicle_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('source_table', 'vehicle_data') }}
)

SELECT *
FROM source
{% if check_if_incremental() %}
    WHERE created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}