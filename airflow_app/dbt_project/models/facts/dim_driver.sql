{{
    config(
        materialized='incremental',
        unique_key='driver_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}


WITH driver AS (
    SELECT *
    FROM {{ ref('production_hailing_staging_driver') }} --pakai ref untuk buat dependency

),

vehicle AS (
    SELECT *
    FROM {{ ref('production_hailing_staging_vehicle')}}
)

SELECT
    driver.driver_id,
    vehicle.vehicle_id,
    driver.name AS driver_name,
    driver.phone_number AS phone_number,
    driver.email AS email,
    vehicle.vehicle_type,
    vehicle.brand AS vehicle_brand,
    vehicle.year AS vehicle_production_year,
    vehicle.license_plate,
    driver.created_at
FROM driver
LEFT JOIN vehicle
on driver.driver_id = vehicle.driver_id

{% if check_if_incremental() %}
    WHERE driver.created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}