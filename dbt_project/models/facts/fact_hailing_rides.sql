{{
    config(
        materialized='incremental',
        unique_key='ride_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}

WITH dim_driver AS (

    SELECT *
    FROM {{ ref('dim_driver')}}
),

dim_customer AS (

    SELECT * 
    FROM {{ ref('dim_customer')}}
),

ride_staging AS (

    SELECT *
    FROM {{ ref('production_hailing_staging_ride') }}
)

SELECT
ride_staging.ride_id,
ride_staging.start_time AS ride_start_time,
ride_staging.end_time AS ride_end_time,
ride_staging.distance_km,
ride_staging.fare,
ride_staging.ride_status,
dim_customer.cust_id,
dim_customer.name AS cust_name,
dim_customer.phone_number AS cust_phone_number,
dim_customer.email AS cust_email,
dim_driver.driver_id,
dim_driver.driver_name,
dim_driver.phone_number AS driver_phone_number,
dim_driver.email AS driver_email,
dim_driver.vehicle_id,
dim_driver.vehicle_type,
dim_driver.vehicle_brand,
dim_driver.vehicle_production_year,
dim_driver.license_plate,
ride_staging.created_at

FROM ride_staging

LEFT JOIN dim_customer
on ride_staging.cust_id = dim_customer.cust_id

LEFT JOIN dim_driver
on ride_staging.driver_id = dim_driver.driver_id

{% if check_if_incremental() %}
    WHERE ride_staging.created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}