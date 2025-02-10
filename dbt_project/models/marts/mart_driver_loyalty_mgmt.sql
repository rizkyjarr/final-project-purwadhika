{{
    config(
        materialized='incremental',
        unique_key='driver_id',
    )
}}

WITH dim_driver AS(
    SELECT *
    FROM {{ ref('dim_driver')}}
),

fact_rides AS(
    SELECT *
    FROM {{ ref('fact_hailing_rides')}}
)

SELECT 
dim_driver.driver_id,
dim_driver.driver_name,
dim_driver.phone_number,
dim_driver.email,
dim_driver.vehicle_type,
COUNT(fact_rides.ride_id) AS no_of_rides,
SUM(TIMESTAMP_DIFF(fact_rides.ride_end_time, fact_rides.ride_start_time, MINUTE)) AS total_ride_duration_min,
SUM(fact_rides.fare) AS total_fare,
SUM(fact_rides.distance_km) as total_distance_km,
SUM(fact_rides.distance_km)-SUM(fact_rides.fare) AS rev_opportunity_loss,
FROM dim_driver
LEFT JOIN fact_rides
ON dim_driver.driver_id = fact_rides.driver_id

GROUP BY driver_id,driver_name,phone_number,email,vehicle_type