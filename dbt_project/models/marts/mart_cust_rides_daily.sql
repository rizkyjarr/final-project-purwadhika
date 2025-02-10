{{
    config(
        materialized='incremental',
        unique_key=['cust_id','ride_date'],
            partition_by={
                "field": "ride_date",
                "data_type": "date"
            }
    )
}}


WITH fact_rides AS(

    SELECT * 
    FROM {{ ref('fact_hailing_rides')}}
)

SELECT 

    fact_rides.cust_id,
    DATE(fact_rides.created_at) AS ride_date,
    fact_rides.cust_name,
    fact_rides.cust_email,
    fact_rides.cust_phone_number,
    COUNT(fact_rides.ride_id) AS no_of_rides,
    SUM(TIMESTAMP_DIFF(fact_rides.ride_end_time, fact_rides.ride_start_time, MINUTE)) AS total_ride_duration_min,
    SUM(fact_rides.fare) AS total_fare,
    SUM(fact_rides.distance_km) as total_distance_km,
    SUM(fact_rides.distance_km)-SUM(fact_rides.fare) AS rev_opportunity_loss,

FROM fact_rides
GROUP BY cust_name, cust_id, ride_date, cust_email, cust_phone_number
    