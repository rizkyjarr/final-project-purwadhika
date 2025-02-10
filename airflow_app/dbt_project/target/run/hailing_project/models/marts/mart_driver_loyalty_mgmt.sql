-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_marts`.`mart_driver_loyalty_mgmt` as DBT_INTERNAL_DEST
        using (

WITH dim_driver AS(
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_facts`.`dim_driver`
),

fact_rides AS(
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_facts`.`fact_hailing_rides`
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
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.driver_id = DBT_INTERNAL_DEST.driver_id
            )

    
    when matched then update set
        `driver_id` = DBT_INTERNAL_SOURCE.`driver_id`,`driver_name` = DBT_INTERNAL_SOURCE.`driver_name`,`phone_number` = DBT_INTERNAL_SOURCE.`phone_number`,`email` = DBT_INTERNAL_SOURCE.`email`,`vehicle_type` = DBT_INTERNAL_SOURCE.`vehicle_type`,`no_of_rides` = DBT_INTERNAL_SOURCE.`no_of_rides`,`total_ride_duration_min` = DBT_INTERNAL_SOURCE.`total_ride_duration_min`,`total_fare` = DBT_INTERNAL_SOURCE.`total_fare`,`total_distance_km` = DBT_INTERNAL_SOURCE.`total_distance_km`,`rev_opportunity_loss` = DBT_INTERNAL_SOURCE.`rev_opportunity_loss`
    

    when not matched then insert
        (`driver_id`, `driver_name`, `phone_number`, `email`, `vehicle_type`, `no_of_rides`, `total_ride_duration_min`, `total_fare`, `total_distance_km`, `rev_opportunity_loss`)
    values
        (`driver_id`, `driver_name`, `phone_number`, `email`, `vehicle_type`, `no_of_rides`, `total_ride_duration_min`, `total_fare`, `total_distance_km`, `rev_opportunity_loss`)


    