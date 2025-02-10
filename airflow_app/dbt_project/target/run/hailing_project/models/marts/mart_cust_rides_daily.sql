-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_marts`.`mart_cust_rides_daily` as DBT_INTERNAL_DEST
        using (


WITH fact_rides AS(

    SELECT * 
    FROM `purwadika`.`rizky_dwh_hailing_facts`.`fact_hailing_rides`
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
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.cust_id = DBT_INTERNAL_DEST.cust_id
                ) and (
                    DBT_INTERNAL_SOURCE.ride_date = DBT_INTERNAL_DEST.ride_date
                )

    
    when matched then update set
        `cust_id` = DBT_INTERNAL_SOURCE.`cust_id`,`ride_date` = DBT_INTERNAL_SOURCE.`ride_date`,`cust_name` = DBT_INTERNAL_SOURCE.`cust_name`,`cust_email` = DBT_INTERNAL_SOURCE.`cust_email`,`cust_phone_number` = DBT_INTERNAL_SOURCE.`cust_phone_number`,`no_of_rides` = DBT_INTERNAL_SOURCE.`no_of_rides`,`total_ride_duration_min` = DBT_INTERNAL_SOURCE.`total_ride_duration_min`,`total_fare` = DBT_INTERNAL_SOURCE.`total_fare`,`total_distance_km` = DBT_INTERNAL_SOURCE.`total_distance_km`,`rev_opportunity_loss` = DBT_INTERNAL_SOURCE.`rev_opportunity_loss`
    

    when not matched then insert
        (`cust_id`, `ride_date`, `cust_name`, `cust_email`, `cust_phone_number`, `no_of_rides`, `total_ride_duration_min`, `total_fare`, `total_distance_km`, `rev_opportunity_loss`)
    values
        (`cust_id`, `ride_date`, `cust_name`, `cust_email`, `cust_phone_number`, `no_of_rides`, `total_ride_duration_min`, `total_fare`, `total_distance_km`, `rev_opportunity_loss`)


    