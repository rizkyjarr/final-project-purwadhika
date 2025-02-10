-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_facts`.`fact_hailing_rides` as DBT_INTERNAL_DEST
        using (

WITH dim_driver AS (

    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_facts`.`dim_driver`
),

dim_customer AS (

    SELECT * 
    FROM `purwadika`.`rizky_dwh_hailing_facts`.`dim_customer`
),

ride_staging AS (

    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_ride`
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


    WHERE ride_staging.created_at > (
        SELECT MAX(created_at)
        FROM `purwadika`.`rizky_dwh_hailing_facts`.`fact_hailing_rides`
    )

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.ride_id = DBT_INTERNAL_DEST.ride_id
            )

    
    when matched then update set
        `ride_id` = DBT_INTERNAL_SOURCE.`ride_id`,`ride_start_time` = DBT_INTERNAL_SOURCE.`ride_start_time`,`ride_end_time` = DBT_INTERNAL_SOURCE.`ride_end_time`,`distance_km` = DBT_INTERNAL_SOURCE.`distance_km`,`fare` = DBT_INTERNAL_SOURCE.`fare`,`ride_status` = DBT_INTERNAL_SOURCE.`ride_status`,`cust_id` = DBT_INTERNAL_SOURCE.`cust_id`,`cust_name` = DBT_INTERNAL_SOURCE.`cust_name`,`cust_phone_number` = DBT_INTERNAL_SOURCE.`cust_phone_number`,`cust_email` = DBT_INTERNAL_SOURCE.`cust_email`,`driver_id` = DBT_INTERNAL_SOURCE.`driver_id`,`driver_name` = DBT_INTERNAL_SOURCE.`driver_name`,`driver_phone_number` = DBT_INTERNAL_SOURCE.`driver_phone_number`,`driver_email` = DBT_INTERNAL_SOURCE.`driver_email`,`vehicle_id` = DBT_INTERNAL_SOURCE.`vehicle_id`,`vehicle_type` = DBT_INTERNAL_SOURCE.`vehicle_type`,`vehicle_brand` = DBT_INTERNAL_SOURCE.`vehicle_brand`,`vehicle_production_year` = DBT_INTERNAL_SOURCE.`vehicle_production_year`,`license_plate` = DBT_INTERNAL_SOURCE.`license_plate`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`
    

    when not matched then insert
        (`ride_id`, `ride_start_time`, `ride_end_time`, `distance_km`, `fare`, `ride_status`, `cust_id`, `cust_name`, `cust_phone_number`, `cust_email`, `driver_id`, `driver_name`, `driver_phone_number`, `driver_email`, `vehicle_id`, `vehicle_type`, `vehicle_brand`, `vehicle_production_year`, `license_plate`, `created_at`)
    values
        (`ride_id`, `ride_start_time`, `ride_end_time`, `distance_km`, `fare`, `ride_status`, `cust_id`, `cust_name`, `cust_phone_number`, `cust_email`, `driver_id`, `driver_name`, `driver_phone_number`, `driver_email`, `vehicle_id`, `vehicle_type`, `vehicle_brand`, `vehicle_production_year`, `license_plate`, `created_at`)


    