
  
    

    create or replace table `purwadika`.`rizky_dwh_hailing_facts`.`fact_hailing_rides`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

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


    );
  