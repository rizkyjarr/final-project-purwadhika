
  
    

    create or replace table `purwadika`.`rizky_dwh_hailing_facts`.`dim_driver`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      


WITH driver AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_driver` --pakai ref untuk buat dependency

),

vehicle AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_vehicle`
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


    );
  