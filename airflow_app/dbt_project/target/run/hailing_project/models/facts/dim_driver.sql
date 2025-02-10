-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_facts`.`dim_driver` as DBT_INTERNAL_DEST
        using (


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


    WHERE driver.created_at > (
        SELECT MAX(created_at)
        FROM `purwadika`.`rizky_dwh_hailing_facts`.`dim_driver`
    )

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.driver_id = DBT_INTERNAL_DEST.driver_id
            )

    
    when matched then update set
        `driver_id` = DBT_INTERNAL_SOURCE.`driver_id`,`vehicle_id` = DBT_INTERNAL_SOURCE.`vehicle_id`,`driver_name` = DBT_INTERNAL_SOURCE.`driver_name`,`phone_number` = DBT_INTERNAL_SOURCE.`phone_number`,`email` = DBT_INTERNAL_SOURCE.`email`,`vehicle_type` = DBT_INTERNAL_SOURCE.`vehicle_type`,`vehicle_brand` = DBT_INTERNAL_SOURCE.`vehicle_brand`,`vehicle_production_year` = DBT_INTERNAL_SOURCE.`vehicle_production_year`,`license_plate` = DBT_INTERNAL_SOURCE.`license_plate`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`
    

    when not matched then insert
        (`driver_id`, `vehicle_id`, `driver_name`, `phone_number`, `email`, `vehicle_type`, `vehicle_brand`, `vehicle_production_year`, `license_plate`, `created_at`)
    values
        (`driver_id`, `vehicle_id`, `driver_name`, `phone_number`, `email`, `vehicle_type`, `vehicle_brand`, `vehicle_production_year`, `license_plate`, `created_at`)


    