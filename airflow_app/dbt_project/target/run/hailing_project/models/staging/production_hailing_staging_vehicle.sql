-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_vehicle` as DBT_INTERNAL_DEST
        using (

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_source`.`production_hailing_source_vehicle`
)

SELECT *
FROM source

    WHERE created_at > (
        SELECT MAX(created_at)
        FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_vehicle`
    )

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.vehicle_id = DBT_INTERNAL_DEST.vehicle_id
            )

    
    when matched then update set
        `vehicle_id` = DBT_INTERNAL_SOURCE.`vehicle_id`,`driver_id` = DBT_INTERNAL_SOURCE.`driver_id`,`vehicle_type` = DBT_INTERNAL_SOURCE.`vehicle_type`,`license_plate` = DBT_INTERNAL_SOURCE.`license_plate`,`year` = DBT_INTERNAL_SOURCE.`year`,`brand` = DBT_INTERNAL_SOURCE.`brand`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`
    

    when not matched then insert
        (`vehicle_id`, `driver_id`, `vehicle_type`, `license_plate`, `year`, `brand`, `created_at`)
    values
        (`vehicle_id`, `driver_id`, `vehicle_type`, `license_plate`, `year`, `brand`, `created_at`)


    