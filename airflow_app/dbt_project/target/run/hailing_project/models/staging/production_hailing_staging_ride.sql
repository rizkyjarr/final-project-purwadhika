-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_ride` as DBT_INTERNAL_DEST
        using (

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_source`.`production_hailing_source_ride`
)

SELECT *
FROM source

    WHERE created_at > (
        SELECT MAX(created_at)
        FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_ride`
    )

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.ride_id = DBT_INTERNAL_DEST.ride_id
            )

    
    when matched then update set
        `ride_id` = DBT_INTERNAL_SOURCE.`ride_id`,`cust_id` = DBT_INTERNAL_SOURCE.`cust_id`,`driver_id` = DBT_INTERNAL_SOURCE.`driver_id`,`start_time` = DBT_INTERNAL_SOURCE.`start_time`,`end_time` = DBT_INTERNAL_SOURCE.`end_time`,`distance_km` = DBT_INTERNAL_SOURCE.`distance_km`,`fare` = DBT_INTERNAL_SOURCE.`fare`,`ride_status` = DBT_INTERNAL_SOURCE.`ride_status`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`
    

    when not matched then insert
        (`ride_id`, `cust_id`, `driver_id`, `start_time`, `end_time`, `distance_km`, `fare`, `ride_status`, `created_at`)
    values
        (`ride_id`, `cust_id`, `driver_id`, `start_time`, `end_time`, `distance_km`, `fare`, `ride_status`, `created_at`)


    