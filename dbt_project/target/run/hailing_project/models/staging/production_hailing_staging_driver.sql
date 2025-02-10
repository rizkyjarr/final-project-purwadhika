-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_driver` as DBT_INTERNAL_DEST
        using (

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_source`.`production_hailing_source_driver`
),

cleaned AS (
    SELECT
        driver_id,
        name,
        email,
        -- Standardizing phone numbers
        REGEXP_REPLACE(phone_number, r'[\s\-\(\)]', '') AS phone_number,
        created_at
    FROM source
)

SELECT * FROM cleaned

    WHERE created_at > (
        SELECT MAX(created_at)
        FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_driver`
    )

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.driver_id = DBT_INTERNAL_DEST.driver_id
            )

    
    when matched then update set
        `driver_id` = DBT_INTERNAL_SOURCE.`driver_id`,`name` = DBT_INTERNAL_SOURCE.`name`,`email` = DBT_INTERNAL_SOURCE.`email`,`phone_number` = DBT_INTERNAL_SOURCE.`phone_number`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`
    

    when not matched then insert
        (`driver_id`, `name`, `email`, `phone_number`, `created_at`)
    values
        (`driver_id`, `name`, `email`, `phone_number`, `created_at`)


    