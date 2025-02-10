-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_customer` as DBT_INTERNAL_DEST
        using (

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_source`.`production_hailing_source_customer`
),

cleaned AS (
    SELECT
        cust_id,
        name,
        -- Standardizing phone numbers (removing parentheses, dashes, spaces)
        REGEXP_REPLACE(phone_number, r'[\s\-\(\)]', '') AS phone_number,
        email,
        created_at
    FROM source
)

SELECT * FROM cleaned

    WHERE created_at > (
        SELECT MAX(created_at)
        FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_customer`
    )

        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.cust_id = DBT_INTERNAL_DEST.cust_id
            )

    
    when matched then update set
        `cust_id` = DBT_INTERNAL_SOURCE.`cust_id`,`name` = DBT_INTERNAL_SOURCE.`name`,`phone_number` = DBT_INTERNAL_SOURCE.`phone_number`,`email` = DBT_INTERNAL_SOURCE.`email`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`
    

    when not matched then insert
        (`cust_id`, `name`, `phone_number`, `email`, `created_at`)
    values
        (`cust_id`, `name`, `phone_number`, `email`, `created_at`)


    