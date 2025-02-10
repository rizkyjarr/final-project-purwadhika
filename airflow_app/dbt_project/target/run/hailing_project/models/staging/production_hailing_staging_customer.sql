
  
    

    create or replace table `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_customer`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

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

    );
  