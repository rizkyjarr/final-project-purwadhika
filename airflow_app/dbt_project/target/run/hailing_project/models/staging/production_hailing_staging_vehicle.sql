
  
    

    create or replace table `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_vehicle`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_source`.`production_hailing_source_vehicle`
)

SELECT *
FROM source

    );
  