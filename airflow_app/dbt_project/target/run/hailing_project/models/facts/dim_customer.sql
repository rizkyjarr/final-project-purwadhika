
  
    

    create or replace table `purwadika`.`rizky_dwh_hailing_facts`.`dim_customer`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

WITH source AS (
    SELECT *
    FROM `purwadika`.`rizky_dwh_hailing_staging`.`production_hailing_staging_customer`
)

SELECT * FROM source

    );
  