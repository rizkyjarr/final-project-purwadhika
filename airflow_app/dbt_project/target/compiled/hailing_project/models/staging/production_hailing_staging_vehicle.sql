

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
