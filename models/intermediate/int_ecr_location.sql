-- models/intermediate/int_ecr_location.sql

WITH int_ecr_location AS (
    SELECT *
    FROM {{ ref('stg_view_ecr_location') }}
)

SELECT
    int_ecr_location.*,
    (SELECT lsoa21cd FROM {{ ref('stg_raw_pcd_lsoa_msoa_2021_lkup') }} WHERE pcds = int_ecr_location.postcode_used LIMIT 1) AS lsoa,
    (SELECT simple_fueltype FROM {{ ref('stg_lkup_fueltype_mappings') }} WHERE source_name = int_ecr_location.energy_source_1 LIMIT 1) AS simple_ft
FROM
    int_ecr_location
