-- models/intermediate/int_accred_location.sql

WITH int_accred_location AS (
    SELECT *
    FROM {{ ref('stg_view_accred_location') }}
),
fuel_type_lookup AS (
    SELECT
        int_accred_location.*,
        (SELECT lsoa21cd FROM {{ ref('stg_raw_pcd_lsoa_msoa_2021_lkup') }} WHERE pcds = int_accred_location.generating_station_postcode LIMIT 1) AS lsoa,
        (SELECT simple_fueltype FROM {{ ref('stg_lkup_fueltype_mappings') }} WHERE source_name = int_accred_location.technology_name LIMIT 1) AS simple_ft
    FROM
        int_accred_location
)
SELECT
    fuel_type_lookup.*,
    (SELECT fit_ft FROM {{ ref('stg_lkup_fit_ft_mapping') }} WHERE simple_ft = fuel_type_lookup.simple_ft LIMIT 1) AS fit_fuel_type
FROM
    fuel_type_lookup
