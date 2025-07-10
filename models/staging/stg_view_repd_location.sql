-- models/staging/stg_view_repd_location.sql

WITH stg_view_repd_location AS (
    SELECT
        operator_or_applicant,
        customer_site,
        generating_station_address,
        post_code,
        county,
        country,
        technology_type,
        registered_cap_1,
        development_status_short,
        operational,
        planning_permission_granted,
        last_updated,
        geopoint
    FROM {{ ref('stg_raw_repd_installations') }}
)

SELECT
    stg_view_repd_location.*,
    {{ nearest_postcode('geopoint') }} AS derived_postcode,
    {{ nearest_postcode_distance('geopoint') }} AS distance_to_postcode_km
    -- To add simple_fueltype, uncomment and validate the logic below
    , (SELECT ft.simple_fueltype
      FROM {{ ref('stg_lkup_fueltype_mappings') }} ft
      WHERE ft.source_name = stg_view_repd_location.technology_type
      LIMIT 1) AS simple_ft
FROM
    stg_view_repd_location
