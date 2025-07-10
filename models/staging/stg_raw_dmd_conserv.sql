-- staging/stg_raw_dmd_conserv.sql

WITH loc AS (
    SELECT *
    FROM {{ source('raw_dmd_conserve_source', 'raw_dmd_conserve') }}
),
postcodes_with_geometry AS (
    SELECT
        x.postcode,
        x.geometry_27700,
        x.geometry_4326
    FROM {{ ref('stg_geo_points_uk_postcodes') }} x

)
SELECT
    "Client" AS client,
    "Company number" AS company_number,
    "Business Address" AS business_address,
    "Site Name" AS site_name,
    "Site address if different " AS site_address_if_different,
    {{ extract_uk_postcode('"Business Address"') }} AS business_postcode,
    {{ extract_uk_postcode('"Site address if different "') }} AS site_postcode,
    COALESCE(
        {{ extract_uk_postcode('"Site address if different "') }},
        {{ extract_uk_postcode('"Business Address"') }}
    ) AS postcode_to_use,
    geometry.geometry_4326 AS geometry_4326, -- Include the geometry field
    geometry.geometry_27700 AS geometry_2770,
    "Site Size "::numeric AS site_size,
    "Actual Export"::numeric AS actual_export,
    "+/- 95% Exported" AS plus_minus_95_percent_exported,
    "Export MPAN" AS export_mpan,
    "Technology" AS technology,
    "Connection Voltage" AS connection_voltage,
    "HH data From to " AS hh_data_from_to,
    "Tariff Start" AS tariff_start,
    "Import MPAN" AS import_mpan,
    "Broker" AS broker,
    "Comission per kWh" AS commission_per_kwh,
    'Solar' AS simple_ft
FROM loc
LEFT JOIN postcodes_with_geometry geometry
    ON COALESCE(
        {{ extract_uk_postcode('"Site address if different "') }},
        {{ extract_uk_postcode('"Business Address"') }}
    ) = geometry.postcode
