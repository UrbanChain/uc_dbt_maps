-- models/staging/int_epc_scotland.sql

WITH int_epc_scot AS (
    SELECT *
    FROM {{ ref('stg_raw_epc_scotland') }}
)

SELECT
    COALESCE(Address1, '') || ' ' ||
    COALESCE(Address2, '') || ' ' ||
    COALESCE("Post Town", '') || ' ' ||
    COALESCE(Postcode, '') AS Address,

    "Actual Building Data Primary Energy Indicator" AS "Primary Energy Use (kWh/m² per year)",
    "EPC Rating BER" AS "Building Emissions (kg CO₂/m²)",
    "Building Environment",
    "Total floor area (m²)",
    "Comparative Asset Rating" AS "Asset Rating (kg CO₂/m²)",
    "Comparative Energy Band" AS "Asset Rating Band",
    "Main Heating Fuel",
    "Property Type"
FROM
    int_epc_scot