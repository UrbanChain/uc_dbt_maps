-- models/staging/stg_view_monthly_co2.sql

WITH stg_co2 AS (
    SELECT *
    FROM {{ ref('stg_raw_co2_regional_actual') }}
),

-- Extracting and grouping relevant data
grouped_region AS (
    SELECT
        region_id,
        DATE_TRUNC('month', from_date) AS month, -- Extract month from the date
        intensity_forecast,
        biomass,
        coal,
        imports,
        gas,
        nuclear,
        other,
        hydro,
        solar,
        wind
    FROM stg_co2
),

-- Calculating monthly averages per region
monthly_averages AS (
    SELECT
        region_id,
        TO_CHAR(month, 'Mon-YY') AS month, -- Format the month as 'Jan-24'
        AVG(intensity_forecast) AS avg_intensity_forecast,
        AVG(biomass) AS avg_biomass,
        AVG(coal) AS avg_coal,
        AVG(imports) AS avg_imports,
        AVG(gas) AS avg_gas,
        AVG(nuclear) AS avg_nuclear,
        AVG(other) AS avg_other,
        AVG(hydro) AS avg_hydro,
        AVG(solar) AS avg_solar,
        AVG(wind) AS avg_wind
    FROM grouped_region
    GROUP BY region_id, month
    ORDER BY region_id, month
),

-- Add geo_id by joining with the lookup table
geo_data AS (
    SELECT
        lkup.geo_id,
        monthly_averages.*

    FROM monthly_averages -- Use monthly_averages instead of parsed_data
    LEFT JOIN (
        SELECT
            co2_region_id,
            geo_id::TEXT
        FROM {{ source('lkup_co2region_dno_source', 'lkup_co2region_dno') }}
    ) lkup
    ON monthly_averages.region_id = lkup.co2_region_id
)

SELECT *
FROM geo_data