-- File: models/intermediate/int_monthly_co2_praxis.sql
-- Links the Praxis Portfolio (Jan24-Oct24) with the Carbon intensity by region

WITH stg_praxis_month AS (
    SELECT
        p.match_month,
        p.lkup_geo_id::TEXT AS lkup_geo_id, -- Ensure geo_id is cast to TEXT
        p.praxis_sub,
        p.total_volume,
        p.weighted_avg_solar,
        p.weighted_avg_wind,
        p.weighted_avg_hydro,
        p.weighted_avg_ad,
        p.weighted_avg_chp,
        p.weighted_avg_grid,
        p.weighted_avg_other,
        p.weighted_avg_solar * {{ var('R') }} AS pv_value,
        p.weighted_avg_wind * {{ var('R') }} AS wind_value,
        p.weighted_avg_hydro * {{ var('R') }} AS hydro_value,
        p.weighted_avg_ad * {{ var('D') }} AS ad_value,
        p.weighted_avg_chp * {{ var('D') }} AS chp_value,
        p.weighted_avg_grid * co2.avg_intensity_forecast AS grid_value, -- Reference co2 here
        p.weighted_avg_other * {{ var('O') }} AS other_value,
        (
            (p.weighted_avg_solar * {{ var('R') }}) +
            (p.weighted_avg_wind * {{ var('R') }}) +
            (p.weighted_avg_hydro * {{ var('R') }}) +
            (p.weighted_avg_ad * {{ var('D') }}) +
            (p.weighted_avg_chp * {{ var('D') }}) +
            (p.weighted_avg_grid * co2.avg_intensity_forecast) +
            (p.weighted_avg_other * {{ var('O') }})
        ) AS praxis_intensity
    FROM {{ ref('stg_view_monthly_praxis') }} p
    LEFT JOIN {{ ref('stg_view_monthly_co2') }} co2
        ON p.match_month = co2.month
        AND p.lkup_geo_id::TEXT = co2.geo_id -- Explicitly cast to TEXT
),

stg_co2_data AS (
    SELECT
        month,
        geo_id::TEXT AS geo_id, -- Ensure geo_id is cast to TEXT
        avg_intensity_forecast,
        avg_solar / 100 AS avg_solar,   -- Convert percentage to fraction
        avg_wind / 100 AS avg_wind,     -- Convert percentage to fraction
        avg_hydro / 100 AS avg_hydro,   -- Convert percentage to fraction
        avg_biomass / 100 AS avg_biomass, -- Convert percentage to fraction
        avg_other / 100 AS avg_other,   -- Convert percentage to fraction
        avg_gas / 100 AS avg_gas,       -- Convert percentage to fraction
        avg_coal / 100 AS avg_coal,     -- Convert percentage to fraction
        avg_nuclear / 100 AS avg_nuclear, -- Convert percentage to fraction
        avg_imports / 100 AS avg_imports, -- Convert percentage to fraction
        (avg_solar / 100) * avg_intensity_forecast as avg_solar_value,
        (avg_wind / 100) * avg_intensity_forecast as avg_wind_value,
        (avg_hydro / 100) * avg_intensity_forecast as avg_hydro_value,
        (avg_biomass / 100) * avg_intensity_forecast as avg_biomass_value,
        (avg_other / 100) * avg_intensity_forecast as avg_other_value,
        (avg_gas / 100) * avg_intensity_forecast as avg_gas_value,
        (avg_coal / 100) * avg_intensity_forecast as avg_coal_value,
        (avg_nuclear / 100) * avg_intensity_forecast as avg_nuclear_value,
        (avg_imports / 100) * avg_intensity_forecast as avg_imports_value
    FROM {{ ref('stg_view_monthly_co2') }}
)

SELECT
    p.*,
    co2.month,
    co2.geo_id AS co2_geo_id,
    co2.avg_intensity_forecast,
    co2.avg_solar,
    co2.avg_wind,
    co2.avg_hydro,
    co2.avg_biomass,
    co2.avg_other,
    co2.avg_gas,
    co2.avg_coal,
    co2.avg_nuclear,
    co2.avg_imports,
    co2.avg_solar_value,
    co2.avg_wind_value,
    co2.avg_hydro_value,
    co2.avg_biomass_value,
    co2.avg_other_value,
    co2.avg_gas_value,
    co2.avg_coal_value,
    co2.avg_nuclear_value,
    co2.avg_imports_value
FROM
    stg_praxis_month p
LEFT JOIN stg_co2_data co2
    ON
        p.match_month = co2.month
    AND
        p.lkup_geo_id = co2.geo_id -- Both are TEXT now
