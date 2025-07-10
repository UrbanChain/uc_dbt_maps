-- models/staging/stg_view_monthly_praxis.sql
WITH stg_praxis AS (
    SELECT
        geo.geo_id AS lkup_geo_id, -- Spatial join result
        loc.gsp_group_id,
        loc.match_month, -- Format month as 'YYYY-MM' for clarity
        loc.praxis_sub,
        loc.geom,
        CAST(loc.volume AS NUMERIC) AS volume, -- Ensure volume is numeric
        CAST(loc.solar AS NUMERIC) AS solar, -- Ensure solar is numeric
        CAST(loc.wind AS NUMERIC) AS wind, -- Ensure wind is numeric
        CAST(loc.hydro AS NUMERIC) AS hydro, -- Ensure hydro is numeric
        CAST(loc.ad AS NUMERIC) AS ad, -- Ensure ad is numeric
        CAST(loc.chp AS NUMERIC) AS chp, -- Ensure chp is numeric
        CAST(loc.grid AS NUMERIC) AS grid, -- Ensure grid is numeric
        CAST(loc.other AS NUMERIC) AS other -- Ensure other is numeric
    FROM {{ ref('stg_raw_dmd_praxis') }} loc
    LEFT JOIN
    {{ source('geo_gb_dno_license_areas_source', 'geo_gb_dno_license_areas') }} geo
    ON
    loc.gsp_group_id = geo.name
),

summed_and_weighted AS (
    SELECT
        match_month,
        praxis_sub,
        lkup_geo_id,
        geom,
        SUM(volume) AS total_volume,
        -- Weighted averages
        CASE WHEN SUM(volume) > 0 THEN SUM(solar * volume) / SUM(volume) ELSE NULL END AS weighted_avg_solar,
        CASE WHEN SUM(volume) > 0 THEN SUM(wind * volume) / SUM(volume) ELSE NULL END AS weighted_avg_wind,
        CASE WHEN SUM(volume) > 0 THEN SUM(hydro * volume) / SUM(volume) ELSE NULL END AS weighted_avg_hydro,
        CASE WHEN SUM(volume) > 0 THEN SUM(ad * volume) / SUM(volume) ELSE NULL END AS weighted_avg_ad,
        CASE WHEN SUM(volume) > 0 THEN SUM(chp * volume) / SUM(volume) ELSE NULL END AS weighted_avg_chp,
        CASE WHEN SUM(volume) > 0 THEN SUM(grid * volume) / SUM(volume) ELSE NULL END AS weighted_avg_grid,
        CASE WHEN SUM(volume) > 0 THEN SUM(other * volume) / SUM(volume) ELSE NULL END AS weighted_avg_other
    FROM stg_praxis
    GROUP BY match_month, praxis_sub, lkup_geo_id, geom
    ORDER BY match_month, praxis_sub, lkup_geo_id, geom
)

SELECT *
FROM summed_and_weighted
