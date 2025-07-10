-- models/staging/stg_raw_dmd_praxis.sql

WITH loc AS (
    SELECT *
    -- add lkup to dno baaed on post code
    FROM {{ source('raw_dmd_praxis_source', 'raw_dmd_praxis_location') }}
),

dmd AS (
    SELECT
        month AS match_month,
        praxis_sub,
        -- Clean and cast numeric columns
        CASE
            WHEN solar::TEXT = '-' THEN NULL
            ELSE solar::TEXT::NUMERIC
        END AS solar,
        CASE
            WHEN wind::TEXT = '-' THEN NULL
            ELSE wind::TEXT::NUMERIC
        END AS wind,
        CASE
            WHEN hydro::TEXT = '-' THEN NULL
            ELSE hydro::TEXT::NUMERIC
        END AS hydro,
        CASE
            WHEN ad::TEXT = '-' THEN NULL
            ELSE ad::TEXT::NUMERIC
        END AS ad,
        CASE
            WHEN chp::TEXT = '-' THEN NULL
            ELSE chp::TEXT::NUMERIC
        END AS chp,
        CASE
            WHEN grid::TEXT = '-' THEN NULL
            ELSE grid::TEXT::NUMERIC
        END AS grid,
        CASE
            WHEN other::TEXT = '-' THEN NULL
            ELSE other::TEXT::NUMERIC
        END AS other,
        mpan,
        LEFT(mpan, 2) AS geo_id,
        regexp_replace(TRIM({{ 'volume' }}), ',', '', 'g')::NUMERIC AS volume
    FROM {{ source('raw_dmd_praxis_source', 'raw_dmd_praxis') }}
)

SELECT
    dmd.match_month,
    dmd.geo_id,
    loc.gsp_group_id,
    loc.mpan_core,
    dmd.praxis_sub,
    dmd.volume,
    dmd.solar,
    dmd.wind,
    dmd.hydro,
    dmd.ad,
    dmd.chp,
    dmd.grid,
    dmd.other,
    loc.metering_point_postcode,
    (SELECT lsoa21cd FROM {{ ref('stg_raw_pcd_lsoa_msoa_2021_lkup') }}
                     WHERE pcds = loc.metering_point_postcode LIMIT 1) AS lsoa,
    (SELECT geometry_4326 FROM {{ source('geo_points_uk_postcodes_source', 'geo_points_uk_postcodes') }}
                     WHERE postcode = loc.metering_point_postcode LIMIT 1) AS geom
FROM loc
JOIN dmd
ON loc.mpan_core = dmd.mpan