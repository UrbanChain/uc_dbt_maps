-- models/staging/stg_view_geo_primary.sql

WITH stg_geo_enwl_primary AS (
    SELECT
        pry_group AS primary_name,
        'ENWL' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_enwl_primary') }}
),

stg_geo_nged_primary AS (
    SELECT
        prim_nrid_name AS primary_name,
        'NGED' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_nged_primary') }}
),

stg_geo_npgl_primary AS (
    SELECT
        pry_group AS primary_name,
        'NPGL' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_npgl_primary') }}
    WHERE substation_class LIKE 'Primary'
),

stg_geo_spen_spd_primary AS (
    SELECT
        primary_substation AS primary_name,
        'SPEN' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_spen_spd_primary') }}
),

stg_geo_spen_spm_primary AS (
    SELECT
        primary_group AS primary_name,
        'SPEN' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_spen_spm_primary') }}
),

stg_geo_ssen_primary AS (
    SELECT
        primary_substation AS primary_name,
        'SSEN' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_ssen_primary') }}
),

stg_geo_ssen_shepd_primary AS (
    SELECT
        primary_substation AS primary_name,
        'SSEN' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_ssen_shepd_primary') }}
),
stg_geo_ukpn_primary AS (
    SELECT
        primarysubstationname AS primary_name,
        'UKPN' AS dno,
        geometry_4326,
        geometry_27700
    FROM {{ ref('stg_geo_ukpn_primary') }}
),

combined_primary AS (
    SELECT * FROM stg_geo_enwl_primary
    UNION ALL
    SELECT * FROM stg_geo_nged_primary
    UNION ALL
    SELECT * FROM stg_geo_npgl_primary
    UNION ALL
    SELECT * FROM stg_geo_spen_spd_primary
    UNION ALL
    SELECT * FROM stg_geo_spen_spm_primary
    UNION ALL
    SELECT * FROM stg_geo_ssen_primary
    UNION ALL
    SELECT * FROM stg_geo_ssen_shepd_primary
    UNION ALL
    SELECT * FROM stg_geo_ukpn_primary
),

aggregated_geometries AS (
    SELECT
        primary_name,
        dno,
        ST_Union(geometry_4326) AS aggregated_geometry_4326,
        ST_Union(geometry_27700) AS aggregated_geometry_27700
    FROM combined_primary
    GROUP BY primary_name, dno
)

SELECT
    primary_name,
    dno,
    aggregated_geometry_4326,
    aggregated_geometry_27700
FROM
    aggregated_geometries