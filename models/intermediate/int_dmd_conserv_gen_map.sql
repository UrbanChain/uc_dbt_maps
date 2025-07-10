-- models/intermediate/int_dmd_conserv_gen_map.sql

WITH stg_raw_dmd_conserv AS (
    SELECT *
    FROM {{ ref('stg_raw_dmd_conserv') }}
),

stg_geo_gb_dno_license_areas AS (
    SELECT *
    FROM {{ ref('stg_geo_gb_dno_license_areas') }}
)

SELECT
    a.*,
    b.geo_id,
    b.name_ltr,
    b.dno,
    b.dno_full,
    b.area AS dno_area,
    b.geometry_4326 AS dno_geometry_4326,
    b.geometry_27700 AS dno_geometry_27700
FROM
    stg_raw_dmd_conserv a
LEFT JOIN
    stg_geo_gb_dno_license_areas b
ON
    ST_Intersects(
        a.geometry_2770,
        b.geometry_27700
    )
