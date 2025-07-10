-- models/staging/stg_geo_natgrid_gsp.sql

WITH source AS (
    SELECT *
    FROM {{ source('geo_natgrid_gsp_source', 'geo_natgrid_gsp') }}
),
transformed AS (
    SELECT
        gsps,
        gspgroup,
        ST_SetSRID(the_geom, 4326) AS geometry_4326, -- Ensure SRID is set to 4326
        ST_Transform(ST_SetSRID(the_geom, 4326), 27700) AS geometry_27700 -- Reprojecting to British National Grid
    FROM source
)

SELECT *
FROM transformed