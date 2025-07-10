-- models/staging/stg_geo_points_uk_postcodes.sql

with source as (
    select * from {{ source('geo_points_uk_postcodes_source', 'geo_points_uk_postcodes') }}
)

select
    postcode,
    country_code,
    admin_county_code,
    admin_district_code,
    admin_ward_code,
    geometry as geometry_27700, -- crs of 2770
    geometry_4326 -- crs of 4326
from source

/*
 -- Assuming the geometry column is named 'geom'
SELECT geometry, ST_SRID(geometry) AS srid
FROM public.geo_points_uk_postcodes
LIMIT 1
 */