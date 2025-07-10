-- models/staging/stg_geo_zones.sql

with source as (
    select *
    from {{ source('geo_zones_source', 'geo_zones') }}
),
transformed AS (
    SELECT
        gid,
        name_1 AS zone_name,
        geometry  AS geometry_4326, -- CRS 4326
        -- Transform geometry from 4326 to 27700
        ST_Transform(geometry, 27700) AS geometry_27700
    from source
)

select *
from transformed