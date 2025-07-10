-- models/staging/stg_geo_ssen_shepd_primary.sql

with source as (
    select *
    from {{ source('geo_ssen_primary_source', 'geo_ssen_shepd_primary') }}
),

transformed as (
    select
        "Primary" as primary_substation, -- Quote reserved keyword
        -- Ensure geometry has SRID 4326 before transforming
        ST_SetSRID(the_geom, 4326) AS geometry_4326 ,
        -- Transform geometry from 4326 to 27700
        ST_Transform(ST_SetSRID(the_geom, 4326), 27700) AS geometry_27700
    from source
)

select *
from transformed