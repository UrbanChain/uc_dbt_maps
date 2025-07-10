-- models/staging/stg_geo_ssen_sepd_gsp.sql

with source as (
    select *
    from {{ source('geo_ssen_sepd_gsp_source', 'geo_ssen_sepd_gsp') }}
),

transformed as (
    select
        licence_ar,
        gsp_name,
        -- Ensure geometry is in SRID 4326
        ST_Transform(geom, 4326) as geometry_4326,
        -- Ensure geometry is in SRID 27700
        ST_Transform(geom, 27700) as geometry_27700
    from source
)

select *
from transformed
