-- models/staging/stg_geo_ssen_primary.sql

with source as (
    select *
    from {{ source('geo_ssen_primary_source', 'geo_ssen_primary') }}
),

transformed as (
    select
        ogc_fid,
        "primary" as primary_substation, -- Quote reserved keyword
        -- Ensure geometry has SRID 27700 before transforming
        ST_SetSRID(wkb_geometry, 27700) AS geometry_27700,
        -- Transform geometry from 27700 to 4326
        ST_Transform(ST_SetSRID(wkb_geometry, 27700), 4326) AS geometry_4326
    from source
)

select *
from transformed
