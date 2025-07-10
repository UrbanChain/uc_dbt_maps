-- models/staging/stg_geo_spen_spm_primary.sql

with source as (
    select *
    from {{ source('geo_spen_spm_primary_source', 'geo_spen_spm_primary') }}
),

transformed as (
    select
        geo_point_2d as geo_centre_point,
        primary_group,
        grid_group,
        gsp,
        -- Ensure geometry has SRID 4326 before transforming
        ST_SetSRID(geometry, 4326) AS geometry_4326,
        -- Transform geometry from 4326 to 27700
        ST_Transform(ST_SetSRID(geometry, 4326), 27700) AS geometry_27700
    from source
)

select *
from transformed
