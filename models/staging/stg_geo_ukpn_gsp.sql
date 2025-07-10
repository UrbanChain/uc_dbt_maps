-- models/staging/stg_geo_ukpn_gsp.sql

with source as (
    select *
    from {{ source('geo_ukpn_gsp_source', 'geo_ukpn_gsp') }}
),

transformed as (
    select
        'UKPN' as licence_ar, -- Add a static value for licence
        grid_supply as gsp_name,
        -- Convert 'the_geom' from text to geometry in SRID 4326
        ST_SetSRID(ST_GeomFromText(the_geom), 4326) as geometry_4326,
        -- Transform geometry from SRID 4326 to SRID 27700
        ST_Transform(ST_SetSRID(ST_GeomFromText(the_geom), 4326), 27700) as geometry_27700
    from source
)

select *
from transformed
