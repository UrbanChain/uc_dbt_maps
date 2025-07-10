-- models/staging/stg_geo_tnuos_gen_zones.sql

with source as (
    select *
    from {{ source('geo_natgrid_generation_zones_source', 'geo_tnuos_gen_zones') }}
),
transformed AS (
    SELECT
        id,
        layer AS zone_name,
        the_geom AS geometry_4326,
        ST_Transform(the_geom, 27700) AS sub_geometry_27700
    from source
)

select *
from transformed