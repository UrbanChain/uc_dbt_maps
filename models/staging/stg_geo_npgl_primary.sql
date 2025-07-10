with source as (
    select *
    from {{ source('geo_npgl_primary_source', 'geo_npgl_primary') }}
),

transformed as (
    select
        ogc_fid,
        "primary" as pry_group,
        substation_class,
        demand_classification as dmd_class,
        summary_overall_primary_classification as gen_class,
        geo_point_2d,
        wkb_geometry as geometry_4326,
        ST_Transform(ST_SetSRID(wkb_geometry, 4326), 27700) AS geometry_27700
    from source
)

select *
from transformed