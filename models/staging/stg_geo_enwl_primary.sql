-- models/staging/stg_geo_enwl_primary.sql

with source as (
    select *
    from {{ source('geo_enwl_primary_source', 'geo_enwl_primary') }}
),

transformed as (
    select
        pry_group,
        ST_Union(geometry) as geometry_4326,
        ST_Union(ST_Transform(geometry, 27700)) as geometry_27700
    from source
    group by pry_group
)

select *
from transformed