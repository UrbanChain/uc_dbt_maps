-- models/staging/stg_geo_gb_dno_license_areas.sql

with source as (
    select *
    from {{ source('geo_gb_dno_license_areas_source', 'geo_gb_dno_license_areas') }}
),

transformed as (
    select
        geo_id,
        name as name_ltr,
        dno_full,
        dno,
        area,
        geom AS geometry_27700,
        ST_Transform(geom, 4326) AS geometry_4326
    from source
)

select *
from transformed