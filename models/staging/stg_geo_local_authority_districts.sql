-- models/staging/stg_geo_local_authority_districts.sql

with source as (
    select *
    from {{ source('geo_local_authority_districts_source', 'geo_local_authority_districts') }}
),

transformed as
    (select
        "FID" as fid,
        "LAD24CD" as lad24cd,
        "LAD24NM" as lad24nm,
        "LAD24NMW" as lad24nmw,
        "BNG_E" as bng_e,
        "BNG_N" as bng_n,
        "LONG" as lon,
        "LAT" as lat,
        "GlobalID" as global_id,
        -- Assign SRID to the geometry
        ST_Transform(ST_SetSRID(geometry, 4326), 27700) AS geometry_27700,
        ST_SetSRID(geometry, 4326) AS geometry_4326
    from source
)

select *
from transformed