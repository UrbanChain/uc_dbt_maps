-- models/staging/stg_geo_nged_primary.sql

with source as (
    select *
    from {{ source('geo_nged_primary_source', 'geo_nged_primary') }}
),

transformed as
    (select
        id,
        nrid,
        nr,
        nr_type_id,
        name,
        prim_nrid,
        prim_nr,
        prim_nrid_name,
        bsp_nrid,
        bsp_nr,
        bsp_nrid_name,
        gsp_nrid,
        gsp_nr,
        gsp_nrid_name,
        -- Transform geometry from 4326 to 27700
        ST_Transform(geometry, 4326) AS geometry_4326,
        geometry AS geometry_27700
    from source
)

select *
from transformed