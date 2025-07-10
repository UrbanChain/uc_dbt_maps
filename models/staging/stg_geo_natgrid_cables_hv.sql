-- models/staging/stg_geo_natgrid_cables_hv.sql

with source as (
    select *
    from {{ source('geo_natgrid_substation_hv_source', 'geo_natgrid_cables_hv') }}
),
transformed AS (
    SELECT
        gdo_gid,
        operating_voltage::TEXT AS operating_cable_voltage,
        action_dtt AS cable_dt,
        status,
        cable_type,
        comments,
        tunnel,
        owned,
        cable_set,
        cable_rout,
        cable_make,
        efd_year,
        geometry AS cable_geometry_4326,
        ST_Transform(geometry, 227700) AS cable_geometry_227700
    from source
    where status like 'C'
)

select *
from transformed