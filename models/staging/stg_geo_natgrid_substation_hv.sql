-- models/staging/stg_geo_natgrid_substation_hv.sql

with source as (
    select *
    from {{ source('geo_natgrid_substation_hv_source', 'geo_natgrid_substation_hv') }}
),
transformed AS (
    SELECT
        gdo_gid,
        substation as substation_name,
        TO_CHAR(source.action_date::DATE, 'YYYY-MM-DD') AS sub_dt,
        status,
        operating_voltage::TEXT AS operating_substation_voltage,
        substation_name_detail,
        owner_flag,
        ST_Transform(geom, 4326) AS sub_geometry_4326,
        geom AS sub_geometry_27700
    from source
    where status like 'C'
)

select *
from transformed