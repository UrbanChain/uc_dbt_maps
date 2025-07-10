-- models/staging/stg_geo_natgrid_overhead_lines_hv.sql

with source as (
    select *
    from {{ source('geo_natgrid_substation_hv_source', 'geo_natgrid_overhead_lines_hv') }}
),
transformed AS (
    SELECT
        gdo_gid,
        route_asse,
        towers_in,
        -- Convert action_dtt to DATE format
        TO_CHAR(action_dtt::DATE, 'YYYY-MM-DD') AS line_dt,
        status,
        -- Convert operating_voltage to STRING
        operating_voltage::TEXT AS operating_line_voltage,
        circuit1,
        circuit2,
        geometry AS line_geometry_4326,
        -- Transform geometry from 4326 to 27700
        ST_Transform(geometry, 27700) AS line_geometry_27700
    from source
    where status like 'C'
)

select *
from transformed