-- models/staging/stg_geo_ukpn_primary.sql

with source as (
    select *
    from {{ source('geo_ukpn_primary_source', 'geo_ukpn_primary') }}
),

transformed as (
    select
        id,
        primarysubstationname,
        grid_site,
        grid_site_floc,
        grid_supply_point,
        grid_supply_point_floc,
        operational_zone,
        dno,
        firmcapacitywinter,
        firmcapacitysummer,
        seasonofconstraint,
        demandrag,
        demand,
        primarysitefunctionallocation,
        geometry AS geometry_4326, -- CRS 4326
        -- Transform geometry from 4326 to 27700
        ST_Transform(geometry, 27700) AS geometry_27700
    from source
)

select *
from transformed
