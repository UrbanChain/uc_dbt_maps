-- models/staging/stg_raw_ecr_ukpn_location.sql

with stg_raw_ecr_ukpn_1_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        poc_voltage,
        licence_area,
        chp_cogeneration_1,
        -- Ensure consistency with double precision
        case
            when registered_cap_1::text = 'Redacted' then NULL
            else registered_cap_1::double precision
        end as registered_cap_1,
        case
            when already_connected_capacity_mw::text = 'Redacted' then NULL
            else already_connected_capacity_mw::double precision
        end as already_connected_capacity_mw,
        connection_status,
        date_connected,
        date_accepted,
        target_energisation_date,
        in_connection_queue,
        energy_source_1,
        energy_source_2,
        last_updated,
        st_setsrid(st_makepoint(x_coordinate::float, y_coordinate::float), 27700) as geopoint
    from {{ ref('stg_raw_ecr_ukpn_1') }}
),

stg_raw_ecr_ukpn_2_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        poc_voltage,
        licence_area,
        chp_cogeneration_1,
        -- Ensure consistency with double precision
        case
            when registered_cap_1::text = 'Redacted' then NULL
            else registered_cap_1::double precision
        end as registered_cap_1,
        case
            when already_connected_capacity_mw::text = 'Redacted' then NULL
            else already_connected_capacity_mw::double precision
        end as already_connected_capacity_mw,
        connection_status,
        date_connected,
        date_accepted,
        target_energisation_date,
        in_connection_queue,
        energy_source_1,
        energy_source_2,
        last_updated,
        st_setsrid(st_makepoint(x_coordinate::float, y_coordinate::float), 27700) as geopoint
    from {{ ref('stg_raw_ecr_ukpn_2') }}
),

combined_location as (
    select * from stg_raw_ecr_ukpn_1_location
    union all
    select * from stg_raw_ecr_ukpn_2_location
)

-- Join with geo_enwl_primary to get pry_group, bsp_group, and gsp_group
select
    combined_location.*,
    geo_primary.primarysubstationname AS derived_pry_group,
    geo_primary.grid_site AS derived_grid_site_group,
    geo_primary.grid_supply_point AS derived_gsp_group,
    coalesce(combined_location.primary_boundary, geo_primary.primarysubstationname) as primary_boundary_used,
    coalesce(combined_location.already_connected_capacity_mw, combined_location.registered_cap_1) as capacity_used
from
    combined_location
left join
    public.geo_ukpn_primary AS geo_primary
on
    ST_Within(
        ST_Transform(combined_location.geopoint, 4326),  -- Transform point to EPSG:4326
        geo_primary.geometry
    )