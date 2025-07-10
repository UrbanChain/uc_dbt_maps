-- models/staging/stg_raw_ecr_enwl_location.sql

with stg_raw_ecr_enwl_1_location as (
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
        registered_cap_1,
        already_connected_registered_capacity,
        connection_status,
        date_connected,
        date_accepted,
        target_energisation_date,
        in_connection_queue,
        energy_source_1,
        energy_source_2,
        last_updated,
        -- Create geopoint in EPSG:27700 (British National Grid)
        st_setsrid(st_makepoint(x_coordinate::float, y_coordinate::float), 27700) as geopoint
    from {{ ref('stg_raw_ecr_enwl_1') }}
),

stg_raw_ecr_enwl_2_location as (
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
        registered_cap_1,
        already_connected_registered_capacity,
        connection_status,
        date_connected,
        date_accepted,
        target_energisation_date,
        in_connection_queue,
        energy_source_1,
        energy_source_2,
        last_updated,
        -- Create geopoint in EPSG:27700 (British National Grid)
        st_setsrid(st_makepoint(x_coordinate::float, y_coordinate::float), 27700) as geopoint
    from {{ ref('stg_raw_ecr_enwl_2') }}
),

-- Combine both sources into one table
combined_location as (
    select * from stg_raw_ecr_enwl_1_location
    union all
    select * from stg_raw_ecr_enwl_2_location
)

-- Join with geo_enwl_primary to get pry_group, bsp_group, and gsp_group
select
    combined_location.*,
    geo_primary.pry_group AS derived_pry_group,
    geo_primary.bsp_group AS derived_bsp_group,
    geo_primary.gsp_group AS derived_gsp_group,
    -- Add primary_boundary_used with the conditional logic
    coalesce(combined_location.primary_boundary, geo_primary.pry_group) as primary_boundary_used,
    coalesce(combined_location.grid_supply_point, geo_primary.gsp_group) as gsp_boundary_used,
    coalesce(combined_location.registered_cap_1, combined_location.already_connected_registered_capacity) as capacity_used
from
    combined_location
left join
    public.geo_enwl_primary AS geo_primary
on
    ST_Within(
        ST_Transform(combined_location.geopoint, 4326),  -- Transform point to EPSG:4326
        geo_primary.geometry
    )
