-- models/staging/stg_raw_ecr_spen_location.sql

with stg_raw_ecr_spen_1_location as (
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
        already_connected_cap_mw,
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
    from {{ ref('stg_raw_ecr_spen') }}
),

stg_raw_ecr_spen_2_location as (
    select
        export_mpan,
        import_mpan,
        NULL as customer_name,   -- Add NULL for missing columns
        NULL as customer_site,
        NULL as town_city,
        NULL as address_line_1,
        NULL as postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        poc_voltage,
        licence_area,
        chp_cogeneration_1,
        registered_cap_1,
        already_connected_cap_mw,
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
    from {{ ref('stg_raw_ecr_spen_small') }}
),

-- Combine both sources into one table
combined_location as (
    select * from stg_raw_ecr_spen_1_location
    union all
    select * from stg_raw_ecr_spen_2_location
)

-- Final SELECT to produce the view from combined_location
select
    combined_location.*,
    geo_primary.primary_group AS derived_pry_spm_group,
    geo_primary_1.primary_substation AS derived_pry_spd_group,
    -- Add primary_boundary_used with the conditional logic
    coalesce(combined_location.primary_boundary, geo_primary.primary_group,geo_primary_1.primary_substation) as primary_boundary_used,
    coalesce(combined_location.already_connected_cap_mw, combined_location.registered_cap_1) as capacity_used
from
    combined_location
left join
    {{ ref('stg_geo_spen_spm_primary') }} AS geo_primary

on
    ST_Within(
        ST_Transform(combined_location.geopoint, 4326),  -- Transform point to EPSG:4326
        geo_primary.geometry_4326
    )
left join
    {{ ref('stg_geo_spen_spd_primary') }} AS geo_primary_1

on
    ST_Within(
        ST_Transform(combined_location.geopoint, 4326),  -- Transform point to EPSG:4326
        geo_primary_1.geometry_4326
    )