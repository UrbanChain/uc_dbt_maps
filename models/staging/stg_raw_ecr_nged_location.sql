-- models/staging/stg_raw_ecr_nged_location.sql


with stg_raw_ecr_nged_location as (
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
        poc_voltage_kv as poc_voltage,
        licence_area,
        chp_cogen_1_yes_no as chp_cogeneration_1,
        registered_cap_1,
        already_connected_cap_mw,
        connection_status,
        date_connected,
        date_accepted,
        target_energisation_date,
        in_queue_yes_no as in_connection_queue,
        energy_source_1,
        energy_source_2,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_nged') }}
)

select
    stg_raw_ecr_nged_location.*,
    geo_primary.name AS derived_pry_group,
    geo_primary.bsp_nrid_name AS derived_bsp_group,
    geo_primary.gsp_nrid_name AS derived_gsp_group,
    -- Add primary_boundary_used with the conditional logic
    coalesce(stg_raw_ecr_nged_location.primary_boundary, geo_primary.name) as primary_boundary_used,
    coalesce(stg_raw_ecr_nged_location.grid_supply_point, geo_primary.gsp_nrid_name) as gsp_boundary_used,
    coalesce(stg_raw_ecr_nged_location.already_connected_cap_mw, stg_raw_ecr_nged_location.registered_cap_1) as capacity_used
from
    stg_raw_ecr_nged_location
left join
    public.geo_nged_primary AS geo_primary
on
    ST_Within(
        ST_Transform(stg_raw_ecr_nged_location.geopoint, 4326),  -- Transform point to EPSG:4326
        geo_primary.geometry
    )