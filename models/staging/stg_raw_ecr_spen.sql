-- models/staging/stg_raw_ecr_spen.sql

with source as (
    select * from {{ source('raw_ecr_spen_source', 'raw_ecr_spen') }}
)

select
    {{ clean_value('"export_mpan_msid"') }} as export_mpan,
    {{ clean_value('"import_mpan_msid"') }} as import_mpan,
    {{ clean_value('"customer_name"') }} as customer_name,
    {{ clean_value('"customer_site"') }} as customer_site,
    {{ clean_value('"address_line"') }} as address_line_1,
    {{ clean_value('"address_line_2"') }} as address_line_2,
    {{ clean_value('"town_city"') }} as town_city,
    {{ clean_value('"county"') }} as county,
    {{ clean_value('"postcode"') }} as postcode,
    {{ clean_value('"country"') }} as country,
    {{ clean_value('"location_xcoordinate_eastings_where_data_is_held"') }} as x_coordinate,
    {{ clean_value('"location_ycoordinate_northings_where_data_is_held"') }} as y_coordinate,
    {{ clean_value('"grid_supply_point"') }} as grid_supply_point,
    {{ clean_value('"bulk_supply_point"') }} as bulk_supply_point,
    {{ clean_value('"primary"') }} as primary_boundary,
    {{ clean_value('"point_of_connection_poc_voltage_kv"') }} as poc_voltage,
    {{ clean_value('"licence_area"') }} as licence_area,
    {{ convert_to_text(clean_value('"energy_source"')) }} as energy_source_1,
    {{ clean_value('"energy_conversion_technology"') }} as energy_tech_1,
    {{ clean_value('"chp_cogeneration_yes_no"') }} as chp_cogeneration_1,
    {{ clean_value('"storage_capacity_1_mwh"') }} as sto_capacity_1_mwh,
    {{ clean_value('"storage_duration_1_hours"') }} as sto_duration_1_hours,

    -- Cast Registered Cap columns to double precision
    "energy_source_energy_conversion_technology_1_registered_capacit"::numeric as registered_cap_1,
    {{ convert_to_text(clean_value('"energy_source_2"')) }} as energy_source_2,
    {{ clean_value('"energy_conversion_technology_2"') }} as energy_tech_2,
    {{ clean_value('"chp_cogeneration_2_yes_no"') }} as chp_cogeneration_2_yes_no,
    {{ clean_value('"storage_capacity_2_mwh"') }} as sto_cap_2_mwh,
    {{ clean_value('"storage_duration_2_hours"') }} as sto_dur_2_hours,

    -- Cast Registered Cap columns to double precision
    "energy_source_energy_conversion_technology_2_registered_capacit"::numeric as energy_source_2_cap,
    {{ convert_to_text(clean_value('"energy_source_3"')) }} as energy_source_3,
    {{ clean_value('"energy_conversion_technology_3"') }} as energy_tech_3,
    {{ clean_value('"chp_cogeneration_3_yes_no"') }} as chp_cogeneration_3_yes_no,
    "storage_capacity_3_mwh"::numeric as sto_cap_3_mwh,
    "storage_duration_3_hours"::numeric as sto_dur_3_hours,

    -- Cast Registered Cap columns to double precision
    "energy_source_energy_conversion_technology_3_registered_capacit"::numeric as energy_source_3_cap,

    {{ clean_value('"flexible_connection_yes_no"') }} as flexible_connection_yes_no,
    {{ clean_value('"connection_status"') }} as connection_status,
    -- Cast Registered Cap columns to double precision
    {{ clean_value('"already_connected_registered_capacity_mw"') }}::numeric as already_connected_cap_mw,

    {{ clean_value('"maximum_export_capacity_mw"') }} as maximum_export_cap_mw,
    {{ clean_value('"maximum_export_capacity_mva"') }} as maximum_export_cap_mva,
    {{ clean_value('"maximum_import_capacity_mw"') }} as maximum_import_cap_mw,
    {{ clean_value('"maximum_import_capacity_mva"') }} as maximum_import_cap_mva,
    {{ convert_to_datetime(clean_value('"date_connected"')) }} as date_connected,
    {{ clean_value('"accepted_to_connect_registered_capacity_mw"') }} as accepted_to_connect_cap_mw,
    {{ clean_value('"change_to_maximum_export_capacity_mw"') }} as change_to_max_export_cap_mw,
    {{ clean_value('"change_to_maximum_export_capacity_mva"') }} as change_to_max_export_cap_mva,
    {{ clean_value('"change_to_maximum_import_capacity_mw"') }} as change_to_max_import_cap_mw,
    {{ clean_value('"change_to_maximum_import_capacity_mva"') }} as change_to_max_import_cap_mva,
    {{ convert_to_datetime(clean_value('"date_accepted"')) }} as date_accepted,
    {{ clean_value('"target_energisation_date"') }} as target_energisation_date,
    {{ clean_value('"distribution_service_provider_y_n"') }} as dno_provider_yes_no,
    {{ clean_value('"transmission_service_provider_y_n"') }} as tso_provider_yes_no,
    {{ clean_value('"reference"') }} as reference,
    {{ clean_value('"in_a_connection_queue_y_n"') }} as in_connection_queue,
    {{ clean_value('"distribution_reinforcement_reference"') }} as distribution_reinforcement_reference,
    {{ clean_value('"transmission_reinforcement_reference"') }} as transmission_reinforcement_reference,
    {{ convert_to_datetime(clean_value('"last_updated"')) }} as last_updated

from source
