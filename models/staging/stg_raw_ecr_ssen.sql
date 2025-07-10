-- models/staging/stg_raw_ecr_ssen.sql

with source as (
    select * from {{ source('raw_ecr_ssen_source', 'raw_ecr_ssen') }}
)

select
    {{ clean_value('"Export MPAN / MSID"') }} as export_mpan,
    {{ clean_value('"Import MPAN / MSID"') }} as import_mpan,
    {{ clean_value('"Customer Name "') }} as customer_name,
    {{ clean_value('"Customer Site "') }} as customer_site,
    {{ clean_value('"Address Line 1"') }} as address_line_1,
    {{ clean_value('"Address Line 2"') }} as address_line_2,
    {{ clean_value('"Town/ City "') }} as town_city,
    {{ clean_value('"County "') }} as county,
    {{ clean_value('"Postcode "') }} as postcode,
    {{ clean_value('"Country"') }} as country,
    {{ clean_value('"location_x_eastings"') }} as x_coordinate,
    {{ clean_value('"location_y_northings"') }} as y_coordinate,
    {{ clean_value('"Grid Supply Point"') }} as grid_supply_point,
    {{ clean_value('"Bulk Supply Point"') }} as bulk_supply_point,
    {{ clean_value('"Primary"') }} as primary_boundary,
    {{ clean_value('"Point of Connection Voltage (kV)"') }} as poc_voltage,
    {{ clean_value('"Licence Area "') }} as licence_area,
    {{ convert_to_text(clean_value('"Energy Source 1"')) }} as energy_source_1,
    {{ clean_value('"Energy Conversion Technology 1"') }} as energy_tech_1,
    {{ clean_value('"CHP Cogeneration (Yes/No)"') }} as chp_cogeneration_1,
    {{ clean_value('"Storage Capacity 1 (MWh)"') }} as sto_capacity_1_mwh,
    {{ clean_value('"Storage Duration 1 (Hours)"') }} as sto_duration_1_hours,

    -- Cast Registered Cap columns to double precision
    CAST({{ clean_value('"Energy Source & Energy Conversion Technology 1 - Registered Cap"') }} AS DOUBLE PRECISION) as registered_cap_1,
    {{ convert_to_text(clean_value('"Energy Source 2"')) }} as energy_source_2,
    {{ clean_value('"Energy Conversion Technology 2"') }} as energy_tech_2,
    {{ clean_value('"CHP Cogeneration 2 (Yes/No)"') }} as chp_cogeneration_2_yes_no,
    {{ clean_value('"Storage Capacity 2 (MWh)"') }} as sto_cap_2_mwh,
    {{ clean_value('"Storage Duration 2 (Hours)"') }} as sto_dur_2_hours,

    -- Cast Registered Cap columns to double precision
    CAST({{ clean_value('"Energy Source & Energy Conversion Technology 2 - Registered Cap"') }} AS DOUBLE PRECISION) as energy_source_2_cap,
    {{ convert_to_text(clean_value('"Energy Source 3"')) }} as energy_source_3,
    {{ clean_value('"Energy Conversion Technology 3"') }} as energy_tech_3,
    {{ clean_value('"CHP Cogeneration 3 (Yes/No)"') }} as chp_cogeneration_3_yes_no,
    {{ clean_value('"Storage Capacity 3 (MWh)"') }} as sto_cap_3_mwh,
    {{ clean_value('"Storage Duration 3 (Hours)"') }} as sto_dur_3_hours,

    -- Cast Registered Cap columns to double precision
    CAST({{ clean_value('"Energy Source & Energy Conversion Technology 3 - Registered Cap"') }} AS DOUBLE PRECISION) as energy_source_3_cap,

    {{ clean_value('"Flexible Connection (Yes/No)"') }} as flexible_connection_yes_no,
    {{ clean_value('"Connection Status "') }} as connection_status,
    -- Cast Registered Cap columns to double precision
    CAST({{ clean_value('"Already connected Registered Capacity (MW)"') }} AS DOUBLE PRECISION) as already_connected_cap_mw,

    {{ clean_value('"Maximum Export Capacity (MW)"') }} as maximum_export_cap_mw,
    {{ clean_value('"Maximum Export Capacity (MVA)"') }} as maximum_export_cap_mva,
    {{ clean_value('"Maximum Import Capacity (MW)"') }} as maximum_import_cap_mw,
    {{ clean_value('"Maximum Import Capacity (MVA)"') }} as maximum_import_cap_mva,
    {{ convert_to_datetime(clean_value('"Date Connected"')) }} as date_connected,
    {{ clean_value('"Accepted to Connect Registered Capacity (MW)"') }} as accepted_to_connect_cap_mw,
    {{ clean_value('"Change to Maximum Export Capacity (MW) "') }} as change_to_max_export_cap_mw,
    {{ clean_value('"Change to Maximum Export Capacity (MVA) "') }} as change_to_max_export_cap_mva,
    {{ clean_value('"Change to Maximum Import Capacity (MW) "') }} as change_to_max_import_cap_mw,
    {{ clean_value('"Change to Maximum Import Capacity (MVA) "') }} as change_to_max_import_cap_mva,
    {{ convert_to_datetime(clean_value('"Date Accepted"')) }} as date_accepted,
    {{ clean_value('"Target Energisation Date"') }} as target_energisation_date,
    {{ clean_value('"Distribution Service Provider (Y/N)"') }} as dno_provider_yes_no,
    {{ clean_value('"Transmission Service Provider (Y/N)"') }} as tso_provider_yes_no,
    {{ clean_value('"Reference"') }} as reference,
    {{ clean_value('"In a Connection Queue (Y/N)"') }} as in_connection_queue,
    {{ clean_value('"Distribution Reinforcement Reference"') }} as distribution_reinforcement_reference,
    {{ clean_value('"Transmission Reinforcement Reference"') }} as transmission_reinforcement_reference,
    {{ convert_to_datetime(clean_value('"Last Updated"')) }} as last_updated

from source
