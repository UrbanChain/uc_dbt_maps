with source as (
    select * from {{ source('raw_ecr_npgl_1_source', 'raw_ecr_npgl_1') }}
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

    -- Use CASE WHEN to explicitly handle empty strings and "DATA NOT AVAILABLE"
    "Location (X-coordinate): Eastings (where data is held)"::integer as x_coordinate,
    "Location (y-coordinate): Northings (where data is held)"::integer as y_coordinate,

    {{ clean_value('"Grid Supply Point"') }} as grid_supply_point,
    {{ clean_value('"Bulk Supply Point"') }} as bulk_supply_point,
    {{ clean_value('"Primary"') }} as primary_boundary,

    "Point of Connection (POC) Voltage (kV)" as poc_voltage,

    {{ clean_value('"Licence Area "') }} as licence_area,
    {{ convert_to_text(clean_value('"Energy Source 1"')) }} as energy_source_1,

    "Energy Conversion Technology 1" as energy_conversion_tech_1,
    {{ clean_value('"CHP Cogeneration (Yes/No)"') }} as chp_cogeneration_1,

    case
        when LOWER({{ clean_value('"Storage Capacity 1 (MWh)"') }}) IN ('', 'data not available')
        then NULL
        else {{ clean_value('"Storage Capacity 1 (MWh)"') }}::real
    end as storage_capacity_1_mwh,
    case
        when LOWER({{ clean_value('"Storage Duration 1 (Hours)"') }}) IN ('', 'data not available')
        then NULL
        else {{ clean_value('"Storage Duration 1 (Hours)"') }}::real
    end as storage_duration_1_hours,

    "Energy Source & Energy Conversion Technology 1 - Registered Cap" as registered_cap_1,
    {{ convert_to_text(clean_value('"Energy Source 2"')) }} as energy_source_2,
    {{ clean_value('"Energy Conversion Technology 2"') }} as energy_conversion_tech_2,
    {{ clean_value('"CHP Cogeneration 2 (Yes/No)"') }} as chp_cogeneration_2,

    case
        when LOWER({{ clean_value('"Storage Capacity 2 (MWh)"') }}) IN ('', 'data not available')
        then NULL
        else {{ clean_value('"Storage Capacity 2 (MWh)"') }}::real
    end as storage_capacity_2_mwh,

    case
        when LOWER({{ clean_value('"Storage Duration 2 (Hours)"') }}) IN ('', 'data not available')
        then NULL
        else {{ clean_value('"Storage Duration 2 (Hours)"') }}::real
    end as storage_duration_2_hours,


    {{ clean_value('"Energy Source & Energy Conversion Technology 2 - Registered Cap"') }} as energy_conversion_tech_2_cap,
    {{ convert_to_text(clean_value('"Energy Source 3"')) }} as energy_source_3,
    {{ clean_value('"Energy Conversion Technology 3"') }} as energy_conversion_tech_3,
    {{ clean_value('"CHP Cogeneration 3 (Yes/No)"') }} as chp_cogeneration_3,

    case
        when LOWER({{ clean_value('"Storage Capacity 3 (MWh)"') }}) IN ('', 'data not available')
        then NULL
        else {{ clean_value('"Storage Capacity 3 (MWh)"') }}::real
    end as storage_capacity_3_mwh,

    case
        when LOWER({{ clean_value('"Storage Duration 3 (Hours)"') }}) IN ('', 'data not available')
        then NULL
        else {{ clean_value('"Storage Duration 3 (Hours)"') }}::real
    end as storage_duration_3_hours,

    {{ clean_value('"Energy Source & Energy Conversion Technology 3 - Registered Cap"') }} as energy_conversion_tech_3_cap,
    {{ clean_value('"Flexible Connection (Yes/No)"') }} as flexible_connection,
    {{ clean_value('"Connection Status "') }} as connection_status,

    "Already connected Registered Capacity (MW) " as already_connected_capacity_mw,
    "Maximum Export Capacity (MW)" as max_export_capacity_mw,
    "Maximum Export Capacity (MVA)" as max_export_capacity_mva,
    "Maximum Import Capacity (MW)" as max_import_capacity_mw,

    case
        when LOWER("Maximum Import Capacity (MVA)") IN ('', 'data not available')
        then NULL
        else "Maximum Import Capacity (MVA)"::real
    end as max_import_capacity_mva,

    {{ convert_to_datetime(clean_value('"Date Connected"')) }} as date_connected,

    "Accepted to Connect Registered Capacity (MW)" as accepted_to_connect_capacity_mw,
    "Change to Maximum Export Capacity (MW) " as change_to_max_export_capacity_mw,
    "Change to Maximum Export Capacity (MVA) " as change_to_max_export_capacity_mva,
    "Change to Maximum Import Capacity (MW) " as change_to_max_import_capacity_mw,
    "Change to Maximum Import Capacity (MVA) " as change_to_max_import_capacity_mva,

    {{ convert_to_datetime(clean_value('"Date Accepted"')) }} as date_accepted,
    {{ convert_to_datetime(clean_value('"Target Energisation Date"')) }} as target_energisation_date,
    {{ clean_value('"Distribution Service Provider (Y/N)"') }} as distribution_service_provider,
    {{ clean_value('"Transmission Service Provider (Y/N)"') }} as transmission_service_provider,
    {{ clean_value('"Reference"') }} as reference,
    {{ clean_value('"In a Connection Queue (Y/N)"') }} as in_connection_queue,
    {{ clean_value('"Distribution Reinforcement Reference"') }} as distribution_reinforcement_reference,
    {{ clean_value('"Transmission Reinforcement Reference"') }} as transmission_reinforcement_reference,
    {{ convert_to_datetime(clean_value('"Last Updated"')) }} as last_updated,
    {{ clean_value('"geopoint"') }} as geopoint
from source
