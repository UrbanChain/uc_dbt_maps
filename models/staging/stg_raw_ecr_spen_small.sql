-- models/staging/stg_raw_ecr_spen_small.sql

with source as (
    select * from {{ source('raw_ecr_spen_source', 'raw_ecr_spen_small') }}
)

select
    {{ clean_value('"Site ID"') }} as site_id,
    "Export MPAN / MSID"::text as export_mpan,
    "Import MPAN / MSID"::text as import_mpan,
    {{ clean_value('"Town/City"') }} as town_city,
    {{ clean_value('"County"') }} as county,
    {{ clean_value('"Country"') }} as country,
    "X Eastings 1 km"::numeric as x_coordinate,
    "Y Northings 1 km"::numeric as y_coordinate,
    {{ clean_value('"Grid Supply Point"') }} as grid_supply_point,
    {{ clean_value('"Bulk Supply Point"') }} as bulk_supply_point,
    {{ clean_value('"Primary"') }} as primary_boundary,
    "Point of Connection (POC) Voltage (kV)"::text as poc_voltage,
    {{ clean_value('"Licence Area"') }} as licence_area,
    {{ clean_value('"Energy Source 1"') }}::text as energy_source_1,
    {{ clean_value('"Energy Conversion Technology 1"') }} as energy_tech_1,
    {{ clean_value('"CHP Cogeneration (Yes/No)"') }} as chp_cogeneration_1,
    {{ clean_value('"Storage Capacity 1 (MWh)"') }} as sto_capacity_1_mwh,
    {{ clean_value('"Storage Duration 1 (Hours)"') }} as sto_duration_1_hours,

    CASE
        WHEN ("Energy Source & Energy Conversion Technology 1 - Registered Cap"::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+$'
            THEN "Energy Source & Energy Conversion Technology 1 - Registered Cap"::numeric
        ELSE NULL
    END AS registered_cap_1,

    {{ convert_to_text(clean_value('"Energy Source 2"')) }} as energy_source_2,
    {{ clean_value('"Energy Conversion Technology 2"') }} as energy_tech_2,
    {{ clean_value('"CHP Cogeneration 2 (Yes/No)"') }} as chp_cogeneration_2_yes_no,
    {{ clean_value('"Storage Capacity 2 (MWh)"') }} as sto_cap_2_mwh,
    {{ clean_value('"Storage Duration 2 (Hours)"') }} as sto_dur_2_hours,

    CASE
        WHEN ("Energy Source & Energy Conversion Technology 2 - Registered Cap"::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+$'
            THEN "Energy Source & Energy Conversion Technology 2 - Registered Cap"::numeric
        ELSE NULL
    END AS registered_cap_2,

    {{ convert_to_text(clean_value('"Energy Source 3"')) }} as energy_source_3,
    {{ clean_value('"Energy Conversion Technology 3"') }} as energy_tech_3,
    {{ clean_value('"CHP Cogeneration 3 (Yes/No)"') }} as chp_cogeneration_3_yes_no,
    {{ clean_value('"Storage Capacity 3 (MWh)"') }} as sto_cap_3_mwh,
    {{ clean_value('"Storage Duration 3 (Hours)"') }} as sto_dur_3_hours,

    CASE
        WHEN ("Energy Source & Energy Conversion Technology 3 - Registered Cap"::TEXT) ~ '^[+-]?([0-9]*[.])?[0-9]+$'
            THEN "Energy Source & Energy Conversion Technology 3 - Registered Cap"::numeric
        ELSE NULL
    END AS registered_cap_3,

    {{ clean_value('"Flexible Connection (Yes/No)"') }} as flexible_connection_yes_no,
    {{ clean_value('"Connection Status"') }} as connection_status,
    -- Cast Registered Cap columns to double precision
    "Already connected Registered Capacity (MW)"::numeric as already_connected_cap_mw,
    {{ clean_value('"Maximum Export Capacity (MW)"') }} as maximum_export_cap_mw,
    {{ clean_value('"Maximum Export Capacity (MVA)"') }} as maximum_export_cap_mva,
    {{ clean_value('"Maximum Import Capacity (MW)"') }} as maximum_import_cap_mw,
    {{ clean_value('"Maximum Import Capacity (MVA)"') }} as maximum_import_cap_mva,
    {{ convert_to_datetime(clean_value('"Date Connected"')) }} as date_connected,
    "Accepted to Connect Registered Capacity (MW)"::numeric as accepted_to_connect_cap_mw,
    {{ clean_value('"Change to Maximum Export Capacity (MW)"') }} as change_to_max_export_cap_mw,
    {{ clean_value('"Change to Maximum Export Capacity (MVA)"') }} as change_to_max_export_cap_mva,
    {{ clean_value('"Change to Maximum Import Capacity (MW)"') }} as change_to_max_import_cap_mw,
    {{ clean_value('"Change to Maximum Import Capacity (MVA)"') }} as change_to_max_import_cap_mva,
    {{ convert_to_datetime(clean_value('"Date Accepted"')) }} as date_accepted,
    {{ clean_value('"Target Energisation Date"') }} as target_energisation_date,
    {{ clean_value('"Distribution Service Provider (Y/N)"') }} as dno_provider_yes_no,
    {{ clean_value('"Transmission Service Provider (Y/N)"') }} as tso_provider_yes_no,
    {{ clean_value('"Reference"') }} as reference,
    {{ clean_value('"In a Connection Queue (Y/N)"') }} as in_connection_queue,
    {{ clean_value('"Distribution Reinforcement Reference"') }} as distribution_reinforcement_reference,
    {{ clean_value('"Transmission Reinforcement Reference"') }} as transmission_reinforcement_reference,
    "Last Updated"::timestamp AS last_updated

from source
