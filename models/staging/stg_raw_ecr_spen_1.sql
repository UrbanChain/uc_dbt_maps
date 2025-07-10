-- models/staging/stg_raw_ecr_spen_1.sql

with source as (
    select * from {{ source('raw_ecr_spen_source', 'raw_ecr_spen_1') }}
)
-- ,
--
-- with_geopoint as (
--     select
--         *,
--         case
--             when {{ clean_value('"Location (Xcoordinate):\nEastings (where data is held)"') }} ~ '^[0-9]+(\.[0-9]+)?$'
--               and {{ clean_value('"Location (ycoordinate):\nNorthings (where data is held)"') }} ~ '^[0-9]+(\.[0-9]+)?$'
--             then ST_SetSRID(ST_MakePoint(
--                 {{ clean_value('"Location (Xcoordinate):\nEastings (where data is held)"') }}::double precision,
--                 {{ clean_value('"Location (ycoordinate):\nNorthings (where data is held)"') }}::double precision
--             ), 27700)
--             else null
--         end as geopoint
--     from source
-- ),
--
-- ranked_source as (
--     select *,
--            row_number() over (
--                partition by geopoint
--                order by "Last Updated" desc nulls last
--            ) as row_num
--     from with_geopoint
--     where geopoint is not null
-- ),
--
-- latest_per_geopoint as (
--     select *
--     from ranked_source
--     where row_num = 1
-- )

select
    {{ clean_value('"Export MPAN / MSID"') }} as export_mpan,
    {{ clean_value('"Import MPAN / MSID"') }} as import_mpan,
    {{ clean_value('"Customer Name"') }} as customer_name,
    {{ clean_value('"Customer Site"') }} as customer_site,
    {{ clean_value('"Address Line 1"') }} as address_line_1,
    {{ clean_value('"Address Line 2"') }} as address_line_2,
    {{ clean_value('"Town / City"') }} as town_city,
    {{ clean_value('"county"') }} as county,
    {{ clean_value('"postcode"') }} as postcode,
    {{ clean_value('"country"') }} as country,
    {{ clean_value('"Location (Xcoordinate):\nEastings (where data is held)"') }} as x_coordinate,
    {{ clean_value('"Location (ycoordinate):\nNorthings (where data is held)"') }} as y_coordinate,
--     geopoint,
    {{ clean_value('"Grid Supply Point"') }} as grid_supply_point,
    {{ clean_value('"Bulk Supply Point"') }} as bulk_supply_point,
    {{ clean_value('"Primary"') }} as primary_boundary,
    {{ clean_value('"Point of Connection (POC)\nVoltage (kV)"') }} as poc_voltage,
    {{ clean_value('"Licence Area"') }} as licence_area,
    {{ convert_to_text(clean_value('"Energy Source 1"')) }} as energy_source_1,
    {{ clean_value('"Energy Conversion Technology 1"') }} as energy_conversion_tech_1,
    {{ clean_value('"CHP Cogeneration (Yes/No)"') }} as chp_cogeneration_1,
    {{ clean_value('"Storage Capacity 1 (MWh)"') }} as storage_capacity_1_mwh,
    {{ clean_value('"Storage Duration 1 (Hours)"') }} as storage_duration_1_hours,
    {{ clean_value('"Energy Source & Energy Conversion Technology 1  Registered Capa"') }} as registered_cap_1,
    {{ convert_to_text(clean_value('"Energy Source 2"')) }} as energy_source_2,
    {{ clean_value('"Energy Conversion Technology 2"') }} as energy_conversion_tech_2,
    {{ clean_value('"CHP Cogeneration 2 (Yes/No)"') }} as chp_cogeneration_2,
    {{ clean_value('"Storage Capacity 2 (MWh)"') }} as storage_capacity_2_mwh,
    {{ clean_value('"Storage Duration 2 (Hours)"') }} as storage_duration_2_hours,
    {{ clean_value('"Energy Source & Energy Conversion Technology 2  Registered Capa"') }} as sourcetech_2,
    {{ convert_to_text(clean_value('"Energy Source 3"')) }} as energy_source_3,
    {{ clean_value('"Energy Conversion Technology 3"') }} as energy_conversion_tech_3,
    {{ clean_value('"CHP Cogeneration 3 (Yes/No)"') }} as chp_cogeneration_3,
    {{ clean_value('"Storage Capacity 3 (MWh)"') }} as storage_capacity_3_mwh,
    {{ clean_value('"Storage Duration 3 (Hours)"') }} as storage_duration_3_hours,
    {{ clean_value('"Energy Source & Energy Conversion Technology 3  Registered Capa"') }} as sourcetech_3,
    {{ clean_value('"Flexible Connection (Yes/No)"') }} as flexible_connection,
    {{ clean_value('"Connection Status"') }} as connection_status,
    {{ clean_value('"Already connected Registered Capacity (MW)"') }} as already_connected_capacity_mw,
    {{ clean_value('"Maximum Export Capacity (MW)"') }} as max_export_capacity_mw,
    {{ clean_value('"Maximum Export Capacity (MVA)"') }} as max_export_capacity_mva,
    {{ clean_value('"Maximum Import Capacity (MW)"') }} as max_import_capacity_mw,
    {{ clean_value('"Maximum Import Capacity (MVA)"') }} as max_import_capacity_mva,
    {{ convert_to_datetime(clean_value('"Date Connected"')) }} as date_connected,
    {{ clean_value('"Accepted to Connect Registered Capacity (MW)"') }} as accepted_to_connect_capacity_mw,
    {{ clean_value('"Change to Maximum Export Capacity (MW)"') }} as change_to_max_export_capacity_mw,
    {{ clean_value('"Change to Maximum Export Capacity (MVA)"') }} as change_to_max_export_capacity_mva,
    {{ clean_value('"Change to Maximum Import Capacity (MW)"') }} as change_to_max_import_capacity_mw,
    {{ clean_value('"Change to Maximum Import Capacity (MVA)"') }} as change_to_max_import_capacity_mva,
    {{ convert_to_datetime(clean_value('"Date Accepted"')) }} as date_accepted,
    {{ convert_to_datetime(clean_value('"Target Energisation Date"')) }} as target_energisation_date,
    {{ clean_value('"Distribution Service Provider (Y/N)"') }} as distribution_service_provider,
    {{ clean_value('"Transmission Service Provider (Y/N)"') }} as transmission_service_provider,
    {{ clean_value('"reference"') }} as reference,
    {{ clean_value('"In a Connection Queue (Y/N)"') }} as in_connection_queue,
    {{ clean_value('"Distribution Reinforcement Reference"') }} as distribution_reinforcement_reference,
    {{ clean_value('"Transmission Reinforcement Reference"') }} as transmission_reinforcement_reference,
    {{ convert_to_datetime(clean_value('"Last Updated"')) }} as last_updated,
    {{ clean_value('"Query Disconnection"') }} as query_disconnection,
    {{ clean_value('"Unique ID"') }} as unique_id,
    {{ convert_to_datetime(clean_value('"publish_date"')) }} as publish_date,
    {{ convert_to_datetime(clean_value('"asof_date"')) }} as asof_date
from latest_per_geopoint;
