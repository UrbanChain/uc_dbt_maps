-- models/staging/stg_raw_ecr_enwl_1_location.sql

with source as (
    select
        {{ clean_value('"Export MPAN / MSID"') }} as export_mpan_msid,
        {{ clean_value('"Import MPAN / MSID"') }} as import_mpan_msid,
        {{ clean_value('"Customer Name"') }} as customer_name,
        {{ clean_value('"Customer Site"') }} as customer_site,
        {{ clean_value('"Town/ City"') }} as town_city,
        {{ clean_value('"Address Line 1"') }} as address_line_1,
        {{ clean_value('"Postcode"') }} as postcode,
        {{ clean_value('"County"') }} as county,
        {{ clean_value('"country"') }} as country,
        {{ clean_value('"Grid Supply Point"') }} as grid_supply_point,
        {{ clean_value('"Bulk Supply Point"') }} as bulk_supply_point,
        {{ clean_value('"Primary"') }} as primary_boundary,
        "Point of Connection (POC) Voltage (kV)" as poc,
        {{ clean_value('"Licence Area"') }} as licence_area,
        {{ clean_value('"CHP Cogeneration (Yes/No)"') }} as chp_cogeneration_yes_no,
        "Energy Source & Energy Conversion Technology 1 - Registered Cap" as registered_cap_1,
        {{ clean_value('"Connection Status"') }} as connection_status,
        {{ clean_value('"Date Connected"') }} as date_connected,
        {{ clean_value('"Date Accepted"') }} as date_accepted,
        {{ clean_value('"Target Energisation Date"') }} as target_energisation_date,
        {{ clean_value('"In a Connection Queue (Y/N)"') }} as in_a_connection_queue_y_n,
        {{ clean_value('"Energy Source 1"') }} as energy_source_1_cln,
        {{ clean_value('"Energy Source 1"') }} as energy_source_1,
        {{ clean_value('"Energy Source 2"') }} as energy_source_2_cln,
        {{ clean_value('"Energy Source 2"') }} as energy_source_2,
        "Location (X-coordinate): Eastings (where data is held)" as x_coordinate,
        "Location (y-coordinate): Northings (where data is held)" as y_coordinate
    from {{ ref('stg_raw_ecr_enwl_1') }}
)

select
    export_mpan_msid,
    import_mpan_msid,
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
    poc,
    licence_area,
    chp_cogeneration_yes_no,
    registered_cap_1,
    connection_status,
    date_connected,
    date_accepted,
    target_energisation_date,
    in_a_connection_queue_y_n,
    energy_source_1_cln,
    energy_source_1,
    energy_source_2_cln,
    energy_source_2,
    -- Create a geographic point using the x and y coordinates
    st_setsrid(st_makepoint(x_coordinate::float, y_coordinate::float), 27700) as geopoint
from source;
