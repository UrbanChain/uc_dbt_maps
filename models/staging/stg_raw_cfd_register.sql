-- models/staging/stg_raw_cfd_register.sql

with source as (
    select * from {{ source('raw_cfd_register_source', 'raw_cfd_register') }}
),

geometries as (
    select
        *,
        st_geomfromtext(
            'POLYGON((' ||
            -- West (longitude latitude)
            trim(split_part(split_part(cfd_geographical_coord, 'W:', 2), ',', 2)) || ' ' || trim(split_part(split_part(cfd_geographical_coord, 'W:', 2), ',', 1)) || ', ' ||
            -- North (longitude latitude)
            trim(split_part(split_part(cfd_geographical_coord, 'N:', 2), ',', 2)) || ' ' || trim(split_part(split_part(cfd_geographical_coord, 'N:', 2), ',', 1)) || ', ' ||
            -- East (longitude latitude)
            trim(split_part(split_part(cfd_geographical_coord, 'E:', 2), ',', 2)) || ' ' || trim(split_part(split_part(cfd_geographical_coord, 'E:', 2), ',', 1)) || ', ' ||
            -- South (longitude latitude)
            trim(split_part(split_part(cfd_geographical_coord, 'S:', 2), ',', 2)) || ' ' || trim(split_part(split_part(cfd_geographical_coord, 'S:', 2), ',', 1)) || ', ' ||
            -- Close the polygon by repeating West
            trim(split_part(split_part(cfd_geographical_coord, 'W:', 2), ',', 2)) || ' ' || trim(split_part(split_part(cfd_geographical_coord, 'W:', 2), ',', 1)) ||
            '))'
        ) as geometry
    from source
)

select
    {{ clean_value('"Name of CFD Unit (title)"') }} as cfd_unit_name,
    {{ clean_value('"cfd_unique_id"') }} as unique_identifier,
    {{ convert_to_datetime(clean_value('"start_date"')) }} as start_date,
    {{ convert_to_datetime(clean_value('"cfd_date_last_updated"')) }} as last_updated_date,
    {{ clean_value('"cfd_allocation_round"') }} as allocation_round,
    {{ clean_value('"cfd_technology_type"') }} as technology_type,
    {{ clean_value('"CFD Unit with Combined Heat and Power"') }} as combined_heat_power,
    {{ clean_value('"cfd_dual_scheme_cfd_unit"') }} as dual_scheme_plant,
    {{ clean_value('"cfd_capacity_whole_station"') }} as whole_station_capacity,
    "Initial Installed Capacity Estimate for CFD Unit" as initial_installed_capacity,
    "Any Reduction to the Capacity of a CFD Unit (field_cfd_reduction)" as capacity_reduction,
    "Initial Strike Price (field_cfd_strikeprice)" as initial_strike_price,
    "Current strike price (field_cfd_current_strikeprice)" as current_strike_price,
    "Reference price" as reference_price,
    "Change to Strike Price (field_cfd_change_strikeprice)" as strike_price_change,
    {{ convert_to_datetime(clean_value('"Target Commissioning Date (field_cfd_target_comm_date)"')) }} as target_commissioning_date,
    {{ convert_to_datetime(clean_value('"Target Commissioning Window Start Date (field_cfd_start_date_ta)"')) }} as commissioning_window_start,
    {{ convert_to_datetime(clean_value('"Generator’s Expected Start Date (field_cfd_generator_start_da)"')) }} as generator_expected_start,
    {{ clean_value('"Proposed start date comments (field_cfd_description_text_first)"') }} as start_date_comments,
    {{ clean_value('"Company Name (field_cfd_applicant_name)"') }} as company_name,
    {{ clean_value('"Country (field_cfd_applicant_address_country_code)"') }} as country,
    {{ clean_value('"Address 1 (field_cfd_applicant_address_address_line1)"') }} as address_line1,
    {{ clean_value('"Address 2 (field_cfd_applicant_address_address_line2)"') }} as address_line2,
    {{ clean_value('"Town/City (field_cfd_applicant_address_locality)"') }} as city,
    {{ clean_value('"County (field_cfd_applicant_address_administrative_area)"') }} as county,
    {{ clean_value('"Postcode (field_cfd_applicant_address_postal_code)"') }} as postal_code,
    {{ clean_value('"Company Registration No (field_cfd_company_reg_no)"') }} as company_registration_no,
    -- geometry,
    cfd_geographical_coord,
    {{ clean_value('"CFD Agreement Type (field_cfd_agreement_type)"') }} as agreement_type,
    {{ clean_value('"Version Name and Number of Standard Terms (field_cfd_vnumber_cf)"') }} as standard_terms_version,
    {{ clean_value('"Reference Number for Modification Agreement (field_cfd_ref_num_)"') }} as modification_agreement_reference,
    {{ convert_to_datetime(clean_value('"Date of Modification Agreement with LCCC (field_cfd_date_enteri)"')) }} as modification_agreement_date,
    {{ clean_value('"Type of Connection (field_cfd_type_connection)"') }} as connection_type,
    {{ clean_value('"Transmission or Distribution connection (field_cfd_connection_t)"') }} as transmission_or_distribution,
    {{ clean_value('"Licence Exempt Embedded or Licence Connected (field_cfd_license)"') }} as licence_status,
    {{ clean_value('"Single Metering or Apportioned Metering (field_cfd_apportioned_)"') }} as metering_type,
    {{ clean_value('"Offshore Leasing Round 2 or 3/Scottish Territorial Waters (fiel)"') }} as offshore_leasing_round,
    {{ convert_to_datetime(clean_value('"Termination Date (field_cfd_date_termination)"')) }} as termination_date
from
    --geometries
    source