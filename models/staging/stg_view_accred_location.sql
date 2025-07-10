-- models/staging/stg_view_accred_location.sql

with stg_raw_ccl_accred_stations as (
    select
        generator_id,
        status_name,
        generator_name,
        scheme_name,
        capacity,
        country,
        technology_name,
        output_type,
        accred_date,
        comm_date,
        organization,
        organization_address,
        fax_num,
        generating_station_address,
        generating_station_postcode,
        {{ clean_uk_postcode('generating_station_postcode') }} AS cln_gen_postcode
    from {{ ref('stg_raw_ccl_accred_stations') }}
),
stg_raw_rego_accred_stations_england as (
    select
        generator_id,
        status_name,
        generator_name,
        scheme_name,
        capacity,
        country,
        technology_name,
        output_type,
        accred_date,
        comm_date,
        organization,
        organization_address,
        fax_num,
        generating_station_address,
        generating_station_postcode,
        {{ clean_uk_postcode('generating_station_postcode') }} AS cln_gen_postcode
    from {{ ref('stg_raw_rego_accred_stations_england') }}
),

stg_raw_rego_accred_stations_ni_sco_wal as (
    select
        generator_id,
        status_name,
        generator_name,
        scheme_name,
        capacity,
        country,
        technology_name,
        output_type,
        accred_date,
        comm_date,
        organization,
        organization_address,
        fax_num,
        generating_station_address,
        generating_station_postcode,
        {{ clean_uk_postcode('generating_station_postcode') }} AS cln_gen_postcode
    from {{ ref('stg_raw_rego_accred_stations_ni_sco_wal') }}
),

stg_raw_ro_accredited_stations as (
    select
        generator_id,
        status_name,
        generator_name,
        scheme_name,
        capacity,
        country,
        technology_name,
        output_type,
        accred_date,
        comm_date,
        organization,
        organization_address,
        fax_num,
        generating_station_address,
        generating_station_postcode,
        {{ clean_uk_postcode('generating_station_postcode') }} AS cln_gen_postcode
    from {{ ref('stg_raw_ro_accredited_stations') }}
),

combined_location as (
    select * from stg_raw_ccl_accred_stations
    union all
    select * from stg_raw_rego_accred_stations_england
    union all
    select * from stg_raw_rego_accred_stations_ni_sco_wal
    union all
    select * from stg_raw_ro_accredited_stations
)

-- Final query with spatial join
select
    combined_location.*
from
    combined_location
