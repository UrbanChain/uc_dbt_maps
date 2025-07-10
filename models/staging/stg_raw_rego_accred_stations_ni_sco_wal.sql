-- models/staging/stg_raw_accredited stations.sql

with source as (
    select * from {{ source('raw_rego_accred_stations_ni_sco_wal_source', 'raw_rego_accred_stations_ni_sco_wal') }}
)

select
    "GeneratorID" as generator_id,
    "StatusName" as status_name,
    "GeneratorName" as generator_name,
    "SchemeName" as scheme_name,
    "Capacity" as capacity,
    "Country" as country,
    "TechnologyName" as technology_name,
    "OutputType" as output_type,
    {{ convert_to_datetime(clean_value('"AccreditationDate"')) }} as accred_date,
    {{ convert_to_datetime(clean_value('"CommissionDate"')) }} as comm_date,
    "organization",
    "organization_address",
    "FaxNumber"as fax_num,
    CASE
        WHEN TRIM(CAST("generating_station_address" AS TEXT)) = '' THEN NULL
        ELSE "generating_station_address"
    END AS generating_station_address,
    {{ extract_uk_postcode('generating_station_address') }} AS generating_station_postcode
from source