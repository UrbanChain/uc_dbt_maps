-- models/staging/stg_raw_ccl_accredited_stations.sql

with source as (
    select * from {{ source('raw_ccl_accred_stations_source', 'raw_ccl_accred_stations') }}
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
    "FaxNumber" as fax_num,
    "generating_station_address",
    {{ extract_uk_postcode('generating_station_address') }} as generating_station_postcode
from source