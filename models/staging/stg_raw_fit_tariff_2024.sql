-- models/staging/stg_raw_fit_tariff_2024.sql

with source as (
    select * from {{ source('raw_fit_tariff_2024_source', 'raw_fit_tariff_2024') }}
)

select
    "Technology Type" as tech_type,
    "Solar PV Installation Type" as pv_type,
    "Minimum Capacity (kW)" as min_cap,
    "Maximum Capacity (kW)" as max_cap,
    "Energy Efficiency Requirement rating" as efficiency_rating,
    "Tariff" as tariff,
    {{ convert_to_datetime(clean_value('"Tariff Start Date"'))}} as tariff_start_dt,
    {{ convert_to_datetime(clean_value('"Tariff End Date"'))}} as tariff_end_dt,
    reference,
    tariff_source,
    {{ convert_to_datetime(clean_value("effective_to"))}} as effective_to,
    fix_export_tariff as fit_export_tariff,
    '30-Jan-2024' as published_on
from source