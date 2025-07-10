-- models/staging/stg_raw_dmd_msoa_nondom.sql

with source as (
    select *
    from {{ source('raw_dmd_msoa_nondom_source', 'raw_dmd_msoa_nondom') }}
),

transformed as
    (select
        "Local authority code" as lacd,
        "Local authority" as local_authority,
        "MSOA code" as msoa_code,
        "Middle layer super output area" as msoa_name,
        "Meter type" as meter_type,
        "Total consumption (kWh)" as total_dmd_kwh,
        "Mean consumption (kWh per meter)" as mean_dmd_kwh_p_meter,
        "Median consumption (kWh per meter)" as med_dmd_kwh_p_meter,
        "Year" as yr,
        "Number of meters" as num_meters
    from source
)

select *
from transformed