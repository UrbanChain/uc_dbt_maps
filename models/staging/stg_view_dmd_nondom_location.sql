-- models/staging/stg_view_dmd_nondom_location.sql

with stg_raw_dmd_msoa_nondom as (
    select
        lacd,
        local_authority,
        msoa_code,
        msoa_name,
        meter_type,
        total_dmd_kwh,
        mean_dmd_kwh_p_meter,
        med_dmd_kwh_p_meter,
        yr,
        num_meters
    from {{ ref('stg_raw_dmd_msoa_nondom') }}
),
stg_geo_local_authority_districts as (
    select
        lad24cd,
        geometry_27700,
        geometry_4326
    from {{ ref('stg_geo_local_authority_districts') }}
),
joined_data as (
    select
        dmd.lacd,
        dmd.local_authority,
        dmd.msoa_code,
        dmd.msoa_name,
        dmd.meter_type,
        dmd.total_dmd_kwh,
        dmd.mean_dmd_kwh_p_meter,
        dmd.med_dmd_kwh_p_meter,
        dmd.yr,
        dmd.num_meters,
        geo.lad24cd,
        geo.geometry_27700,
        geo.geometry_4326
    from stg_raw_dmd_msoa_nondom dmd
    left join stg_geo_local_authority_districts geo
    on dmd.lacd = geo.lad24cd
)

select *
from joined_data