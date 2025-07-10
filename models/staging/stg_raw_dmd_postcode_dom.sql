-- models/staging/stg_raw_dmd_postcode_dom.sql

with source as (
    select *
    from {{ source('raw_dmd_postcode_dom_source', 'raw_dmd_postcode_dom') }}
),

transformed as (
    select
        "Outcode" as postcode_outcode,
        "Postcode" as postcode,
        "Num_meters" as num_meters,
        "Total_cons_kwh" as total_dmd_kwh,
        "Mean_cons_kwh" as mean_dmd_kwh_p_meter,
        "Median_cons_kwh" as med_dmd_kwh_p_meter,
        "year"
    from source
),

geo_points as (
    select *
    from public.geo_points_uk_postcodes
),

joined as (
    select
        t.*,
        g.geometry,
        g.geometry_4326
    from transformed t
    left join geo_points g
    on t.postcode = g.postcode
)

select *
from joined
