-- models/staging/stg_view_geo_gsp.sql

with stg_geo_enwl_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_enwl_gsp') }}
),

stg_geo_npgl_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_npgl_gsp') }}
),

stg_geo_spen_spd_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_spen_spd_gsp') }}
),

stg_geo_spen_spm_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_spen_spm_gsp') }}
),

stg_geo_ssen_sepd_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_ssen_sepd_gsp') }}
),

stg_geo_ssen_shepd_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_ssen_shepd_gsp') }}
),

stg_geo_ukpn_gsp as (
    select
        licence_ar,
        gsp_name,
        geometry_4326,
        geometry_27700
    from {{ ref('stg_geo_ukpn_gsp') }}
),

combined_location as (
    select * from stg_geo_enwl_gsp
    union all
    select * from stg_geo_npgl_gsp
    union all
    select * from stg_geo_spen_spd_gsp
    union all
    select * from stg_geo_spen_spm_gsp
    union all
    select * from stg_geo_ssen_sepd_gsp
    union all
    select * from stg_geo_ssen_shepd_gsp
    union all
    select * from stg_geo_ukpn_gsp
)

SELECT
    combined_location.*
FROM
    combined_location