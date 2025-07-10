-- models/staging/stg_raw_fit_installations.sql

with source as (
    select * from {{ source('raw_fit_installations_source', 'raw_fit_installations') }}
),

latest_asof as (
    select max(asof_date) as max_asof_date from source
),

filtered_source as (
    select *
    from source
    where asof_date = (select max_asof_date from latest_asof)
)

select
    "Technology" as technology,
    "Installed capacity" as installed_capacity,
    "Declared net capacity" as declared_net_capacity,
    {{ convert_to_datetime(clean_value('"Application date"')) }} as application_date,
    {{ convert_to_datetime(clean_value('"Commissioning date"')) }} as commissioning_date,
    CASE
        WHEN {{ convert_to_datetime(clean_value('"Commissioning date"')) }} < '2010-04-01' THEN '2010-04-02'
        WHEN {{ convert_to_datetime(clean_value('"Commissioning date"')) }} > '2019-12-31' THEN '2019-12-30'
        ELSE {{ convert_to_datetime(clean_value('"Commissioning date"')) }}
    END AS fit_eff_commission_dt,
    {{ convert_to_datetime(clean_value('"MCS issue date"')) }} as mcs_issue_date,
    "Export status" as export_status,
    "TariffCode" as tariff_code,
    "Tariff Description" as tariff_description,
    "Installation Type" as installation_type,
    "Installation Country" as installation_country,
    "Government Office Region" as government_office_region,
    "Local Authority" as local_authority,
    "Constituency" as constituency,
    "Accreditation Route" as accreditation_route,
    "Extension (Y/N)" as extension_flag,
    "Community school category" as community_school_category,
    "MPAN Prefix" as mpan_prefix,
    CASE
        WHEN TRIM(CAST("PostCode" AS TEXT)) = '' THEN NULL
        ELSE "PostCode"
    END AS postcode_sector,
    "LLSOA Code" as llsoa_code,
    "MLSOA Code" as mlsoa_code,
    asof_date
from filtered_source

