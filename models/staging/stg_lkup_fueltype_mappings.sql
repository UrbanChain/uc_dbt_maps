-- models/staging/stg_lkup_fueltype_mappings.sql

with source as (
    select * from {{ source('lkup_fueltype_mappings_source', 'lkup_fueltype_mappings') }}
)

select
    source as source_name,
    mapping as simple_fueltype
From source