-- models/staging/stg_lkup_fit_ft_mapping.sql

with source as (
    select * from {{ source('lkup_fit_ft_mapping_source','lkup_fit_ft_mapping') }}
)

select
    simple_ft,
    fit_ft
From source