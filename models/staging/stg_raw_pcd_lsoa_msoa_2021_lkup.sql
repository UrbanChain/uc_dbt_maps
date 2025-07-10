-- models/staging/stg_raw_pcd_lsoa_msoa_2021_lkup_source.sql

with source as (
    select * from {{ source('raw_pcd_lsoa_msoa_2021_lkup_source', 'raw_pcd_lsoa_msoa_2021_lkup') }}
)

select
    *
from source
