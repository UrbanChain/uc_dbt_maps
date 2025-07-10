-- models/staging/stg_raw_cfd_auction_output.sql

with source as (
    select * from {{ source('raw_cfd_auction_output_source', 'raw_cfd_auction_output') }}
)

select
    {{ clean_value('"_id"') }} as id,
    {{ clean_value('"Auction"') }} as auction,
    {{ clean_value('"Version"') }} as version,
    {{ convert_to_datetime(clean_value('"Publication_Date"')) }} as pub_date,
    {{ clean_value('"Project_Name"') }} as project_name,
    {{ clean_value('"Developer"') }}  as developer_name,
    {{ clean_value('"Technology_Type"') }}  as cfd_technology_type,
    {{ clean_value('"Capacity_MW"') }}  as capacity_mw,
    {{ clean_value('"Strike_Price_GBP_Per_MWh"') }}  as strike_price_gbp_mwh,
    {{ clean_value('"Delivery_Year"') }}  as delivery_yr,
    {{ clean_value('"Homes_Powered"') }}  as homes_powered,
    {{ clean_value('"Region"') }}  as country
from
    source