-- models/staging/stg_raw_epc_scotland.sql

with source as (
    select * from {{ source('raw_epc_scotland_source', 'raw_epc_scotland') }}
)

select
    "BUILDING_REFERENCE_NUMBER" as Property_UPRN,
    "OSG_REFERENCE_NUMBER" as OSG_UPRN,
    "ADDRESS1" as Address1,
    "ADDRESS2" as Address2,
    "POST_TOWN" as "Post Town",
    "POSTCODE" as Postcode,
    {{ convert_to_datetime(clean_value('"INSPECTION_DATE"')) }} as "Date of Assessment",
    {{ convert_to_datetime(clean_value('"LODGEMENT_DATE"')) }} as "Date of Certificate",
    "PROPERTY_TYPE_SHORT" as "Property Short Description",
    "PROPERTY_TYPE"	as "Property Type",
    "FLOOR_AREA" as "Total floor area (m²)",
    "CURRENT_ENERGY_PERFORMANCE_RATING" as	"Current Energy Performance Rating",
    "CURRENT_ENERGY_PERFORMANCE_BAND"	as "Energy Band",
    "POTENTIAL_ENERGY_PERFORMANCE_RATING" as "Potential Energy Performance Rating",
    "POTENTIAL_ENERGY_PERFORMANCE_BAND" as "Potential Band",
    "NEW_BUILD_ENERGY_PERFORMANCE_RATING" as"New Build Benchmark Rating",
    "NEW_BUILD_ENERGY_PERFORMANCE_BAND" as "New Build Benchmark Band",
    "MEETS_2002_STANDARD" as "Compliant 2002",
    "ASSET_RATING" as "Comparative Asset Rating",
    "ASSET_RATING_BAND" as	"Comparative Energy Band",
    "BUILDING_EMISSIONS" as "EPC Rating BER",
    "RENEWABLE_SOURCES"	as "Renewable Energy Source",
    "ELECTRICITY_SOURCE" as	"Electricity Source",
    "MAIN_HEATING_FUEL" as	"Main Heating Fuel",
    "BUILDING_ENVIRONMENT" as "Building Environment",
    "PRIMARY_ENERGY_VALUE" as "Actual Building Data Primary Energy Indicator",
    "APPROXIMATE_ENERGY_USE" as "Actual Building Data Approx Energy Use",
    "IMPROVEMENT_RECOMMENDATIONS" as Recommendations,
    "LOCAL_AUTHORITY" as "Local Authority Identifier",
    "DATA_ZONE" as	"Data Zone",
    "CONSTITUENCY" as "Ward Code",
    "CONSTITUENCY_LABEL" as "Ward Name",
    "TRANSACTION_TYPE"	as "Transaction Type",
    "TARGET_EMISSIONS" as "EPC Rating TER",
    "DATA_ZONE_2011" as	"Data Zone 2011",
    {{ convert_to_datetime(clean_value('"CREATED_AT"')) }} as "Lodgement Date",
    "REPORT_REFERENCE_NUMBER" as RRN
from source