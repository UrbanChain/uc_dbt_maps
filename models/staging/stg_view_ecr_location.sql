-- models/staging/stg_view_ecr_location.sql

with stg_raw_ecr_ukpn_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used,

        -- Normalize connection_status field
        case
            when lower(connection_status) in ('accepted to connect', 'accecpted to connect', 'accepted to connect')
                then 'Accepted to Connect'
            when lower(connection_status) = 'connected'
                then 'Connected'
            else connection_status -- Preserve other values as is
        end as connection_status,

        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_ukpn_location') }}
),

stg_raw_ecr_npgl_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used,

        -- Normalize connection_status field
        case
            when lower(connection_status) in ('accepted to connect', 'accecpted to connect', 'accepted to connect')
                then 'Accepted to Connect'
            when lower(connection_status) = 'connected'
                then 'Connected'
            else connection_status
        end as connection_status,

        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_npgl_location') }}
),

stg_raw_ecr_enwl_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used,

        -- Normalize connection_status field
        case
            when lower(connection_status) in ('accepted to connect', 'accecpted to connect', 'accepted to connect')
                then 'Accepted to Connect'
            when lower(connection_status) = 'connected'
                then 'Connected'
            else connection_status
        end as connection_status,

        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_enwl_location') }}
),

stg_raw_ecr_nged_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used,

        -- Normalize connection_status field
        case
            when lower(connection_status) in ('accepted to connect', 'accecpted to connect', 'accepted to connect')
                then 'Accepted to Connect'
            when lower(connection_status) = 'connected'
                then 'Connected'
            else connection_status
        end as connection_status,

        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_nged_location') }}
),

stg_raw_ecr_ssen_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used,

        -- Normalize connection_status field
        case
            when lower(connection_status) in ('accepted to connect', 'accecpted to connect', 'accepted to connect')
                then 'Accepted to Connect'
            when lower(connection_status) = 'connected'
                then 'Connected'
            else connection_status
        end as connection_status,

        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_ssen_location') }}
),

stg_raw_ecr_spen_location as (
    select
        export_mpan,
        import_mpan,
        customer_name,
        customer_site,
        town_city,
        address_line_1,
        postcode,
        county,
        country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used,

        -- Normalize connection_status field
        case
            when lower(connection_status) in ('accepted to connect', 'accecpted to connect', 'accepted to connect')
                then 'Accepted to Connect'
            when lower(connection_status) = 'connected'
                then 'Connected'
            else connection_status
        end as connection_status,

        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        last_updated,
        geopoint
    from {{ ref('stg_raw_ecr_spen_location') }}
),

combined_location as (
    select * from stg_raw_ecr_ukpn_location
    union all
    select * from stg_raw_ecr_npgl_location
    union all
    select * from stg_raw_ecr_enwl_location
    union all
    select * from stg_raw_ecr_nged_location
    union all
    select * from stg_raw_ecr_ssen_location
    union all
    select * from stg_raw_ecr_spen_location
)

SELECT
    combined_location.*,
    {{ nearest_postcode('combined_location.geopoint') }} AS derived_postcode,
    {{ nearest_postcode_distance('combined_location.geopoint') }} AS distance_to_postcode_km,
    COALESCE(
        {{ nearest_postcode('combined_location.geopoint') }},
        CASE
            WHEN LENGTH(TRIM(combined_location.postcode)) >= 7
                AND TRIM(combined_location.postcode) !~ '--$'
                THEN combined_location.postcode
            ELSE NULL
        END
    ) AS postcode_used

FROM
    combined_location


