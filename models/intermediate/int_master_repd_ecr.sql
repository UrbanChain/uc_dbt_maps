-- File: models/intermediate/int_master_repd_ecr.sql
-- Links the REPD with the ECR, matching the closest spatial points fitting the criteria

WITH int_ecr_location AS (
    SELECT
        export_mpan,
        import_mpan,
        customer_name AS ecr_customer_name,
        customer_site AS ecr_customer_site,
        town_city,
        address_line_1,
        postcode AS ecr_postcode,
        postcode_used,
        county AS ecr_county,
        country AS ecr_country,
        grid_supply_point,
        bulk_supply_point,
        primary_boundary,
        licence_area,
        capacity_used AS ecr_capacity_used,
        connection_status,
        date_connected,
        date_accepted,
        in_connection_queue,
        energy_source_1,
        simple_ft AS ecr_simple_ft,
        last_updated AS ecr_last_updated,
        geopoint AS ecr_geopoint -- Geopoints are already geometries in CRS 27700
    FROM
        {{ ref('int_ecr_location') }} e
    WHERE
        connection_status = 'Connected'
),
closest_repd_location AS (
    SELECT
        e.*,
        r.operator_or_applicant,
        r.customer_site AS repd_site_name,
        r.generating_station_address,
        r.post_code,
        r.derived_postcode,
        r.county,
        r.country,
        r.technology_type,
        r.simple_ft,
        r.registered_cap_1,
        r.development_status_short,
        r.operational,
        r.planning_permission_granted,
        r.last_updated,
        r.geopoint AS repd_geopoint, -- Geopoints are already geometries in CRS 27700
        ST_Distance(e.ecr_geopoint, r.geopoint) AS distance -- Calculate distance in CRS 27700 units
    FROM
        int_ecr_location e
    LEFT JOIN
        {{ ref('stg_view_repd_location') }} r
    ON
        e.ecr_simple_ft = r.simple_ft
        AND
        e.ecr_capacity_used BETWEEN r.registered_cap_1 * 0.8 AND r.registered_cap_1 * 1.2
    WHERE
        r.development_status_short LIKE 'Operational'
        AND r.geopoint IS NOT NULL
),
ranked_locations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY export_mpan, import_mpan ORDER BY distance ASC) AS rank -- Rank by distance
    FROM
        closest_repd_location
)
SELECT
    *
FROM
    ranked_locations
WHERE
    rank = 1
