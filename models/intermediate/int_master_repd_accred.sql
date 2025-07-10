-- File: models/intermediate/int_master_repd_accred.sql
-- Links the REPD with the Accreditation Register, matching the closest spatial points fitting the criteria

WITH int_accred_location AS (
    SELECT
        generator_id,
        status_name,
        generator_name,
        scheme_name,
        capacity,
        country as accred_country,
        technology_name,
        output_type,
        accred_date,
        comm_date,
        organization,
        organization_address,
        fax_num,
        generating_station_address as accred_generating_station_address,
        generating_station_postcode as accred_generating_station_postcode,
        cln_gen_postcode,
        lsoa,
        simple_ft as accred_simple_ft,
        fit_fuel_type
    FROM
        {{ ref('int_accred_location') }} a
    WHERE
        status_name = 'live'
),
closest_repd_location AS (
    SELECT
        a.*,
        r.operator_or_applicant,
        r.customer_site,
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
        r.last_updated
    FROM
        int_accred_location a
    LEFT JOIN
        {{ ref('stg_view_repd_location') }} r
    ON
        a.accred_simple_ft = r.simple_ft
        AND
        a.capacity/1000 BETWEEN r.registered_cap_1 * 0.8 AND r.registered_cap_1 * 1.2
        AND
        a.cln_gen_postcode = r.derived_postcode
    WHERE
        r.development_status_short LIKE 'Operational'
        AND r.geopoint IS NOT NULL)

    SELECT
        *
    FROM
        closest_repd_location