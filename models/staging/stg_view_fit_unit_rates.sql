-- models/staging/stg_view_fit_unit_rates.sql

WITH rates AS (
    SELECT
        tech_type,
        min_cap,
        max_cap,
        tariff_start_dt,
        tariff_end_dt,
        effective_to,
        reference,
        tariff_source,
        fit_export_tariff,
        tariff
    FROM {{ ref('stg_raw_fit_tariff') }}
),

unit AS (
    SELECT
        technology,
        declared_net_capacity,
        commissioning_date,
        fit_eff_commission_dt,
        export_status,
        tariff_code,
        tariff_description,
        installation_type,
        accreditation_route,
        extension_flag,
        community_school_category,
        llsoa_code
    FROM {{ ref('stg_raw_fit_installations') }}
    WHERE
        accreditation_route <> 'MCS'
    AND
        declared_net_capacity >= 30
    AND
        export_status <> 'No Export (Off-Grid)'
    AND
        llsoa_code IS NOT NULL
),

combined_unit_rates AS (
    SELECT
        u.technology,
        u.declared_net_capacity,
        u.commissioning_date,
        u.export_status,
        r.fit_export_tariff,
        r.tariff,
        u.tariff_code,
        u.tariff_description,
        u.installation_type,
        u.llsoa_code

    FROM unit AS u
    LEFT JOIN rates AS r
    ON u.technology = r.tech_type
       AND u.declared_net_capacity >= r.min_cap
       AND u.declared_net_capacity <= r.max_cap
       AND u.fit_eff_commission_dt >= r.tariff_start_dt
       AND u.fit_eff_commission_dt <= r.effective_to
)

SELECT
    *
FROM combined_unit_rates
