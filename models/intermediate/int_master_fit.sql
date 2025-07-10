-- models/intermediate/int_master_fit.sql
-- links the FiT Register and FiT Strikes
-- next link the Accreditation Register with the FiT Register and FiT Strikes to get
-- generator_id, status_name, generator_name, scheme_name, accred_date, comm_date
-- the next step will give an idea of expiry of FiT obligation

WITH stg_fit AS (
    SELECT  *
    FROM {{ ref('stg_view_fit_unit_rates') }}
)
SELECT
    ia.*,
    ur.*
FROM
    stg_fit ur
LEFT JOIN {{ ref('int_accred_location') }} ia
    ON ia.fit_fuel_type = ur.technology
    AND ia.capacity BETWEEN ur.declared_net_capacity * 0.8 AND ur.declared_net_capacity * 1.2
    -- AND ur.installation_type != 'Domestic'
    -- AND ABS(EXTRACT(YEAR FROM ia.comm_date) - EXTRACT(YEAR FROM ur.commissioning_date)) <= 1
    AND ia.lsoa = ur.llsoa_code
    --AND ur.export_status != 'No Export'
    --AND ur.export_status != 'No Export (Off-Grid)'
    WHERE   ia.scheme_name like 'REGO'
    --AND     ia.capacity <= 5000
    --AND     ia.capacity >= 50
    --AND     ia.lsoa is not null

