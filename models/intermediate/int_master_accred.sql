-- models/intermediate/int_master_accred.sql
-- links the Accredidation Register with the ECR

WITH int_accred_location AS (
    SELECT  *
    FROM {{ ref('int_accred_location') }}
    WHERE   scheme_name like 'REGO'
    AND     lsoa is not null
)
SELECT
    ia.*,
    ur.*
FROM
    int_accred_location ia
LEFT JOIN {{ ref('int_ecr_location') }} ur
    ON ia.lsoa = ur.lsoa
    AND ia.simple_ft = ur.simple_ft
    AND ia.capacity BETWEEN ur.capacity_used * 0.8 AND ur.capacity_used * 1.2
    -- AND ur.installation_type != 'Domestic'
    -- AND ABS(EXTRACT(YEAR FROM ia.comm_date) - EXTRACT(YEAR FROM ur.commissioning_date)) <= 1
    -- AND ur.export_status != 'No Export'
    -- AND ur.export_status != 'No Export (Off-Grid)'

