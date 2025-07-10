WITH loc AS (
    SELECT *
    FROM {{ source('raw_dmd_praxis_source', 'raw_dmd_location_praxis') }}
),

dmd AS (
    SELECT *
    FROM {{ source('raw_dmd_praxis_source', 'raw_dmd_praxis') }}
)

SELECT
    loc.mpan_core,
    loc.metering_point_postcode,
    loc.gsp_group_id,
    dmd.praxis_sub,
    dmd.volume
FROM loc
JOIN dmd
ON loc.mpan_core = dmd.mpan
