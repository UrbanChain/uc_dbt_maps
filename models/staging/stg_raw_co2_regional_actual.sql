-- models/staging/stg_raw_co2_regional_actual.sql

WITH raw_data AS (
    SELECT
        region_id,
        region_name,
        short_name,
        "from"::timestamp AS from_date,
        "to"::timestamp AS to_date,
        intensity_forecast,
        intensity_index,
        generation_mix
    FROM {{ source('raw_co2_regional_actual_source', 'raw_co2_regional_actual') }}
),

parsed_data AS (
    SELECT
        region_id,
        region_name,
        short_name,
        from_date,
        to_date,
        intensity_forecast,
        intensity_index,
        -- Parsing generation_mix into individual columns and removing '%' and trimming spaces
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'biomass:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS biomass,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'coal:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS coal,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'imports:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS imports,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'gas:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS gas,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'nuclear:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS nuclear,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'other:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS other,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'hydro:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS hydro,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'solar:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS solar,
        CAST(REGEXP_REPLACE(TRIM(SPLIT_PART(SPLIT_PART(generation_mix, 'wind:', 2), ';', 1)), '%', '', 'g') AS FLOAT) AS wind
    FROM raw_data
),

-- Add geo_id by joining with the lookup table
geo_data AS (
    SELECT
        parsed_data.*,
        lkup.geo_id
    FROM parsed_data
    LEFT JOIN (
        SELECT
            co2_region_id,
            geo_id
        FROM {{ source('lkup_co2region_dno_source', 'lkup_co2region_dno') }}
    ) lkup
    ON parsed_data.region_id = lkup.co2_region_id
)

SELECT *
FROM geo_data
