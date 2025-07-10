-- models/staging/stg_view_geo_osm_lines.sql

WITH power_lines AS (
    SELECT
        id,
        name,
        name_en,
        ref,
        operator,
        wd_operator,
        construction,
        location,
        max_voltage,
        voltages,
        frequency,
        CAST(circuits AS TEXT) AS circuits,
        cables,
        start_date,
        geometry AS geometry_4326,
        ST_Transform(geometry, 27700) AS geometry_27700,
        'power_line' AS source_table
    FROM {{ source('geo_osm_power', 'geo_osm_power_line') }}
),
substation_polygons AS (
    SELECT
        id,
        name,
        name_en,
        ref,
        operator,
        wd_operator,
        construction,
        NULL AS location, -- Not present in substation
        max_voltage,
        voltages,
        frequency,
        NULL AS circuits, -- Not present in substation
        NULL AS cables,   -- Not present in substation
        start_date,
        geometry AS geometry_4326,
        ST_Transform(geometry, 27700) AS geometry_27700,
        'substation_polygon' AS source_table
    FROM {{ source('geo_osm_power', 'geo_osm_power_substation_polygon') }}
)

SELECT *
FROM power_lines
UNION ALL
SELECT *
FROM substation_polygons
