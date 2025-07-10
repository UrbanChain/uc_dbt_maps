-- macros/nearest_postcode.sql

{% macro nearest_postcode(geo_point) %}
    (
        SELECT CASE
            WHEN {{ geo_point }} IS NOT NULL THEN (
                SELECT postcode
                FROM {{ source('geo_points_uk_postcodes_source', 'geo_points_uk_postcodes') }} AS geo_points_uk_postcodes
                ORDER BY ST_Transform({{ geo_point }}, 4326) <-> geo_points_uk_postcodes.geometry_4326
                LIMIT 1
            )
            ELSE NULL
        END
    )
{% endmacro %}

{% macro nearest_postcode_distance(geo_point) %}
    (
        SELECT CASE
            WHEN {{ geo_point }} IS NOT NULL THEN (
                SELECT ST_Distance(
                    ST_Transform({{ geo_point }}, 4326),
                    geo_points_uk_postcodes.geometry_4326
                ) / 1000 AS distance_km
                FROM {{ source('geo_points_uk_postcodes_source', 'geo_points_uk_postcodes') }} AS geo_points_uk_postcodes
                ORDER BY ST_Transform({{ geo_point }}, 4326) <-> geo_points_uk_postcodes.geometry_4326
                LIMIT 1
            )
            ELSE NULL
        END
    )
{% endmacro %}



