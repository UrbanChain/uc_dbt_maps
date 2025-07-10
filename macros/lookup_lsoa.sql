{% macro lookup_lsoa(postcode) %}
    (
        SELECT lsoa21cd
        FROM {{ ref('stg_raw_pcd_lsoa_msoa_2021_lkup') }} AS lookup
        WHERE lookup.pcds = {{ postcode }}
        LIMIT 1
    )
{% endmacro %}
