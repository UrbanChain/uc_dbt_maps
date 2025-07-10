-- macros/clean_fit_year.sql

{% macro extract_tariff_fy(column_name) %}
    (regexp_match({{ column_name }}, '\\d{4}/\\d{2}'))[1]
{% endmacro %}
