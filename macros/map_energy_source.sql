-- macros/map_energy_source.sql

{% macro map_energy_source(fuel_type) %}
    {% set mappings = var('energy_source_mappings') %}
    {% if fuel_type is not none %}
        {% set mapped_value = mappings.get(fuel_type, "Unknown") %}
    {% else %}
        {% set mapped_value = "Unknown" %}
    {% endif %}
    {{ return(mapped_value) }}
{% endmacro %}



