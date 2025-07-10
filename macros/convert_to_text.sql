-- macros/convert_to_text.sql

{% macro convert_to_text(column_name) %}
    cast({{ column_name }} as text)
{% endmacro %}
