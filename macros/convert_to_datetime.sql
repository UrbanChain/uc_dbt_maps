-- macros/convert_to_datetime.sql

{% macro convert_to_datetime(column_name) %}
    case
        -- Detect 'YYYY-MM-DD' format
        when strpos({{ column_name }}, '-') > 0 then
            to_timestamp({{ column_name }}, 'YYYY-MM-DD')

        -- Detect 'DD/MM/YYYY' format
        when strpos({{ column_name }}, '/') > 0 and length({{ column_name }}) = 10 then
            to_timestamp({{ column_name }}, 'DD/MM/YYYY')

        -- Detect 'MM/DD/YY HH:MM' format
        when strpos({{ column_name }}, '/') > 0 and length({{ column_name }}) >= 10 and strpos({{ column_name }}, ':') > 0 then
            to_timestamp({{ column_name }}, 'MM/DD/YY HH24:MI')

        -- Handle cases with two-digit years (MM/DD/YY or D/M/YY)
        when strpos({{ column_name }}, '/') > 0 and length({{ column_name }}) = 8 then
            to_timestamp({{ column_name }}, 'MM/DD/YY')
        when strpos({{ column_name }}, '/') > 0 and length({{ column_name }}) = 7 then
            to_timestamp({{ column_name }}, 'D/M/YY')

        -- Default to NULL if format does not match
        else
            null
    end
{% endmacro %}
