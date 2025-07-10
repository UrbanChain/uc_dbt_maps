{% macro clean_value(column_name) %}
    case
        -- Handle text or character varying fields
        when pg_typeof({{ column_name }}) in ('character varying', 'text') then
            case
                when lower(regexp_replace(cast(TRIM({{ column_name }}) as text), E'[\r\n]+', '', 'g')) in (
                    'data not available',
                    'data not applicable',
                    '--redacted--',
                    'redacted',
                    ''
                )
                then NULL
                else TRIM({{ column_name }})
            end

        -- Handle numeric fields
        when pg_typeof({{ column_name }}) in ('double precision', 'numeric', 'integer', 'bigint') then
            case
                when {{ column_name }} is null or cast({{ column_name }} as text) = '' then NULL
                else {{ column_name }}
            end

        -- Default case for unsupported types
        else {{ column_name }}
    end
{% endmacro %}
