-- macros/clean_uk_postcodes.sql

{% macro clean_uk_postcode(postcode) %}
    -- Remove extra spaces, convert to uppercase, validate UK postcode format, and handle blank values as NULL
    CASE
        WHEN TRIM(CAST({{ postcode }} AS TEXT)) = '' -- Blank or whitespace-only values
        THEN NULL
        WHEN TRIM(CAST({{ postcode }} AS TEXT)) ~* '^([A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2})$'
        THEN UPPER(REGEXP_REPLACE(TRIM(CAST({{ postcode }} AS TEXT)), '\s+', '', 'g')) -- Normalize spaces
        ELSE NULL -- Return NULL if not a valid postcode
    END
{% endmacro %}

