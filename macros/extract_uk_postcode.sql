-- macros/extract_uk_postcode.sql

{% macro extract_uk_postcode(text_field) %}
    CASE
        -- Normalize whitespace, handle non-breaking spaces, and extract case-insensitive UK postcode
        WHEN regexp_match(
            REGEXP_REPLACE(REGEXP_REPLACE({{ text_field }}, '\xa0', ' ', 'g'), '\s+', ' ', 'g'), -- Normalize spaces
            '(?i)([A-Z]{1,2}[0-9][0-9A-Z]? ?[0-9][A-Z]{2})' -- Case-insensitive regex
        ) IS NOT NULL
        THEN regexp_replace(
                REGEXP_MATCH(
                    REGEXP_REPLACE(REGEXP_REPLACE({{ text_field }}, '\xa0', ' ', 'g'), '\s+', ' ', 'g'), -- Normalize spaces
                    '(?i)([A-Z]{1,2}[0-9][0-9A-Z]? ?[0-9][A-Z]{2})' -- Case-insensitive regex
                )::text,
                '[{}"]', '', 'g' -- Remove unwanted characters
            )
        ELSE NULL
    END
{% endmacro %}
