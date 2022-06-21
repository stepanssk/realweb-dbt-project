{% macro process_strings(string) %}
    TRIM(LOWER({{string}}))
{% endmacro %}