{% macro is_realweb(campaign_name) %}
    REGEXP_CONTAINS(TRIM(LOWER({{campaign_name}})), r'^realweb')
{% endmacro %}