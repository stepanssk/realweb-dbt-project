{% macro is_ret_campaign(campaign_name) %}
    REGEXP_CONTAINS(LOWER({{campaign_name}}), r'([\[_]old[\]_])|([\[_]ret[\]_])')
{% endmacro %}