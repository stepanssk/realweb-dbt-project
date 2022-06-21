{% macro install_source(campaign_name) %}
    CASE REGEXP_EXTRACT(TRIM(LOWER({{campaign_name}})), r'realweb_([a-z]+)_')
        WHEN 'fb' THEN 'facebook'
        WHEN 'hw' THEN 'huawei'
        WHEN 'mt' THEN 'mytarget'
        WHEN 'tiktok' THEN 'tiktok'
        WHEN 'uac' THEN 'google_ads'
        WHEN 'vk' THEN 'vkontakte'
        WHEN 'ya' THEN 'yandex'
        ELSE 'other' END
{% endmacro %}