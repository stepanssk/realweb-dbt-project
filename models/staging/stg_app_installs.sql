-- это необязательная часть, мы говорим dbt: "если среда - prod, то материализуй как таблицу"
{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

WITH source AS (
    SELECT 
        media_source,
        af_prt AS partner,
        campaign,
        DATETIME(attributed_touch_time) AS attributed_touch_time,
        DATETIME(install_time) AS install_time,
        platform,
        city,
        device_type
    FROM {{ source('hackaton', 'app_installs') }}
)

SELECT
    media_source,
    partner,
    campaign,
    attributed_touch_time,
    install_time,
    platform,
    city,
    device_type
FROM source