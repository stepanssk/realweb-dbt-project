{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            }
  )
}}

{% endif %}

WITH yandex AS
        (SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            is_ret_campaign,
            is_realweb,
            clicks,
            costs,
            NULL AS installs,
            impressions, 
            'yandex' AS source
        FROM {{ ref('stg_yandex') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    vkontakte AS
        (SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            is_ret_campaign,
            is_realweb,
            clicks,
            costs,
            NULL AS installs,
            impressions, 
            'vkontakte' AS source
        FROM {{ ref('stg_vkontakte') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    tiktok AS
        (SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            is_ret_campaign,
            is_realweb,
            clicks,
            costs,
            NULL AS installs,
            impressions,
            'tiktok' AS source
        FROM {{ ref('stg_tiktok') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    mytarget AS
        (SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            is_ret_campaign,
            is_realweb,
            clicks,
            costs,
            NULL AS installs,
            impressions,
            'mytarget' AS source
        FROM {{ ref('stg_mytarget') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    huawei_ads AS
        (SELECT 
            date,
            campaign_name,
            'not set' AS adset_name,
            platform,
            is_ret_campaign,
            is_realweb,
            clicks,
            costs,
            NULL AS installs,
            impressions,
            'huawei_ads' AS source
        FROM {{ ref('stg_huawei_ads') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    google_ads AS
        (SELECT *, 'google_ads' AS source
        FROM {{ ref('stg_google_ads') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    facebook AS
        (SELECT *, 'facebook' AS source
        FROM {{ ref('stg_facebook') }}
        WHERE is_realweb AND NOT is_ret_campaign),

    af_installs AS
        (SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            source,
            COUNT(*) AS installs
        FROM {{ ref('stg_af_installs') }}
        WHERE is_realweb 
        AND NOT is_ret_campaign 
        AND source != 'other'
        GROUP BY 1, 2, 3, 4, 5),

    all_sources AS
        (SELECT * FROM yandex
        UNION ALL
        SELECT * FROM vkontakte
        UNION ALL
        SELECT * FROM tiktok
        UNION ALL
        SELECT * FROM mytarget
        UNION ALL
        SELECT * FROM huawei_ads
        UNION ALL
        SELECT * FROM google_ads
        UNION ALL
        SELECT * FROM facebook),

    sources_installs AS
        (SELECT date,
            campaign_name,
            IF(alls.adset_name='(not set)', afi.adset_name, alls.adset_name) 
            AS adset_name,
            platform,
            source,
            is_ret_campaign,
            is_realweb,
            IFNULL(clicks, 0) AS clicks,
            IFNULL(costs, 0) AS costs,
            IFNULL(IFNULL(alls.installs, afi.installs), 0) AS installs,
            IFNULL(impressions, 0) AS impressions 
        FROM all_sources AS alls FULL OUTER JOIN af_installs AS afi 
        USING (date, campaign_name, adset_name, platform, source))

SELECT date,
    campaign_name,
    adset_name,
    platform,
    source,
    installs,
    clicks,
    costs
FROM sources_installs
WHERE platform != 'no_platform'
AND campaign_name IS NOT NULL   