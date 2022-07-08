-- Партицируем по дате

{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
  )
}}

{% endif %}

-- Количество установок из таблицы stg_af_installs
WITH 
    af_installs AS (
        SELECT 
            date, 
            COUNT(DISTINCT appsflyer_id) AS installs, 
            campaign_name, 
            adset_name, 
            platform, 
            source
        FROM {{ ref('stg_af_installs') }}
        WHERE is_realweb AND NOT is_ret_campaign AND source!='other'
        GROUP BY date, campaign_name, adset_name, platform, source
    ),
    
    -- Данные из facebook

    facebook AS (
        SELECT 
            date, 
            SUM(installs) AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'facebook' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_facebook') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Данные из google_ads

    google_ads AS (
        SELECT 
            date, 
            SUM(installs) AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'google_ads' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_google_ads') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Данные из huawei

    huawei_ads AS (
        SELECT 
            date, 
            NULL AS installs, 
            campaign_name, 
            '' AS adset_name, 
            platform,
            'huawei' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_huawei_ads') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Данные из mytarget

    mytarget AS (
        SELECT 
            date, 
            NULL AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'mytarget' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_mytarget') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Данные из tiktok
    
    tiktok AS (
        SELECT 
            date,
            NULL AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'tiktok' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_tiktok') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Данные из VK

    vkontakte AS (
        SELECT 
            date, 
            NULL AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'vkontakte' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_vkontakte') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Данные из yandex

    yandex AS (
        SELECT 
            date, 
            NULL AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'yandex' AS source, 
            SUM(impressions) AS impressions, 
            SUM(clicks) AS clicks, 
            SUM(costs) AS costs
        FROM {{ ref('stg_yandex') }}
        WHERE is_realweb AND NOT is_ret_campaign
        GROUP BY date, campaign_name, adset_name, platform, source
    ),

    -- Объединяем все источники:

    union_of_sources AS (
        SELECT * FROM facebook
        UNION ALL
        SELECT * FROM google_ads
        UNION ALL
        SELECT * FROM huawei_ads
        UNION ALL
        SELECT * FROM mytarget
        UNION ALL
        SELECT * FROM tiktok
        UNION ALL
        SELECT * FROM vkontakte
        UNION ALL
        SELECT * FROM yandex
    ),

-- Теперь объединяем данные по установкам и источникам
    
    final_data AS (
        SELECT 
            COALESCE(union_of_sources.date, af_installs.date) AS date, 
            COALESCE(union_of_sources.campaign_name, af_installs.campaign_name) AS campaign_name,
            COALESCE(union_of_sources.adset_name, af_installs.adset_name) AS adset_name, 
            COALESCE(union_of_sources.platform, af_installs.platform) AS platform, 
            COALESCE(union_of_sources.source, af_installs.source) AS source,
            COALESCE(union_of_sources.installs, af_installs.installs, 0) AS installs,
            COALESCE(impressions, 0) AS impressions,
            COALESCE(clicks, 0) AS clicks,
            COALESCE(costs, 0) AS costs,
        FROM af_installs

        -- Джоиним с таблицей с данными из AppsFlyer

        FULL JOIN union_of_sources
        ON 
            af_installs.date = union_of_sources.date AND
            af_installs.campaign_name = union_of_sources.campaign_name AND
            af_installs.adset_name = union_of_sources.adset_name AND
            af_installs.platform = union_of_sources.platform AND
            af_installs.source = union_of_sources.source
    )

SELECT * FROM final_data
WHERE campaign_name IS NOT NULL AND (platform = 'ios' OR platform = 'android')