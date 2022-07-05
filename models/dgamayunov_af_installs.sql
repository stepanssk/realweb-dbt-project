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
            COUNT(DISTINCT appsflyer_id) AS installations, 
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
            'huawei_ads' AS source, 
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

    vk AS (
        SELECT 
            date, 
            NULL AS installs, 
            campaign_name, 
            adset_name, 
            platform,
            'vk' AS source, 
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
        SELECT * FROM vk
        UNION ALL
        SELECT * FROM yandex
    ),

-- Теперь объединяем данные по установкам и источникам
    
    final_data AS (
        SELECT 
            union_of_sources.date, 
            union_of_sources.campaign_name,
            union_of_sources.adset_name, 
            union_of_sources.platform, 
            union_of_sources.source,
            COALESCE(union_of_sources.installs, af_installs.installations, 0) AS installs,
            COALESCE(union_of_sources.impressions, 0) AS impressions,
            COALESCE(union_of_sources.clicks, 0) AS clicks,
            COALESCE(union_of_sources.costs, 0) AS costs,
        FROM union_of_sources

        -- Джоиним с таблицей с данными из AppsFlyer

        FULL JOIN af_installs
        ON 
            union_of_sources.date = af_installs.date AND
            union_of_sources.campaign_name = af_installs.campaign_name AND
            union_of_sources.adset_name = af_installs.adset_name AND
            union_of_sources.platform = af_installs.platform AND
            union_of_sources.source = af_installs.source
    )

SELECT * FROM final_data
WHERE campaign_name IS NOT NULL AND (platform = 'ios' OR platform = 'android')