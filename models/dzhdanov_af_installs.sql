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

/* Основные условия: кампании Реалвеб, известные источники, user acquisition*/

WITH installs_af AS (
        SELECT
            date,
            campaign_name,
            adset_name,
            platform,
            source,
            COUNT(DISTINCT appsflyer_id) AS installs
        FROM {{ ref('stg_af_installs') }}
        WHERE is_realweb AND NOT is_ret_campaign AND source != 'other'
        GROUP BY 1, 2, 3, 4, 5
    ),

/* Для источников без данных install ставим Null значение. Для huawei adset  */

facebook AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' source, 
        clicks,
        impressions,
        installs,
        costs      
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

google_ads AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' source, 
        clicks,
        impressions,
        installs,
        costs      
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

huawei_ads AS (
    SELECT 
        date,
        campaign_name,
        '' adset_name,
        platform,
        'huawei' source, 
        clicks,
        impressions,
        NULL installs,
        costs      
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

mytarget AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' source, 
        clicks,
        impressions,
        NULL installs,
        costs      
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

tiktok AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' source, 
        clicks,
        impressions,
        NULL installs,
        costs      
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

vkontakte AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' source, 
        clicks,
        impressions,
        NULL installs,
        costs      
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

yandex AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' source, 
        clicks,
        impressions,
        NULL installs,
        costs      
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb 
      AND NOT is_ret_campaign
),

/* Все источники вместе */

sources_united AS (
    SELECT * FROM facebook
    UNION ALL 
    SELECT * FROM google_ads
    UNION All
    SELECT * FROM huawei_ads
    UNION ALL
    SELECT * FROM mytarget
    UNION  ALL
    SELECT * FROM tiktok
    UNION ALL
    SELECT * FROM vkontakte
    UNION ALL
    SELECT * FROM yandex
),

/* Данные из источников и из af_installs */

final_data AS (
    SELECT 
      CASE
        WHEN s.date IS NOT NULL
        THEN s.date
        ELSE inst.date
      END AS date,
      CASE
        WHEN s.campaign_name IS NOT NULL 
        THEN s.campaign_name
        ELSE inst.campaign_name
      END AS campaign_name,
      CASE
        WHEN s.adset_name IS NOT NULL
        THEN s.adset_name
        ELSE inst.adset_name
      END AS adset_name,
      CASE
        WHEN s.platform IS NOT NULL
        THEN s.platform
        ELSE inst.platform
      END AS platform,
      CASE
        WHEN s.source IS NOT NULL
        THEN s.source
        ELSE inst.source
      END AS source,
      CASE
        WHEN s.clicks IS NOT NULL
        THEN s.clicks
        ELSE 0
      END AS clicks,
      CASE
        WHEN s.impressions IS NOT NULL
        THEN s.impressions
        ELSE 0 
      END AS impressions,
      CASE 
        WHEN s.installs IS NOT NULL
        THEN s.installs
        WHEN inst.installs IS NOT NULL
        THEN inst.installs
        ELSE 0
      END AS installs,
      CASE 
        WHEN s.costs IS NOT NULL
        THEN s.costs
        ELSE 0
      END AS costs

    FROM sources_united s
    FULL JOIN installs_af inst
      ON s.date = inst.date 
      AND s.campaign_name = inst.campaign_name 
      AND s.adset_name = inst.adset_name 
      AND s.platform = inst.platform 
      AND s.source = inst.source)

/* Убираем кампании Null и оставляем данные только с платформ ios и android */

SELECT * FROM final_data
WHERE campaign_name IS NOT NULL 
  AND (platform = 'ios' OR platform = 'android')