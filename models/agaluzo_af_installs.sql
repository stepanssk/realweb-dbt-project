-- Партицирование по дате

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

-- Установки

WITH installs AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT(DISTINCT appsflyer_id) AS num_of_installs 
  FROM {{ ref('stg_af_installs')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
    AND source != 'other' 
  GROUP BY 1, 2, 3, 4, 5
),

-- Каналы

facebook AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'facebook' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    SUM(installs) AS num_of_installs
  FROM {{ ref('stg_facebook')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
),

google_ads AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'google_ads' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    SUM(installs) AS num_of_installs
  FROM {{ ref('stg_google_ads')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
),

huawei_ads AS (
   SELECT 
    date,
    campaign_name,
    '-' AS adset_name,
    platform,
    'huawei' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    NULL AS num_of_installs
  FROM {{ ref('stg_huawei_ads')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
),

 mytarget AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'mytarget' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    NULL AS num_of_installs
  FROM {{ ref('stg_mytarget')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
 ),

 tiktok AS (
     SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'tiktok' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    NULL AS num_of_installs
  FROM {{ ref('stg_tiktok')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
 ),

 vkontakte AS (
   SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'vkontakte' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    NULL AS num_of_installs
  FROM {{ ref('stg_vkontakte')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
 ),

 yandex AS (
   SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'yandex' AS source,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(costs) AS costs,
    NULL AS num_of_installs
  FROM {{ ref('stg_yandex')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
 ),


-- Объединение ресурсов

all_sources_combination AS (
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
    SELECT * FROM yandex),



-- Объединение c установками

final_table AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        IFNULL(IF(comb.num_of_installs IS NOT Null, comb.num_of_installs, inst.num_of_installs), 0) AS num_of_installs, 
        IFNULL(comb.clicks, 0) AS num_of_clicks, 
        IFNULL(comb.impressions, 0) num_of_impressions,
        IFNULL(comb.costs, 0) costs
    FROM all_sources_combination AS comb
    FULL OUTER JOIN installs AS inst
    USING (date, campaign_name, adset_name, platform, source))


-- Финальная таблица с фильтром по платформам

SELECT * 
FROM final_table
WHERE platform IN ('ios','android')