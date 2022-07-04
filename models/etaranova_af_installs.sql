{{
  config(
    materialized='table',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
      }
)}}

-- для начала посчитаем количество установок, сгруппированным по дате, кампании, группе объявлений, платформе и источнике,
-- которые нам насчитал AppFlyer

WITH installs_af AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT(DISTINCT appsflyer_id) AS installs 
  FROM {{ ref('stg_af_installs')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
    AND source != 'other' 
  GROUP BY 1, 2, 3, 4, 5
),

-- Далее соберем данные из рекламных кабинетов: 
                -- facebook,
                -- google_ads,
                -- huawei_ads,
                -- mytarget,
                -- tiktok,
                -- vkontakte,
                -- yandex
-- целевая таблица: 
    --клики, 
    --показы, 
    --установки, 
    --расходы в разбивке:
            -- по дате, 
            --кампании, 
            --группе объявления, 
            --платформе и 
            --источнику (рекламному кабинету).

-- В части рекламных кабинетов нет данных по инсталам, но мы все равно добавим эту колонку для будущего джоина

facebook AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'facebook' AS source,
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    SUM(installs) installs
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
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    SUM(installs) installs
  FROM {{ ref('stg_google_ads')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
),

huawei_ads AS (
   SELECT 
    date,
    campaign_name,
    'x' AS adset_name,
    platform,
    'huawei' AS source,
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    NULL AS installs --у huawei нет данных по установкам, поэтому пока оставим null
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
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    NULL AS installs --у mytarget нет данных по установкам, поэтому пока оставим null
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
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    NULL AS installs --у tiktok нет данных по установкам, поэтому пока оставим null
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
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    NULL AS installs --у vkontakte нет данных по установкам, поэтому пока оставим null
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
    SUM(clicks) clicks,
    SUM(impressions) impressions,
    SUM(costs) costs,
    NULL AS installs --у yandex нет данных по установкам, поэтому пока оставим null
  FROM {{ ref('stg_yandex')}}
  WHERE is_realweb
    AND NOT is_ret_campaign
  GROUP BY 1, 2, 3, 4
 ),

-- Объединяем все данные из всех рекламных кабинетов 
united_from_all_sources AS (
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


-- Обогатим объединенные данные из рекламных кабинетов данными по установкам из AppsFlyer.

union_with_installs_from_af AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        IFNULL(IF(l.installs IS NOT Null, l.installs, r.installs), 0) AS installs, --если данные по установкам есть в РК, то берем оттуда, если нет, то из AF. Null заменяем на 0
        IFNULL(l.clicks, 0) clicks, 
        IFNULL(l.impressions, 0) impressions,
        IFNULL(l.costs, 0.0) costs
    FROM united_from_all_sources AS l
    FULL OUTER JOIN installs_af AS r
    USING (date, campaign_name, adset_name, platform, source))


SELECT * FROM union_with_installs_from_af