-- Материализация в Production датасете
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

--Сперва соберем данные из всех рекламных кабинетов, произведем группировку и затем объединим
WITH
huawei AS
(
SELECT
        date,
        campaign_name,
        '' as adset_name,
        platform,
        'huawei' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL installs,
        SUM(costs) costs    
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

facebook AS
(
SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        SUM(installs) installs,
        SUM(costs) costs   
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

google AS
(
SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        SUM(installs) installs,
        SUM(costs) costs  
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

mytarget AS
(
SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL installs,
        SUM(costs) costs   
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

tiktok AS
(
SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL installs,
        SUM(costs) costs   
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

vkontakte AS
(
SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL installs,
        SUM(costs) costs   
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

yandex AS
(
SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' as source, 
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL installs,
        SUM(costs) costs  
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

--Объединяем данные выше
all_ads_cabinet AS 
(
    SELECT * FROM facebook
    UNION ALL 
    SELECT * FROM google
    UNION ALL
    SELECT * FROM huawei
    UNION ALL 
    SELECT * FROM mytarget
    UNION ALL
    SELECT * FROM tiktok
    UNION ALL 
    SELECT * FROM vkontakte
    UNION ALL
    SELECT * FROM yandex
),

--Рассмотрим базу installs. Cгруппируем количество установок по date, campaign_name, adset_name, platform, source
installs AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT(DISTINCT appsflyer_id) AS installs 
  FROM {{ ref('stg_af_installs') }}
  WHERE is_realweb
    AND NOT is_ret_campaign
    AND source != 'other' 
  GROUP BY 1, 2, 3, 4, 5
),

/*Объединяем installs и all_ads_cabinet по принципу: там где в рекламных кабинетах all_ads_cabinet нет данных об установке - берем их из базы installs.
Так же нас просили заменить все вычисляемые значения с NULL (если есть) на 0 */
installs_and_cabinet AS (
  SELECT 
    IFNULL(installs.date,all_ads_cabinet.date) as date,
    IFNULL(installs.campaign_name,all_ads_cabinet.campaign_name) as campaign_name,
    IFNULL(installs.adset_name,all_ads_cabinet.adset_name) as adset_name,
    IFNULL(installs.platform,all_ads_cabinet.platform) as platform,
    IFNULL(installs.source,all_ads_cabinet.source) as source,
    IFNULL(IFNULL(all_ads_cabinet.installs, installs.installs), 0) AS installs,
    IFNULL(all_ads_cabinet.clicks, 0) AS clicks,
    IFNULL(all_ads_cabinet.impressions, 0) AS impressions,
    IFNULL(all_ads_cabinet.costs, 0) AS costs
  FROM all_ads_cabinet
  FULL JOIN installs
  ON 
    all_ads_cabinet.date = installs.date 
    AND
    all_ads_cabinet.campaign_name = installs.campaign_name 
    AND
    all_ads_cabinet.adset_name = installs.adset_name 
    AND
    all_ads_cabinet.platform = installs.platform 
    AND
    all_ads_cabinet.source = installs.source
)


--Итоговый результат совмещенных таблиц. Оставляем только платформы ios и android
SELECT* FROM installs_and_cabinet