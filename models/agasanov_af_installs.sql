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

-- дата (date - переменная для группировки)
-- кампания (campaign_name, где поле is_realweb = True AND is_ret_campaign=False AND campaign_name IS NOT NULL - переменная для группировки)
-- группа объявления (adset_name - переменная для группировки)
-- платформа (platform, где platform = 'android' OR platform = 'ios' - переменная для группировки)
-- источник/рекламный кабинет (source, где source != 'other' для таблицы установок и source = 'название_источника' - переменная для группировки)
-- агрегируемые значения:
-- клики (clicks)
-- показы (impressions)
-- установки (installs если есть в таблице, если нет, то количество установок из stg_af_installs)
-- расходы (costs)


-- получаем данные из таблиц по источникам для сцепления
WITH yandex_info AS (
    SELECT date,
        campaign_name,
        CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
        platform,
        'yandex' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL as installs,
        SUM(costs) costs
    FROM {{ ref('stg_yandex')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

  vkontakte_info AS (
    SELECT date,
       campaign_name,
       CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
       platform,
       'vkontakte' as source,
       SUM(clicks) clicks,
       SUM(impressions) impressions,
       NULL as installs,
       SUM(costs) costs
    FROM {{ ref('stg_vkontakte')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

  tiktok_info AS (
    SELECT date,
       campaign_name,
       CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
       platform,
       'tiktok' as source,
       SUM(clicks) clicks,
       SUM(impressions) impressions,
       NULL as installs,
       SUM(costs) costs
    FROM {{ ref('stg_tiktok')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

  mytarget_info AS (
    SELECT date,
       campaign_name,
       CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
       platform,
       'mytarget' as source,
       SUM(clicks) clicks,
       SUM(impressions) impressions,
       NULL as installs,
       SUM(costs) costs
    FROM {{ ref('stg_mytarget')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

  huawei_info AS (
    SELECT date,
           campaign_name,
           '-' as adset_name,
           platform,
           'huawei' as source,
           SUM(clicks) clicks,
           SUM(impressions) impressions,
           NULL as installs,
           SUM(costs) costs
    FROM {{ ref('stg_huawei_ads')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

  google_info AS (
    SELECT date,
           campaign_name,
           CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
           platform,
           'google_ads' as source,
           SUM(clicks) clicks,
           SUM(impressions) impressions,
           SUM(installs) installs,
           SUM(costs) costs
    FROM {{ ref('stg_google_ads')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

  facebook_info AS (
    SELECT date,
           campaign_name,
           CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
           platform,
           'facebook' as source,
           SUM(clicks) clicks,
           SUM(impressions) impressions,
           SUM(installs) installs,
           SUM(costs) costs
    FROM {{ ref('stg_facebook')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4,5),

-- таблица с количеством установок с группировкой по дате, кампании, группе объявления, платформе, источнику
  installs AS (
    SELECT date,
           campaign_name,
           CASE
          WHEN adset_name IS NULL OR adset_name = '--'
          THEN '-'
          ELSE adset_name
          END AS adset_name,
           platform,
           source,
           COUNT(DISTINCT appsflyer_id) AS installs
    FROM {{ ref('stg_af_installs')}}
    WHERE is_realweb = True AND NOT is_ret_campaign AND
    platform IN ('android','ios') AND campaign_name IS NOT NULL AND source != 'other'
    GROUP BY 1,2,3,4,5),

-- объединенная таблица всех рекламных кабинетов
  united_table AS (
    SELECT * FROM yandex_info
    UNION ALL
    SELECT * FROM vkontakte_info
    UNION ALL
    SELECT * FROM tiktok_info
    UNION ALL
    SELECT * FROM mytarget_info
    UNION ALL
    SELECT * FROM huawei_info
    UNION ALL
    SELECT * FROM google_info
    UNION ALL
    SELECT * FROM facebook_info),

-- добавление установок из таблицы с установками в таблицу данных по всем рекламным кабинетам
  data_final AS (
    SELECT date,
           campaign_name,
           adset_name,
           platform,
           source,
           IFNULL(clicks, 0) AS clicks,
           IFNULL(impressions, 0) AS impressions,
           IFNULL(IFNULL(ut.installs, inst.installs), 0) AS installs,
           IFNULL(costs, 0.0) AS costs
    FROM united_table ut
    FULL OUTER JOIN installs inst
    USING(date, campaign_name, adset_name, platform, source))

    SELECT *
    FROM data_final