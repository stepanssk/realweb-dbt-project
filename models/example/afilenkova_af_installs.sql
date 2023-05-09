WITH
  -- выбираем данные об установках из таблицы stg_af_installs
  installs AS (
    SELECT
      date,
      campaign_name,
      adset_name,
      platform,
      media_source AS source,
      COUNT(DISTINCT appsflyer_id) AS installs
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb AND source != 'other' AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
  ),  
  
  -- выбираем данные о кликах и показах из таблиц stg_yandex_ads и stg_google_ads
  clicks_and_views AS (
    SELECT
      DATE_TRUNC('day', date) AS date,
      campaign_name,
      adgroup_name,
      CASE WHEN device_type = 'TABLET' THEN 'android_tablet' ELSE device_type END AS platform,
      'yandex' AS source,
      SUM(impressions) AS views,
      SUM(clicks) AS clicks
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND source != 'other' AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
    UNION ALL
    SELECT
      DATE_TRUNC('day', date) AS date,
      campaign_name,
      adgroup_name,
      platform,
      'google' AS source,
      SUM(impressions) AS views,
      SUM(clicks) AS clicks
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND source != 'other' AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
  ),
  
  -- выбираем данные о расходах из таблиц stg_yandex_ads и stg_google_ads
  costs AS (
    SELECT
      DATE_TRUNC('day', date) AS date,
      campaign_name,
      adgroup_name,
      CASE WHEN device_type = 'TABLET' THEN 'android_tablet' ELSE device_type END AS platform,
      'yandex' AS source,
      SUM(spend) AS cost
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND source != 'other' AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
    UNION ALL
    SELECT
      DATE_TRUNC('day', date) AS date,
      campaign_name,
      adgroup_name,
      platform,
      'google' AS source,
      SUM(cost_micros) / 1000000.0 AS cost
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND source != 'other' AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
  )

-- объединяем все данные в одну таблицу
SELECT
  i.date,
  i.campaign_name,
  i.adset_name AS adgroup_name,
  i.platform,
  i.source,
  COALESCE(i.installs, 0) AS installs,
  COALESCE(cv.views, 0) AS views,
  COALESCE(cv.clicks, 0) AS clicks,
  COALESCE(c.cost, 0) AS cost
FROM installs i
LEFT JOIN clicks_and_views cv
  ON i.date = cv.date AND i.campaign_name = cv.campaign_name AND i.adgroup_name = cv.adgroup_name AND i.platform = cv.platform AND i.source = cv.source
LEFT JOIN costs c
  ON i.date = c.date AND i.campaign_name = c.campaign_name AND i.adgroup_name = c.adgroup_name AND i.platform = c.platform AND i.source = c.source
