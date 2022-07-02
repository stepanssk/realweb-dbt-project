
-- Клики, показы, установки, расходы в разбивке по дате, кампании, группе объявления, платформе и источнику (рекламному кабинету).
-- В таблице должны быть только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), только User Acquisition (is_ret_campaign=FALSE).
-- Если данные о количестве установок есть в таблицах из рекламных кабинетов, берите их оттуда, а если нет - то из stg_af_installs


-- Материализация в Production датасете с партицированием по датам
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

-- Количество установок в таблице stg_af_installs
-- Только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), только User Acquisition (is_ret_campaign=FALSE)
WITH installations_common AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    count(distinct appsflyer_id) as installs_qty
  FROM {{ ref('stg_af_installs') }}
  WHERE is_realweb and not is_ret_campaign and source!='other'
  GROUP BY date, campaign_name, adset_name, platform, source
  ORDER BY date, campaign_name, adset_name, platform, source
),

-- Клики, показы, установки, расходы в разбивке по дате, кампании, группе объявления, платформе и источнику (рекламному кабинету)
-- Только кампании Риалвеба (is_realweb=TRUE), только User Acquisition (is_ret_campaign=FALSE)
-- Все таблицы единого формата, если нет данных по установкам, указываем null
-- Добавляем название источника
facebook AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'facebook' AS source,
    sum(installs) installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_facebook') }}
WHERE is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

google_ads AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'google_ads' AS source,
    sum(installs) installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_google_ads') }}
WHERE campaign_name is not null and is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

huawei_ads AS (
  SELECT 
    date,
    campaign_name,
    -- нет поля "рекламное объявление", зададим "-", такое значение уже встречается в наших таблицах
    '-' AS adset_name,
    platform,
    'huawei_ads' AS source,
    null installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_huawei_ads') }}
WHERE is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

mytarget AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'mytarget' AS source,
    null installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_mytarget') }}
WHERE is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

tiktok AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'tiktok' AS source,
    null installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_tiktok') }}
WHERE is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

vkontakte AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'vkontakte' AS source,
    null installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_vkontakte') }}
WHERE is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

yandex AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'yandex' AS source,
    null installs_qty,
    sum(clicks) clicks_qty,
    sum(impressions) impressions_qty,
    sum(costs) costs
FROM {{ ref('stg_yandex') }}
WHERE is_realweb and not is_ret_campaign
GROUP BY date, campaign_name, adset_name, platform
ORDER BY date, campaign_name, adset_name, platform
),

-- Собираем данные по всем источникам в одну таблицу
all_sources AS (
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

-- Объединяем данные по источникам и данные по установкам
all_data AS (
  SELECT 
    all_sources.date,
    all_sources.campaign_name,
    all_sources.adset_name,
    all_sources.platform,
    all_sources.source,
    -- Если есть кол-во установок в данных по источнику, берём его, если нет - в данных по установках
    -- Пустые значения в числовых полях заменим на 0
    IFNULL(IFNULL(all_sources.installs_qty, installations_common.installs_qty), 0) AS installs_qty,
    IFNULL(all_sources.clicks_qty, 0) AS clicks_qty,
    IFNULL(all_sources.impressions_qty, 0) AS impressions_qty,
    IFNULL(all_sources.costs, 0) AS costs
  FROM all_sources
  -- Полное объединение, чтобы точно ничего не потерять
  FULL JOIN installations_common
  ON 
    all_sources.date = installations_common.date AND
    all_sources.campaign_name = installations_common.campaign_name AND
    all_sources.adset_name = installations_common.adset_name AND
    all_sources.platform = installations_common.platform AND
    all_sources.source = installations_common.source
)

-- Оставляем только платформы ios и android, непустые названия рекламных кампаний
-- и ситуации, когда показы (верхний уровень воронки) не нулевые, либо установки не нулевые 
SELECT * FROM all_data
WHERE
  campaign_name is not null AND
  (platform = 'ios' OR platform = 'android') AND
  (impressions_qty !=0 OR installs_qty !=0)