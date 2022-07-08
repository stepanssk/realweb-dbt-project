-- Модель представляет собой SQL-код, по результатам работы которого должна получиться целевая таблица:
-- - клики,
-- - показы,
-- - установки,
-- - расходы в разбивке по дате,
-- - кампании,
-- - группе объявления,
-- - платформе и источнику (рекламному кабинету). 

-- В таблице должны быть только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'),
-- только User Acquisition (is_ret_campaign=FALSE). 

-- Если данные о количестве установок есть в таблицах из рекламных кабинетов, берите их оттуда,
-- а если нет - то из stg_af_installs (это требование заказчика, такое бывает). 

-- Обращайтесь к существующим моделям с помощью функции ref, например:
-- SELECT * FROM {{ ref('stg_yandex') }}
-- WHERE is_realweb AND NOT is_ret_campaign

-- Дополнительно можете добавить в модель партицирование по дате.



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
WITH

all_installs AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT(DISTINCT appsflyer_id) AS sum_installs 
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
        clicks,
        costs,
        installs,
        impressions
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

google AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google' AS source,
        clicks,
        costs,
        installs,
        impressions
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

huawei AS (
    SELECT
        date,
        campaign_name,
        '-' adset_name,
        platform,
        'huawei' AS source,
        clicks,
        costs,
        NULL installs,
        impressions
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

mytarget AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        clicks,
        costs,
        NULL installs,
        impressions
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

tiktok AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        clicks,
        costs,
        NULL installs,
        impressions
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

vkontakte AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        clicks,
        costs,
        NULL installs,
        impressions
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

yandex AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        clicks,
        costs,
        NULL installs,
        impressions
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

-- Объединение ресурсов
sources_all AS (
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

-- Общая таблица
installs_sources AS (
    SELECT
        COALESCE(all_installs.date, sources_all.date) date,
        COALESCE(all_installs.campaign_name, sources_all.campaign_name) campaign_name,
        COALESCE(all_installs.adset_name, sources_all.adset_name) adset_name,
        COALESCE(all_installs.platform, sources_all.platform) platform,
        COALESCE(all_installs.source, sources_all.source) source,
        COALESCE(clicks,0) clicks,
        COALESCE(costs,0) costs,
        COALESCE(installs, sum_installs, 0) installs,
        COALESCE(impressions,0) impressions
    FROM all_installs
    FULL OUTER JOIN sources_all
    ON all_installs.date = sources_all.date
    AND all_installs.campaign_name = sources_all.campaign_name
    AND all_installs.adset_name = sources_all.adset_name
    AND all_installs.platform = sources_all.platform
    AND all_installs.source = sources_all.source
     WHERE clicks IS NOT NULL
       OR costs IS NOT NULL
        OR installs IS NOT NULL
        OR impressions IS NOT NULL   
)


SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions
FROM installs_sources
