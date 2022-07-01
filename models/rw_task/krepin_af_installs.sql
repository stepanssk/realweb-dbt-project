--Модель представляет собой SQL-код, по результатам работы которого должна получиться целевая таблица:
--  - клики
--  - показы
--  - установки
--  - расходы 

--в разбивке по:
--  - дате
--  - кампании
--  - группе объявления
--  - платформе
--  - источнику (по рекламному кабинету) 

--В таблице должны быть только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), только User Acquisition (is_ret_campaign=FALSE). 
--Если данные о количестве установок есть в таблицах из рекламных кабинетов, берите их оттуда, а если нет - то из stg_af_installs (это требование заказчика, такое бывает).

--Обращайтесь к существующим моделям с помощью функции ref, например:

--SELECT * FROM {{ ref('stg_yandex') }}
--WHERE is_realweb AND NOT is_ret_campaign

--Примечания: 
-- 1) За основу взят SQL-код запроса для dbt_rsultanov - rsultanov_af_installs с некоторыми корректировками
-- 2) Вид финальной таблицы:
--    - дата (date)
--    - кампания (campaign_name)
--    - группа объявления (adset_name)
--    - источник (source) 
--    - платформа (platform)
--    - клики (clicks)
--    - расходы (costs)
--    - установки (installs)
--    - показы (impressions)

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



WITH 
--выбираем установки по условиям is_realweb=TRUE, source!='other', is_ret_campaign=FALSE
installs AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        appsflyer_id,
        source
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb AND NOT is_ret_campaign AND source != 'other'
),

--группируем installs, приводя к виду финальной таблицы
grouped AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) sum_installs
    FROM installs
    GROUP BY 1,2,3,4,5
),
-- из всех источников берем нужные данные, с соответствующими условиями (is_realweb=TRUE, is_ret_campaign=FALSE), приводя к виду финальной таблицы
--в некоторых источниках отсутствуют столбец с установками, добавляем его путем использования NULL installs
 
fb AS (
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

google_ads AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source,
        clicks,
        costs,
        installs,
        impressions
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

hw AS (
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

mt AS (
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

tt AS (
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

vk AS (
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

ya AS (
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

--объдиняем все источники в одну таблицу
sources AS (
    SELECT * FROM fb
    UNION ALL
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM hw
    UNION ALL
    SELECT * FROM mt
    UNION ALL
    SELECT * FROM tt
    UNION ALL
    SELECT * FROM vk
    UNION ALL
    SELECT * FROM ya
),

--джойним grouped и sources, заменяя, где необходимо, NULL-значения столбцов grouped значениями из соответствующих столбцов sources с использованием функции COALESCE 

joined AS (
    SELECT
        COALESCE(grouped.date, sources.date) date,
        COALESCE(grouped.campaign_name, sources.campaign_name) campaign_name,
        COALESCE(grouped.adset_name, sources.adset_name) adset_name,
        COALESCE(grouped.platform, sources.platform) platform,
        COALESCE(grouped.source, sources.source) source,
        COALESCE(clicks,0) clicks,
        COALESCE(costs,0) costs,
        COALESCE(installs, sum_installs, 0) installs,
        COALESCE(impressions,0) impressions
    FROM grouped
    FULL OUTER JOIN sources
    ON grouped.date = sources.date
    AND grouped.campaign_name = sources.campaign_name
    AND grouped.adset_name = sources.adset_name
    AND grouped.platform = sources.platform
    AND grouped.source = sources.source
    
),

-- фильтруем полученные данные по условию "clicks + costs + installs + impressions > 0", т.к. при User Acquisition хотя бы одна из ключевых метрик должна быть отличной от нуля

final AS (
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
    FROM joined
    WHERE clicks + costs + installs + impressions > 0
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
FROM final
