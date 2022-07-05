--Модель представляет собой SQL-код, по результатам работы которого должна получиться целевая таблица: 
--клики, показы, установки, расходы в разбивке по дате, кампании, группе объявления, платформе и источнику (рекламному кабинету). 
--В таблице должны быть только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), 
--только User Acquisition (is_ret_campaign=FALSE).
--Если данные о количестве установок есть в таблицах из рекламных кабинетов, берите их оттуда, а если нет - то из stg_af_installs
 
 -- 1. Партицирование по дате:

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

--  2. Данные о количестве установок из табл. 'stg_af_installs':

WITH grouped AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) AS count_appsflyer_id
    FROM {{ ref('stg_af_installs')}}
    WHERE is_realweb
        AND source != 'other'
        AND NOT is_ret_campaign
    GROUP BY 1, 2, 3, 4, 5
    ),

--  3. Данные из:

--      3.1 facebook'

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
        WHERE is_realweb 
            AND NOT is_ret_campaign
    ),

--      3.2 google_ads

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
        WHERE is_realweb 
            AND NOT is_ret_campaign
    ),

--      3.3 huawei

    huawei AS (
         SELECT
             date,
             campaign_name,
             '' AS adset_name,
             platform,
             'huawei' AS source,
             clicks,
             costs,
             NULL installs,
             impressions
         FROM {{ ref('stg_huawei_ads') }}
         WHERE is_realweb 
            AND NOT is_ret_campaign
     ),

--      3.4 mytarget

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
         WHERE is_realweb 
            AND NOT is_ret_campaign
     ),

--      3.5 tiktok

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
         WHERE is_realweb 
            AND NOT is_ret_campaign
     ),

--      3.6 vkontakte

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
         WHERE is_realweb 
            AND NOT is_ret_campaign
     ),

--      3.7 yandex

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
         WHERE is_realweb 
            AND NOT is_ret_campaign
     ),

--  4. Объединяем 3.1-3.7 в таблицу

sources AS (
    SELECT * FROM facebook
    UNION ALL
    SELECT * FROM google_ads
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

--  5. grouped (п.1) + sources (п.4), с заменой NULL

joined AS (
    SELECT
        COALESCE(grouped.date, sources.date) date,
        COALESCE(grouped.campaign_name, sources.campaign_name) campaign_name,
        COALESCE(grouped.adset_name, sources.adset_name) adset_name,
        COALESCE(grouped.platform, sources.platform) platform,
        COALESCE(grouped.source, sources.source) source,
        COALESCE(clicks, 0) clicks,
        COALESCE(costs, 0) costs,
        COALESCE(installs, count_appsflyer_id,0) installs,
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