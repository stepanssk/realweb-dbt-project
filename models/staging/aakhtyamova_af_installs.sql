/*Модель представляет собой SQL-код, по результатам работы которого получается целевая таблица: 
сгруппированные по столбцам: дата, кампания, группа объявлений, платформа, источник (рекламный кабинет),
- клики, показы, установки, расходы.
 
 В таблице - только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'),
 только User Acquisition (is_ret_campaign=FALSE). 
 Данные о количестве установок берутся из таблиц из рекламных кабинетов, а если их там нет, то из stg_af_installs */

{% if target.name == 'prod' %}

-- партицирование по дате
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

/* таблица с кол-вом установок с группировкой столбцам: 
дата, кампания, группа объявлений, платформа, источник (рекламный кабинет)
с заданными фильтрами */
WITH af_installs AS(
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(*) installs
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb 
        AND NOT is_ret_campaign 
        AND source != 'other'
    GROUP BY 1, 2, 3, 4, 5
),

/* таблицы по каждому из семи источников с данными по кликам, показам, установкам, расходам.
Установки есть только facebook и google_ads, для huawei_ads также отсутствуют данные по adset_name - 
добавляем недостающие столбцы, фильтруем по заданным условиям*/
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

-- сводная таблица по всем источникам
sources AS (
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

/* джойним таблицу по источникам и таблицу с данными по установкам по пяти столбцам, по которым проводили группировку
используем полное внешнее соединение (по причине отсутствия некоторых данных adset_name).
числовые null-значения заполняем нулями */
final AS (
    SELECT 
        CASE WHEN af_installs.date IS NOT NULL 
                THEN af_installs.date 
            ELSE sources.date
            END date,
        CASE WHEN af_installs.campaign_name IS NOT NULL 
                THEN af_installs.campaign_name 
            ELSE sources.campaign_name
            END campaign_name,
        CASE WHEN af_installs.adset_name IS NOT NULL 
                THEN af_installs.adset_name 
            ELSE sources.adset_name
            END adset_name,
        CASE WHEN af_installs.platform IS NOT NULL 
                THEN af_installs.platform 
            ELSE sources.platform
            END platform,
        CASE WHEN af_installs.source IS NOT NULL 
                THEN af_installs.source 
            ELSE sources.source
            END source, 
        CASE WHEN clicks IS NOT NULL
                THEN clicks
            ELSE 0
            END clicks,
        CASE WHEN impressions IS NOT NULL
                THEN impressions
            ELSE 0
            END impressions,
        CASE WHEN sources.installs IS NOT NULL
                THEN sources.installs
            WHEN af_installs.installs IS NOT NULL
                THEN af_installs.installs
            ELSE 0
            END installs,
        CASE WHEN costs IS NOT NULL
                THEN costs
            ELSE 0
            END costs
    FROM af_installs
        FULL JOIN sources
        ON sources.date = af_installs.date
            AND sources.campaign_name = af_installs.campaign_name
            AND sources.adset_name = af_installs.adset_name
            AND sources.platform = af_installs.platform
            AND sources.source = af_installs.source
)

-- итоговая таблица с колонками в заданном порядке
SELECT 
    date, 
    campaign_name,
    adset_name,
    platform,
    source, 
    clicks,
    impressions,
    installs,
    costs 
FROM final