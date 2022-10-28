{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
  )
}}

/* Приводим таблицы, в которых нету installs, к единому виду */

WITH yandex AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb
    FROM {{ ref('stg_yandex') }}
),

vkontakte AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb
    FROM {{ ref('stg_vkontakte') }}
),

mytarget AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb
    FROM {{ ref('stg_mytarget') }}
),

huawei_ads AS (
    SELECT 
        date,
        campaign_name,
        '-' AS adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb
    FROM {{ ref('stg_huawei_ads') }}
),

tiktok AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb
    FROM {{ ref('stg_tiktok') }}
),

/* Соединяем таблицы, в которых нету installs */

union_tables_without_installs AS (
    SELECT * FROM yandex
    UNION ALL
    SELECT * FROM vkontakte
    UNION ALL
    SELECT * FROM mytarget
    UNION ALL
    SELECT * FROM huawei_ads
    UNION ALL
    SELECT * FROM tiktok
),

/* Группируем таблицу с installs, считаем количество установок */

for_join_installs AS (
    SELECT
        date,
        campaign_name,
        COUNT(appsflyer_id) AS installs
    FROM {{ ref('stg_af_installs') }}
    GROUP BY 1,2
),

/* Джоиним таблицы выше с installs */

join_tables_without_installs AS (
    SELECT *
    FROM union_tables_without_installs t1
    LEFT JOIN for_join_installs t2
    USING(date, campaign_name)
),

/* Приводим таблицы, в которых ЕСТЬ installs, к единому виду */

google_ads AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb,
        installs
    FROM {{ ref('stg_google_ads') }}
),

facebook AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb,
        installs
    FROM {{ ref('stg_facebook') }}
),

/* Соединяем все таблицы */

union_tables AS (
    SELECT * FROM join_tables_without_installs
    UNION ALL
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM facebook
),

/* Добавляем столбец Source */

add_source AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        impressions,
        is_ret_campaign,
        is_realweb,
        installs,
        {{ install_source('campaign_name') }} AS source
    FROM union_tables
),

/*  Фильтрация  */
final AS (
    SELECT *
    FROM add_source
    WHERE 
            is_realweb
        AND NOT
            is_ret_campaign
        AND 
            source != 'other'
)

SELECT *
FROM final