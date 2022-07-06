-- В папке models напишите модель username_af_installs и материализуйте её как таблицу в датасете dbt_production
-- должна получиться целевая таблица: клики, показы, установки, расходы  
-- clicks, impressions, installs, costs

-- в разбивке по дате, кампании, группе объявления, платформе и источнику (рекламному кабинету). 
-- date, campaign_name, adset_name, platform, source

-- В таблице должны быть только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), только User Acquisition (is_ret_campaign=FALSE). 

-- Если данные о количестве установок есть в таблицах из рекламных кабинетов, берите их оттуда, а если нет - то из stg_af_installs (это требование заказчика, такое бывает). 
-- Обращайтесь к существующим моделям с помощью функции ref

{% if target.name == 'prod' %}

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

{% endif %}

WITH installs_t AS (
    SELECT
        date, 
        campaign_name, 
        adset_name, 
        platform, 
        source,
        COUNT (DISTINCT(appsflyer_id) )AS installs_count
    FROM {{ ref('stg_af_installs') }}
    WHERE 
        is_realweb = TRUE
        AND source!='other'
        AND is_ret_campaign=FALSE
    GROUP BY
        date, 
        campaign_name, 
        adset_name, 
        platform, 
        source
),

facebook AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source,
        clicks,
        impressions,
        installs,
        costs  
    FROM {{ ref('stg_facebook') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign=FALSE  
),

google_ads AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source,
        clicks,
        impressions,
        installs,
        costs  
    FROM {{ ref('stg_google_ads') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign=FALSE  
),

huawei_ads AS (
    SELECT
        date,
        campaign_name,
        "" as adset_name,
        platform,
        'huawei_ads' AS source,
        clicks,
        impressions,
        NULL as installs,
        costs  
    FROM {{ ref('stg_huawei_ads') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign=FALSE  
),

mytarget AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        clicks,
        impressions,
        NULL as installs,
        costs  
    FROM {{ ref('stg_mytarget') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign=FALSE  
),

tiktok AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        clicks,
        impressions,
        NULL as installs,
        costs  
    FROM {{ ref('stg_tiktok') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign=FALSE  
),

vkontakte AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        clicks,
        impressions,
        NULL as installs,
        costs  
    FROM {{ ref('stg_vkontakte') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign=FALSE  
),

yandex AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        clicks,
        impressions,
        NULL as installs,
        costs  
    FROM {{ ref('stg_yandex') }}
    WHERE 
        is_realweb = TRUE
        AND is_ret_campaign = FALSE  
),

sources_all AS (
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

final AS (
    SELECT
        CASE 
            WHEN installs_t.date IS NOT NULL
            THEN installs_t.date
            ELSE sources_all.date
            END AS date,
        CASE 
            WHEN installs_t.campaign_name IS NOT NULL
            THEN installs_t.campaign_name
            ELSE sources_all.campaign_name
            END AS campaign_name,
        CASE 
            WHEN installs_t.adset_name IS NOT NULL
            THEN installs_t.adset_name
            ELSE sources_all.adset_name
            END AS adset_name,
        CASE 
            WHEN installs_t.platform IS NOT NULL
            THEN installs_t.platform
            ELSE sources_all.platform
            END AS platform,
        CASE 
            WHEN installs_t.source IS NOT NULL
            THEN installs_t.source
            ELSE sources_all.source
            END AS source,
        CASE 
            WHEN clicks IS NOT NULL
            THEN clicks
            ELSE 0
            END AS clicks,
        CASE 
            WHEN impressions IS NOT NULL
            THEN impressions
            ELSE 0
            END AS impressions,
        CASE 
            WHEN installs IS NOT NULL
            THEN installs
            WHEN installs_count IS NOT NULL
            THEN installs_count
            ELSE 0
            END AS installs,
        CASE 
            WHEN costs IS NOT NULL
            THEN costs
            ELSE 0
            END AS costs,
    FROM installs_t
    FULL JOIN sources_all
        ON installs_t.date = sources_all.date
        AND installs_t.campaign_name = sources_all.campaign_name
        AND installs_t.adset_name = sources_all.adset_name
        AND installs_t.platform = sources_all.platform
        AND installs_t.source = sources_all.source
             
)

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