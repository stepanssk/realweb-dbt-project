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


-- данные из таблиц по источнику (рекламному кабинету)
WITH facebook_df AS (
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
    FROM {{ ref('stg_facebook')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

google_df AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        SUM(installs) installs,
        SUM(costs) costs
    FROM {{ ref('stg_google_ads')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

huawei_df AS (
    SELECT 
        date,
        campaign_name,
        '' adset_name,
        platform,
        'huawei' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL as installs,
        SUM(costs) costs
    FROM {{ ref('stg_huawei_ads')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

mytarget_df AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL as installs,
        SUM(costs) costs
    FROM {{ ref('stg_mytarget')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

tiktok_df AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL as installs,
        SUM(costs) costs
    FROM {{ ref('stg_tiktok')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

vkontakte_df AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL as installs,
        SUM(costs) costs
    FROM {{ ref('stg_vkontakte')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

yandex_df AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' as source,
        SUM(clicks) clicks,
        SUM(impressions) impressions,
        NULL as installs,
        SUM(costs) costs
    FROM {{ ref('stg_yandex')}}
    WHERE is_realweb
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL
    GROUP BY 1,2,3,4
),

-- таблицу с установками группируем по дате, кампании, группе объявления, платформе и источнику
installs AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) AS installs
    FROM {{ ref('stg_af_installs')}}
    WHERE is_realweb 
        AND NOT is_ret_campaign 
        AND platform IN ('ios', 'android') 
        AND campaign_name IS NOT NULL 
        AND source != 'other'
    GROUP BY 1,2,3,4,5
),

-- объединяем все таблицы по источникам (рекламному кабинету)
all_sources AS (
    SELECT * FROM facebook_df
    UNION ALL
    SELECT * FROM google_df
    UNION ALL
    SELECT * FROM huawei_df
    UNION ALL
    SELECT * FROM mytarget_df
    UNION ALL
    SELECT * FROM tiktok_df
    UNION ALL
    SELECT * FROM vkontakte_df
    UNION ALL
    SELECT * FROM yandex_df
),

-- таблицу по всем источникам объединяем с таблицей с установками
final AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        IFNULL(IF(l.installs IS NULL, r.installs, l.installs), 0) installs,
        IFNULL(clicks, 0) clicks,
        IFNULL(impressions, 0) impressions,
        IFNULL(costs, 0) costs
    FROM installs r
    FULL OUTER JOIN all_sources l
    USING (date, campaign_name, adset_name, platform, source)
)

SELECT *
FROM final