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

WITH af_installs AS (
    SELECT DISTINCT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT(appsflyer_id)) AS installs
    FROM
        {{ ref('stg_af_installs') }}
    WHERE
        is_realweb
        AND source != 'other'
        AND NOT is_ret_campaign
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
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        SUM(installs) AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

google AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        SUM(installs) AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

huawei AS (
    SELECT 
        date,
        campaign_name,
        '' AS adset_name,
        platform,
        'huawei' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

mytarget AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_mytarget') }} 
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

tiktok AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

vk AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_vkontakte') }} 
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

yandex AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        NULL AS installs,
        SUM(impressions) AS impressions
    FROM {{ ref('stg_yandex') }} 
    WHERE is_realweb AND NOT is_ret_campaign
    GROUP BY 1,2,3,4,5
),

union_table AS (
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
    SELECT * FROM vk
    UNION ALL 
    SELECT * FROM yandex
),

result_table AS (
     SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        IFNULL(clicks, 0) AS clicks,
        IFNULL(costs, 0) AS costs,
        IFNULL(IF(ut.installs IS NOT NULL, ut.installs, af.installs), 0) AS installs,
        IFNULL(impressions, 0) as impressions
    FROM af_installs AS af
    FULL JOIN union_table AS ut
    USING (date, campaign_name, adset_name, platform, source)
)

SELECT * FROM result_table
