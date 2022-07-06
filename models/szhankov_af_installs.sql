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

WITH af_installs_data AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        SUM(1) AS installs
    FROM {{ ref('stg_af_installs') }} 
    WHERE source!='other' AND is_realweb AND NOT is_ret_campaign
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
        platform,
        'facebook' AS source,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        costs,
        installs
    FROM {{ ref('stg_facebook') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

google_ads AS (
    SELECT
        date,
        platform,
        'google_ads' AS source,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        costs,
        installs
    FROM {{ ref('stg_google_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

huawei AS (
    SELECT
        date,
        platform,
        'huawei' AS source,
        campaign_name,
        'not_set' AS adset_name,
        impressions,
        clicks,
        costs,
        NULL AS installs
    FROM {{ ref('stg_huawei_ads') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

mytarget AS (
    SELECT
        date,
        platform,
        'mytarget' AS source,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        costs,
        NULL AS installs
    FROM {{ ref('stg_mytarget') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

tiktok AS (
    SELECT
        date,
        platform,
        'tiktok' AS source,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        costs,
        NULL AS installs
    FROM {{ ref('stg_tiktok') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

vkontakte AS (
    SELECT
        date,
        platform,
        'vkontakte' AS source,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        costs,
        NULL AS installs
    FROM {{ ref('stg_vkontakte') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

yandex AS (
    SELECT
        date,
        platform,
        'yandex' AS source,
        campaign_name,
        adset_name,
        impressions,
        clicks,
        costs,
        NULL AS installs
    FROM {{ ref('stg_yandex') }}
    WHERE is_realweb AND NOT is_ret_campaign
),

all_sources AS (
    SELECT * FROM facebook UNION ALL
    SELECT * FROM google_ads UNION ALL
    SELECT * FROM huawei UNION ALL
    SELECT * FROM mytarget UNION ALL
    SELECT * FROM tiktok UNION ALL
    SELECT * FROM vkontakte UNION ALL
    SELECT * FROM yandex
),

all_sources_with_af_installs AS (
    SELECT 
        date,
        platform,
        source,
        campaign_name,
        adset_name,
        l.impressions,
        l.clicks,
        l.costs,
        IF(l.installs IS NOT NULL, l.installs, r.installs) AS installs
    FROM all_sources AS l
    FULL OUTER JOIN af_installs_data AS r
    USING (date, campaign_name, adset_name, platform, source)
    WHERE platform = 'ios' OR platform = 'android'
)

SELECT
    date,
    source,
    campaign_name,
    adset_name,
    platform,
    impressions,
    clicks,
    costs,
    installs
FROM all_sources_with_af_installs
