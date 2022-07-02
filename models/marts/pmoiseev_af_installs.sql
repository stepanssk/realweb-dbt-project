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

WITH af_installs AS (
    SELECT 
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) as count_installs,
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb AND is_ret_campaign = FALSE AND source != 'other'
    GROUP BY 1,2,3,4,5
),

facebook AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'facebook' as source,
    clicks,
    impressions,
    installs,
    costs,
  FROM {{ ref('stg_facebook') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

google_ads AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'google_ads' as source,
    clicks,
    impressions,
    installs,
    costs,
  FROM {{ ref('stg_google_ads') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

huawei_ads AS (
  SELECT 
    date,
    campaign_name,
    'unknown' as adset_name,
    platform,
    'huawei_ads' as source,
    clicks,
    impressions,
    NULL as installs,
    costs,
  FROM {{ ref('stg_huawei_ads') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

mytarget AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'mytarget' as source,
    clicks,
    impressions,
    NULL as installs,
    costs,
  FROM {{ ref('stg_mytarget') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

tiktok AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'tiktok' as source,
    clicks,
    impressions,
    NULL as installs,
    costs,
  FROM {{ ref('stg_tiktok') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

vkontakte AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'vkontakte' as source,
    clicks,
    impressions,
    NULL as installs,
    costs,
  FROM {{ ref('stg_vkontakte') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

yandex AS(
  SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'yandex' as source,
    clicks,
    impressions,
    NULL as installs,
    costs,
  FROM {{ ref('stg_yandex') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

all_sources AS
(
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

joined_tbl AS (
    SELECT
        COALESCE(af_installs.date, all_sources.date) date,
        COALESCE(af_installs.campaign_name, all_sources.campaign_name) campaign_name,
        COALESCE(af_installs.adset_name, all_sources.adset_name) adset_name,
        COALESCE(af_installs.platform, all_sources.platform) platform,
        COALESCE(af_installs.source, all_sources.source) source,
        COALESCE(clicks, 0) clicks,
        COALESCE(impressions, 0) impressions,
        COALESCE(installs, count_installs, 0) installs,
        COALESCE(costs, 0) costs          
    FROM af_installs
    FULL OUTER JOIN all_sources
    ON af_installs.date = all_sources.date
    AND af_installs.campaign_name = all_sources.campaign_name
    AND af_installs.adset_name = all_sources.adset_name
    AND af_installs.platform = all_sources.platform
    AND af_installs.source = all_sources.source
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
FROM joined_tbl
