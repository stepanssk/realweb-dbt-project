{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
    cluster_by = ["source"]
  )
}}

{% endif %}


WITH installs AS (
  SELECT 
    date,
    campaign_name,
    adset_name,
    media_source,
    platform,
    appsflyer_id,
    source
  FROM {{ ref('stg_af_installs') }}
  WHERE is_realweb AND source!='other' AND is_ret_campaign = FALSE 
),

installs_gr AS (
  SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT(DISTINCT appsflyer_id) as installs_gr_cnt
  FROM installs
  GROUP BY 1,2,3,4,5
  
  ),

fb AS(
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

google_ads AS(
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
  FROM  {{ ref('stg_google_ads') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

huawei_ads AS(
  SELECT 
    date,
    campaign_name,
    'huawei_ads' as adset_name,
    platform,
    'huawei_ads' as source,
    clicks,
    impressions,
    NULL as installs,
    costs,
  FROM  {{ ref('stg_huawei_ads') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

mytarget AS(
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
  FROM  {{ ref('stg_mytarget') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

tiktok AS(
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
  FROM  {{ ref('stg_tiktok') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

vkontakte AS(
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
  FROM  {{ ref('stg_vkontakte') }}
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
  FROM  {{ ref('stg_yandex') }}
  WHERE is_realweb AND is_ret_campaign = FALSE 
),

joined AS
(
  SELECT * FROM fb
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

joined_ AS (
    SELECT
        COALESCE(installs_gr.date, joined.date) date,
        COALESCE(installs_gr.campaign_name, joined.campaign_name) campaign_name,
        COALESCE(installs_gr.adset_name, joined.adset_name) adset_name,
        COALESCE(installs_gr.platform, joined.platform) platform,
        COALESCE(installs_gr.source, joined.source) source,
        COALESCE(clicks,0) clicks,
        COALESCE(costs,0) costs,
        COALESCE(installs, installs_gr_cnt, 0) installs,
        COALESCE(impressions,0) impressions
    FROM installs_gr
    FULL OUTER JOIN joined
    ON installs_gr.date = joined.date
    AND installs_gr.adset_name = joined.adset_name
    AND installs_gr.platform = joined.platform
    AND installs_gr.source = joined.source
    AND installs_gr.campaign_name = joined.campaign_name
),

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
    FROM joined_
    WHERE clicks + costs + installs + impressions > 0
    -- AND
    )


SELECT * FROM final
WHERE platform in ('ios', 'android')
-- order by 1,2,3,4,5