{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
    cluster_by = ["platform", "source"]
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
    WHERE is_realweb AND NOT is_ret_campaign AND source != 'other'
),

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

sources AS (
    SELECT * FROM fb
    UNION ALL
    SELECT * FROM hw
    UNION ALL
    SELECT * FROM mt
    UNION ALL
    SELECT * FROM tt
    UNION ALL
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM vk
    UNION ALL
    SELECT * FROM ya
),

joined AS (
    SELECT
        COALESCE(grouped.date, sources.date) date,
        COALESCE(grouped.campaign_name, sources.campaign_name) campaign_name,
        COALESCE(grouped.adset_name, sources.adset_name) adset_name,
        COALESCE(grouped.platform, sources.platform) platform,
        COALESCE(grouped.source, sources.source) source,
        clicks,
        costs,
        COALESCE(installs, sum_installs) installs,
        impressions
    FROM grouped
    FULL OUTER JOIN sources
    ON grouped.date = sources.date
    AND grouped.adset_name = sources.adset_name
    AND grouped.platform = sources.platform
    AND grouped.source = sources.source
    AND grouped.campaign_name = sources.campaign_name
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