{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
    cluster_by = ["is_realweb", "is_ret_campaign"]
  )
}}

{% endif %}

WITH source_t AS (
    SELECT DISTINCT
        ad_date,
        campaign_name,
        adset_name,
        clicks,
        spend,
        installs,
        impressions
    FROM {{ source('vprok_for_dbt_ed', 'google_ads') }}
),

transform AS (
    SELECT 
        DATE(ad_date) date,
        {{ process_strings('campaign_name') }} AS campaign_name,
        {{ process_strings('adset_name') }} AS adset_name,
        {{ platform('campaign_name') }} AS platform,
        SUM(SAFE_CAST(clicks AS INT64)) AS clicks,
        SUM(SAFE_CAST(spend AS FLOAT64)) AS costs,
        SUM(SAFE_CAST(installs AS INT64)) AS installs,
        SUM(SAFE_CAST(impressions AS INT64)) AS impressions,
    FROM source_t
    GROUP BY 1,2,3,4
),

final AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        clicks,
        costs,
        installs,
        impressions,
        {{ is_ret_campaign('campaign_name') }} is_ret_campaign,
        {{ is_realweb('campaign_name') }} is_realweb,
    FROM transform
)

SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    is_ret_campaign,
    is_realweb,
    clicks,
    costs,
    installs,
    impressions
FROM final