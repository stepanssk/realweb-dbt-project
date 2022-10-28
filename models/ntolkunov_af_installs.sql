{%- if target.name == 'prod' -%}
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
{%- endif %}

--отфильтруем данные по установкам:
--только кампании Риалвеба (is_realweb=TRUE), только с известным источником (source!='other'), 
--только User Acquisition (is_ret_campaign=FALSE), только ios и android (platform IN ('ios', 'android'))
WITH installs AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        appsflyer_id,
        source
    FROM {{ ref('stg_af_installs') }}
    WHERE is_realweb AND source != 'other' AND NOT is_ret_campaign AND platform IN ('ios', 'android')
),
--сгруппируем по дате, кампании, группе объявлений, платформе и источнику,
--посчитаем общее число установок для каждой группы
grouped_installs AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) count_installs
    FROM installs
    GROUP BY 1,2,3,4,5
),

{#- создадим список названий таблиц с источниками -#}
{%- set sources = ['facebook', 'google_ads', 'huawei_ads', 'tiktok', 'vkontakte', 'yandex', 'mytarget'] -%}

{#- #}
--выберем обработанные данные по источникам
{%- for source in sources -%}
{{ process_source_table(source) }},
{%- endfor -%}

{#- #}
--объединим данные по источникам
all_sources AS (
{%- for source in sources %}
    SELECT * FROM {{ source }}
    {{ 'UNION ALL' if not loop.last }}
{%- endfor %}
),
--обогатим таблицу установок данными по источникам
joined_data AS (
    SELECT
        COALESCE(grouped_installs.date, all_sources.date) date,
        COALESCE(grouped_installs.campaign_name, all_sources.campaign_name) campaign_name,
        COALESCE(grouped_installs.adset_name, all_sources.adset_name, '-') adset_name,
        COALESCE(grouped_installs.platform, all_sources.platform) platform,
        COALESCE(grouped_installs.source, all_sources.source) source,
        COALESCE(clicks, 0) clicks,
        COALESCE(costs, 0) costs,
        COALESCE(all_sources.installs, grouped_installs.count_installs, 0) installs,
        COALESCE(impressions, 0) impressions
    FROM grouped_installs
    FULL OUTER JOIN all_sources
    USING (date, adset_name, platform, source, campaign_name)
)

SELECT * 
FROM joined_data
WHERE clicks + costs + installs + impressions > 0