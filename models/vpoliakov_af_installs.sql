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

--агрегируем и фильтруем данные AF
WITH af_installs_agg AS
(
SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT (DISTINCT appsflyer_id) AS af_installs,
FROM
    {{ ref('stg_af_installs') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
    AND source != 'other'
GROUP BY
    1,2,3,4,5
),

--в кабинете yandex нет установок, а в AF - данных по adset yandex
--суммируем клики и т.д. по названию кампании, чтобы не задублировать число установок из-за разных групп
agg_yandex AS 
(
SELECT 
    date,
    campaign_name,
    CAST(NULL AS STRING) AS adset_name,
    platform,
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions,
FROM 
    {{ ref('stg_yandex') }}
GROUP BY
    1, 2, 4
),

--выделяем данные AF по huawei и mytarget в отдельные временные таблицы:
--для них есть группы в AF, но нет групп в кабинетах, при этом есть кампании, попавшие больше чем в одну группу
--чтобы избежать дублирования кликов и показов, агрегируем данные AF по названию кампании и джойним с данным кабинетов
af_mt AS
(
SELECT 
    date,
    campaign_name,
    platform,
    source,
    SUM(af_installs) AS installs,
FROM
    af_installs_agg
WHERE 
    source IN ('mytarget')
GROUP BY 
    1, 2, 3, 4
),

--соединяем данные AF и кабинетов для mytarget
mt_installs AS
(
SELECT 
    agg.date,
    agg.campaign_name,
    c.adset_name,
    agg.platform,
    agg.source,
    c.clicks,
    c.costs,
    agg.installs,
    c.impressions,
FROM 
    af_mt AS agg
LEFT JOIN 
    {{ ref('stg_mytarget') }} AS c ON agg.date = c.date
    AND agg.campaign_name = c.campaign_name
    AND agg.platform = c.platform
),
--собираем данные huawei
af_hw AS
(
SELECT 
    date,
    campaign_name,
    platform,
    source,
    SUM(af_installs) AS installs,
FROM
    af_installs_agg
WHERE 
    source IN ('huawei')
GROUP BY 
    1, 2, 3, 4
),

--соединяем данные AF и кабинетов для huawei
hw_installs AS
(
SELECT 
    hw.date,
    hw.campaign_name,
    CAST (NULL AS STRING) adset_name,
    hw.platform,
    hw.source,
    cab.clicks,
    cab.costs,
    hw.installs,
    cab.impressions,
FROM 
    af_hw AS hw
LEFT JOIN 
    {{ ref('stg_huawei_ads') }} AS cab ON hw.date = cab.date
    AND hw.campaign_name = cab.campaign_name
    AND hw.platform = cab.platform
),

--создаем единую временную таблицу для кабинетов fb и google
--поскольку в них уже есть все нужные данные, объединять с AF не нужно
fb_gl_installs AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    clicks,
    costs,
    installs,
    impressions,
FROM
    {{ ref('stg_google_ads') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    clicks,
    costs,
    installs,
    impressions,
FROM
    {{ ref('stg_facebook') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
),

--выделяем vk
af_vk AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    af_installs AS installs,
FROM 
    af_installs_agg
WHERE
    source IN ('vkontakte')
),

--собираем данные по vk
vk_installs AS
(
SELECT
    af.date,
    af.campaign_name,
    af.adset_name,
    af.platform,
    af.source,
    co.clicks,
    co.costs,
    af.installs,
    co.impressions,
FROM 
    af_vk AS af
LEFT JOIN
    {{ ref('stg_vkontakte') }} AS co ON af.date = co.date
    AND af.campaign_name = co.campaign_name
    AND af.platform = co.platform
),

--собираем данные по ya
af_ya AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    af_installs AS installs,
FROM 
    af_installs_agg
WHERE 
    source IN ('yandex')
),

ya_installs AS
(
SELECT
    ya.date,
    ya.campaign_name,
    ya.adset_name,
    ya.platform,
    ya.source,
    aya.clicks,
    aya.costs,
    ya.installs,
    aya.impressions,
FROM 
    af_ya AS ya
LEFT JOIN
    agg_yandex AS aya ON ya.date = aya.date
    AND ya.campaign_name = aya.campaign_name
    AND ya.platform = aya.platform
),

--добираем данные AF по tiktok и other
af_tk AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    af_installs AS installs,
FROM 
    af_installs_agg
WHERE
    source IN ('tiktok', 'other')
),

tk_installs AS
(
SELECT 
    a.date,
    a.campaign_name,
    a.adset_name,
    a.platform,
    a.source,
    tk.clicks,
    tk.costs,
    a.installs,
    tk.impressions,
FROM 
    af_tk AS a
LEFT JOIN
    {{ ref('stg_vkontakte') }} AS tk ON a.date = tk.date
    AND a.campaign_name = tk.campaign_name
    AND a.adset_name = tk.adset_name
    AND a.platform = tk.platform
),

final AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    vk_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    fb_gl_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    hw_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    mt_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    tk_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    ya_installs
)

SELECT *
FROM final