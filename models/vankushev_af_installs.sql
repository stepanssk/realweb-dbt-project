--Материализуем таблицу и запускаем в прод.
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

-- Ход выполнения задачи:
-- 1) формируем агрегированные данные из stg_af_installs
-- 2) подтягиваем данные из других источников
-- 3) связываем вместе все таблицы с каналами 
-- 4) дополняем + связываем: установки и источники
-- 5) фильтруем и тестим финальную таблицу

-- №1: формирование агрегированных данных из stg_af_installs
WITH af_data AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT appsflyer_id) AS installs -- кол-во установок
    FROM 
        {{ ref('stg_af_installs') }} 
    WHERE 
        is_realweb = TRUE  
    AND source != 'other' 
    AND is_ret_campaign = FALSE
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform,
        source
),

-- №2: подтягиваем данные из других источников (facebook)
facebook_data AS (
  SELECT 
        date, 
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source, --добавляем группу источника
        SUM(installs) AS installs,
        SUM(clicks) AS clicks,
        SUM(costs) AS costs,
        SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_facebook') }}
  WHERE
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
),

-- №2: подтягиваем данные из других источников (google ads)
google_ads_data AS (
  SELECT 
    date, 
    campaign_name,
    adset_name,
    platform,
    'google_ads' AS source, --добавляем группу источника
    SUM(installs) AS installs,
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_google_ads') }}
  WHERE
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
),

-- №2: подтягиваем данные из других источников (huawei)
huawei_data AS (
  SELECT 
    date, 
    campaign_name,
    ' ' as adset_name, --добавляем пустую группу для группы объявлений
    platform,
    'huawei' AS source, --добавляем группу источника
    NULL as installs, --добавляем нулевые установки
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_huawei_ads') }}
  WHERE
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
),

-- №2: подтягиваем данные из других источников (mytarget)
my_target_data AS (
  SELECT 
    date, 
    campaign_name,
    adset_name,
    platform,
    'mytarget' AS source, --добавляем группу источника
    NULL as installs, --добавляем нулевые установки
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_mytarget') }}
  WHERE
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
),

-- №2: подтягиваем данные из других источников (tiktok)
tiktok_data AS (
  SELECT 
    date, 
    campaign_name,
    adset_name,
    platform,
    'tiktok' AS source, --добавляем группу источника
    NULL as installs, --добавляем нулевые установки
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_tiktok') }}
  WHERE
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
),

-- №2: подтягиваем данные из других источников (vk)
vkontakte_data AS (
   SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'vkontakte' AS source, --добавляем группу источника
    NULL AS installs, --добавляем нулевые установки
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_vkontakte') }}
  WHERE 
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
 ),

-- №2: подтягиваем данные из других источников (yandex)
 yandex_data AS (
   SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    'yandex' AS source, --добавляем группу источника
    NULL AS installs, --добавляем нулевые установки
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions
  FROM 
    {{ ref('stg_yandex') }}
  WHERE 
    is_ret_campaign = FALSE
  AND 
    is_realweb = TRUE
  GROUP BY 
    date,
    campaign_name,
    adset_name,
    platform
 ),

-- №3: связываем вместе все таблицы с каналами 
connected_source_data AS (
    SELECT * FROM facebook_data
    UNION ALL --связываем с дублями
    SELECT * FROM google_ads_data
    UNION ALL 
    SELECT * FROM huawei_data
    UNION ALL 
    SELECT * FROM my_target_data
    UNION ALL 
    SELECT * FROM tiktok_data
    UNION ALL 
    SELECT * FROM vkontakte_data
    UNION ALL 
    SELECT * FROM yandex_data
),

-- №4: дополняем + связываем: установки и источники
final_data AS (
    -- Дополняем данные 
    SELECT 
        COALESCE(connected_source_data.date, 
                af_data.date) AS date,
        COALESCE(connected_source_data.campaign_name, 
                 af_data.campaign_name) AS campaign_name,
        COALESCE(connected_source_data.adset_name, 
                 af_data.adset_name) AS adset_name,
        COALESCE(connected_source_data.platform, 
                 af_data.platform) AS platform,
        COALESCE(connected_source_data.source, 
                 af_data.source) AS source,
        COALESCE(connected_source_data.installs, 
                 af_data.installs) AS installs,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(costs, 0) AS costs,
        COALESCE(impressions, 0) AS impressions
    FROM  
        af_data
    
    -- Связываем таблицы
    FULL OUTER JOIN 
        connected_source_data
    USING(date, campaign_name, adset_name, platform, source)
)

--№5: фильтруем и тестим финальную таблицу
SELECT 
    *
FROM 
    final_data
WHERE 
    campaign_name IS NOT NULL 
AND 
    platform IN ('ios','android')