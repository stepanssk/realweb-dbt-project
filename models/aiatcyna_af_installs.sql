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

/* таблица с количеством установок с в разбивке по дате, кампании, группе объявления,
 платформе и источнику (рекламному кабинету).
 Добавлен фильтр по заданным условиям*/

WITH installs_data AS (
    SELECT 
        date, 
        campaign_name, 
        adset_name, 
        platform, 
        source, 
        COUNT(appsflyer_id) AS installs 
  FROM {{ ref('stg_af_installs') }} 
  WHERE is_realweb 
        AND NOT is_ret_campaign 
        AND source != 'other' 
  GROUP BY 1, 2, 3, 4, 5
  ),

/* далее идут таблицы по каждому из источников с данными о кликах, показах, установках, расходах. 
Добавлен фильтр по заданным условиям*/

    facebook_data AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            'facebook' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            SUM(installs) AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_facebook') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

    google_data AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            'google_ads' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            SUM(installs) AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_google_ads') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

    huawei_data AS (
        SELECT 
            date,
            campaign_name,
            'n/a' AS adset_name, -- нет данных, добавляем поле, заполняем "n/a"
            platform,
            'huawei' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            NULL AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_huawei_ads') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

    mytarget_data AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            'mytarget' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            NULL AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_mytarget') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

    tiktok_data AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            'tiktok' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            NULL AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_tiktok') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

    vk_data AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            'vkontakte' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            NULL AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_vkontakte') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

        yandex_data AS (
        SELECT 
            date,
            campaign_name,
            adset_name,
            platform,
            'yandex' AS source, -- нет поля с источником, добавляем свое 
            SUM(clicks) AS clicks,
            SUM(impressions) AS impressions,
            NULL AS installs,
            SUM(costs) AS costs 
        FROM {{ ref('stg_yandex') }} 
        WHERE is_realweb 
            AND NOT is_ret_campaign 
        GROUP BY 1, 2, 3, 4
        ),

-- объединяем таблицы для всех рекламных кабинетов в одну

    union_data AS (
        SELECT * FROM facebook_data
        UNION ALL
        SELECT * FROM google_data
        UNION ALL
        SELECT * FROM huawei_data
        UNION ALL
        SELECT * FROM mytarget_data
        UNION ALL
        SELECT * FROM tiktok_data
        UNION ALL
        SELECT * FROM vk_data
        UNION ALL
        SELECT * FROM yandex_data
    ),

-- объединяем данные из рекламных кабинетов с таблицей с установками 

    final_data AS (
        SELECT date,
            campaign_name,
            adset_name,
            platform,
            source,
            IFNULL(un.clicks, 0) AS clicks,
            IFNULL(un.impressions, 0) AS impressions,
            IFNULL(IFNULL(un.installs, inst.installs), 0) AS installs,
            IFNULL(un.costs, 0.0) AS costs
    FROM union_data AS un
    FULL OUTER JOIN installs_data AS inst
    USING (date, campaign_name, adset_name, platform, source))
    
SELECT *
FROM final_data









    
    










