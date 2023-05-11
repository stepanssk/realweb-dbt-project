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



with all_data as 
(SELECT *
FROM {{ ref('tiktok_summary') }} 
UNION ALL
SELECT *
FROM {{ ref('facebook_summary') }} 
UNION ALL
SELECT *
FROM {{ ref('google_summary') }} 
UNION ALL
SELECT *
FROM {{ ref('yandex_summary') }} 
UNION ALL
SELECT *
FROM {{ ref('huawei_summary') }} 
UNION ALL
SELECT *
FROM {{ ref('mytarget_summary') }} 
UNION ALL
SELECT *
FROM {{ ref('vk_summary') }})

select 
    *
 from all_data