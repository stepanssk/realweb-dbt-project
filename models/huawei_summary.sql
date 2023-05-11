{{
  config(
    materialized='table')
}}


with grouped_stg_af_installs AS --7327 cтрок
(
SELECT DISTINCT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    count(distinct appsflyer_id) installs

FROM {{ ref('stg_af_installs') }} 
WHERE is_realweb AND NOT is_ret_campaign AND source!='other'
group by 1,2,3,4,5
),

red_stg_huawei as --382 cтроки
(SELECT
    date,
    campaign_name,
    --adset_name,
    platform,
    'huawei' as source,
    clicks, 
    impressions,
    --installs, 
    costs
FROM {{ ref('stg_huawei_ads') }}
WHERE is_realweb AND NOT is_ret_campaign
),

intersection as
(select 
    date,
    campaign_name,
    --adset_name,
from grouped_stg_af_installs
where source = 'huawei'
intersect distinct
select 
    date,
    campaign_name,
    --adset_name
from red_stg_huawei
),

exception_af as
(select 
    date,
    campaign_name,
    --adset_name,
from grouped_stg_af_installs
where source = 'huawei'
except distinct
select 
    date,
    campaign_name,
    --adset_name
from red_stg_huawei
),

exception_huawei as
(select 
    date,
    campaign_name,
    --adset_name,
from red_stg_huawei
except distinct
select 
    date,
    campaign_name,
    --adset_name
from grouped_stg_af_installs
)


select    
    i.date,
    i.campaign_name,
    af.adset_name,
    af.platform,
    af.source,
    t.clicks, 
    t.impressions,
    af.installs, 
    t.costs
from intersection i
 join red_stg_huawei t on (i.date=t.date) and (i.campaign_name=t.campaign_name) 
    join grouped_stg_af_installs af on (i.date=af.date) and (i.campaign_name=af.campaign_name)
union distinct
select    
    e.date,
    e.campaign_name,
    af.adset_name,
    af.platform,
    af.source,
    0 clicks, 
    0 impressions,
    af.installs, 
    0 costs
from exception_af e
    join grouped_stg_af_installs af on (e.date=af.date) and (e.campaign_name=af.campaign_name) 
union distinct
select    
    et.date,
    et.campaign_name,
    null adset_name,
    t.platform,
    t.source,
    t.clicks, 
    t.impressions,
    0 installs, 
    t.costs
from exception_huawei et
    join red_stg_huawei t on (et.date=t.date) and (et.campaign_name=t.campaign_name) 
