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

red_stg_tiktok as --382 cтроки
(SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    'tiktok' as source,
    clicks, 
    impressions,
    --installs, 
    costs
FROM {{ ref('stg_tiktok') }}
WHERE is_realweb AND NOT is_ret_campaign
),

intersection as
(select 
    date,
    campaign_name,
    adset_name,
from grouped_stg_af_installs
where source = 'tiktok'
intersect distinct
select 
    date,
    campaign_name,
    adset_name
from red_stg_tiktok
),

exception_af as
(select 
    date,
    campaign_name,
    adset_name,
from grouped_stg_af_installs
where source = 'tiktok'
except distinct
select 
    date,
    campaign_name,
    adset_name
from red_stg_tiktok
),

exception_tiktok as
(select 
    date,
    campaign_name,
    adset_name,
from red_stg_tiktok
except distinct
select 
    date,
    campaign_name,
    adset_name
from grouped_stg_af_installs
)


select    
    i.date,
    i.campaign_name,
    i.adset_name,
    af.platform,
    af.source,
    t.clicks, 
    t.impressions,
    af.installs, 
    t.costs
from intersection i
 join red_stg_tiktok t on (i.date=t.date) and (i.campaign_name=t.campaign_name) and (i.adset_name=t.adset_name)
    join grouped_stg_af_installs af on (i.date=af.date) and (i.campaign_name=af.campaign_name) and (i.adset_name=af.adset_name)
union distinct
select    
    e.date,
    e.campaign_name,
    e.adset_name,
    af.platform,
    af.source,
    0 clicks, 
    0 impressions,
    af.installs, 
    0 costs
from exception_af e
    join grouped_stg_af_installs af on (e.date=af.date) and (e.campaign_name=af.campaign_name) and (e.adset_name=af.adset_name)
union distinct
select    
    et.date,
    et.campaign_name,
    et.adset_name,
    t.platform,
    t.source,
    t.clicks, 
    t.impressions,
    0 installs, 
    t.costs
from exception_tiktok et
    join red_stg_tiktok t on (et.date=t.date) and (et.campaign_name=t.campaign_name) and (et.adset_name=t.adset_name)
