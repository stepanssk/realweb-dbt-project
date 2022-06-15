{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
  )
}}

{% endif %}

WITH source AS (
    SELECT
        transaction_id,
        order_date,
        SAFE_CAST(revenue AS FLOAT64) AS revenue,
        user_id,
        platform,
        promo_code,
        discount,
        delivery_type,
        region,
        status_id
    FROM {{ source('hackaton', 'crm_data') }}
)

SELECT *
FROM source