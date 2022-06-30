SELECT COUNT(*) counter
FROM {{ ref('stg_facebook') }}