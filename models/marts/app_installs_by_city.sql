WITH source AS (
    SELECT
        install_time,
        platform,
        city
    FROM {{ ref('stg_app_installs') }}
),

modified AS (
    SELECT
        city,
        COUNT( DISTINCT IF(platform = 'android', install_time, NULL)) AS android_installs, 
        COUNT( DISTINCT IF(platform = 'ios', install_time, NULL)) AS ios_installs, 
    FROM source
    GROUP BY 1
    ORDER BY 2 DESC
)

SELECT
    city,
    android_installs,
    ios_installs
FROM modified