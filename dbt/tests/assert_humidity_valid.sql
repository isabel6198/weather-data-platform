SELECT *
FROM {{ ref('stg_weather_snapshots') }}
WHERE humidity_pct < 0
   OR humidity_pct > 100