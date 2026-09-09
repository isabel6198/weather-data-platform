SELECT *
FROM {{ ref('stg_weather_snapshots') }}
WHERE temperature_c < -80
   OR temperature_c > 60