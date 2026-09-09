SELECT *
FROM {{ ref('stg_weather_snapshots') }}
WHERE precipitation_mm < 0