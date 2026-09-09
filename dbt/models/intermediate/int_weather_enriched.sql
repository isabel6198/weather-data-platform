SELECT

    observation_id,
    city,
    latitude,
    longitude,

    observed_at,

    observed_at::date AS weather_date,

    EXTRACT(
        HOUR FROM observed_at
    )::integer AS weather_hour,

    temperature_c,
    humidity_pct,
    precipitation_mm,
    weather_code,
    wind_speed_kmh,

    CASE
        WHEN precipitation_mm > 0
            THEN TRUE
        ELSE FALSE
    END AS is_raining,

    CASE
        WHEN weather_code = 0
            THEN 'clear'

        WHEN weather_code IN (1, 2)
            THEN 'partly_cloudy'

        WHEN weather_code = 3
            THEN 'overcast'

        WHEN weather_code IN (45, 48)
            THEN 'fog'

        WHEN weather_code BETWEEN 51 AND 57
            THEN 'drizzle'

        WHEN weather_code BETWEEN 61 AND 67
            THEN 'rain'

        WHEN weather_code BETWEEN 71 AND 77
            THEN 'snow'

        WHEN weather_code BETWEEN 80 AND 82
            THEN 'rain_showers'

        WHEN weather_code BETWEEN 85 AND 86
            THEN 'snow_showers'

        WHEN weather_code BETWEEN 95 AND 99
            THEN 'thunderstorm'

        ELSE 'other'

    END AS weather_condition,

    source,
    ingested_at

FROM {{ ref('stg_weather_snapshots') }}