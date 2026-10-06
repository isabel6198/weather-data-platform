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

   {{ weather_condition('weather_code') }}
    AS weather_condition,

    source,
    ingested_at

FROM {{ ref('stg_weather_snapshots') }}