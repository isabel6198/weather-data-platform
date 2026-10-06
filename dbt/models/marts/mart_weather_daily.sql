SELECT

    city,
    weather_date,

    MIN(temperature_c)
        AS min_temperature_c,

    MAX(temperature_c)
        AS max_temperature_c,

    ROUND(
        AVG(temperature_c)::numeric,
        2
    ) AS avg_temperature_c,

    ROUND(
        AVG(humidity_pct)::numeric,
        2
    ) AS avg_humidity_pct,

    SUM(precipitation_mm)
        AS total_precipitation_mm,

    ROUND(
        AVG(wind_speed_kmh)::numeric,
        2
    ) AS avg_wind_speed_kmh,

    COUNT(*)
        AS observation_count

FROM {{ ref('fct_weather_hourly') }}

GROUP BY
    city,
    weather_date