SELECT

   {{ dbt_utils.generate_surrogate_key([
    'city',
    'weather_time'
    ]) }} AS observation_id,

    trim(city) AS city,

    latitude,
    longitude,

    weather_time AS observed_at,

    temperature_2m AS temperature_c,
    relative_humidity_2m AS humidity_pct,
    precipitation AS precipitation_mm,
    weather_code,
    wind_speed_10m AS wind_speed_kmh,

    source,
    ingested_at

FROM {{ source('raw', 'weather_snapshots') }}