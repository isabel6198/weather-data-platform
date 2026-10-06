{{
    config(
        materialized='incremental',
        unique_key='observation_id',
        incremental_strategy='merge'
    )
}}

SELECT

    observation_id,

    city,
    latitude,
    longitude,

    observed_at,
    weather_date,
    weather_hour,

    temperature_c,
    humidity_pct,
    precipitation_mm,
    wind_speed_kmh,

    weather_code,
    weather_condition,
    is_raining,

    source,
    ingested_at

FROM {{ ref('int_weather_enriched') }}

{% if is_incremental() %}

WHERE ingested_at >= (
    SELECT
        COALESCE(
            MAX(ingested_at),
            '1900-01-01'::timestamptz
        )
        - INTERVAL '2 hours'

    FROM {{ this }}
)

{% endif %}