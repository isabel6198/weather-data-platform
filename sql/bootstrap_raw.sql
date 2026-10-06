CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_snapshots (
    city                    TEXT NOT NULL,
    latitude                DOUBLE PRECISION NOT NULL,
    longitude               DOUBLE PRECISION NOT NULL,
    weather_time            TIMESTAMPTZ NOT NULL,

    temperature_2m          DOUBLE PRECISION,
    relative_humidity_2m    INTEGER,
    precipitation           DOUBLE PRECISION,
    weather_code            INTEGER,
    wind_speed_10m          DOUBLE PRECISION,

    source                  TEXT NOT NULL DEFAULT 'open-meteo',
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (city, weather_time)
);


CREATE TABLE IF NOT EXISTS raw.city_metadata (
    city        TEXT PRIMARY KEY,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    elevation   DOUBLE PRECISION,
    timezone    TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
