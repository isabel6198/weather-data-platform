{% snapshot city_metadata_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='city',
        strategy='timestamp',
        updated_at='snapshot_updated_at'
    )
}}

SELECT
    city,
    latitude,
    longitude,
    elevation,
    timezone,
    updated_at,

    updated_at AT TIME ZONE 'UTC'
        AS snapshot_updated_at

FROM {{ source('raw', 'city_metadata') }}

{% endsnapshot %}