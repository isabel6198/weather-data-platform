
SELECT
        city,
        latitude,
        longitude,
        elevation,
        timezone,

        dbt_valid_from AS valid_from

FROM    {{ ref ('city_metadata_snapshot')}}

WHERE dbt_valid_to IS NULL