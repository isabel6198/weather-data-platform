{% macro weather_condition(code_column) %}

    CASE
        WHEN {{ code_column }} = 0
            THEN 'clear'

        WHEN {{ code_column }} IN (1, 2)
            THEN 'partly_cloudy'

        WHEN {{ code_column }} = 3
            THEN 'overcast'

        WHEN {{ code_column }} IN (45, 48)
            THEN 'fog'

        WHEN {{ code_column }} BETWEEN 51 AND 57
            THEN 'drizzle'

        WHEN {{ code_column }} BETWEEN 61 AND 67
            THEN 'rain'

        WHEN {{ code_column }} BETWEEN 71 AND 77
            THEN 'snow'

        WHEN {{ code_column }} BETWEEN 80 AND 82
            THEN 'rain_showers'

        WHEN {{ code_column }} BETWEEN 85 AND 86
            THEN 'snow_showers'

        WHEN {{ code_column }} BETWEEN 95 AND 99
            THEN 'thunderstorm'

        ELSE 'other'
    END

{% endmacro %}