from datetime import datetime, timedelta, timezone

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator

CITIES = [
    {"name": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"name": "Lyon", "latitude": 45.7640, "longitude": 4.8357},
    {"name": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
    {"name": "Nantes", "latitude": 47.2184, "longitude": -1.5536},
    {"name": "Montpellier", "latitude": 43.6108, "longitude": 3.8767},
]


@dag(
    dag_id="weather_pipeline",
    schedule="0 * * * *",
    start_date=pendulum.datetime(
        2026, 9, 9,
        tz="Europe/Paris"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "airflow", "dbt"],
)
def weather_pipeline():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=5),
    )
    def ingest_weather():

        import requests

        url = "https://api.open-meteo.com/v1/forecast"

        params = {
            "latitude": ",".join(
                str(city["latitude"]) for city in CITIES
            ),
            "longitude": ",".join(
                str(city["longitude"]) for city in CITIES
            ),
            "current": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "weather_code",
                "wind_speed_10m",
            ]),
            "timezone": "UTC",
        }

        response = requests.get(
            url,
            params=params,
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        # Avec plusieurs coordonnées,
        # Open-Meteo renvoie une liste de réponses.
        if not isinstance(payload, list):
            payload = [payload]

        if len(payload) != len(CITIES):
            raise ValueError(
                f"Nombre de réponses inattendu : "
                f"{len(payload)} au lieu de {len(CITIES)}"
            )

        ingested_at = datetime.now(timezone.utc)

        rows = []

        for city, weather_data in zip(CITIES, payload):

            current = weather_data["current"]

            weather_time = datetime.fromisoformat(
                current["time"]
            )

            if weather_time.tzinfo is None:
                weather_time = weather_time.replace(
                    tzinfo=timezone.utc
                )

            rows.append(
                (
                    city["name"],
                    weather_data["latitude"],
                    weather_data["longitude"],
                    weather_time,
                    current["temperature_2m"],
                    current["relative_humidity_2m"],
                    current["precipitation"],
                    current["weather_code"],
                    current["wind_speed_10m"],
                    "open-meteo",
                    ingested_at,
                )
            )

        hook = PostgresHook(
            postgres_conn_id="data_postgres"
        )

        connection = hook.get_conn()

        insert_sql = """
            INSERT INTO raw.weather_snapshots (
                city,
                latitude,
                longitude,
                weather_time,
                temperature_2m,
                relative_humidity_2m,
                precipitation,
                weather_code,
                wind_speed_10m,
                source,
                ingested_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )

            ON CONFLICT (city, weather_time)
            DO UPDATE SET
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m =
                    EXCLUDED.relative_humidity_2m,
                precipitation = EXCLUDED.precipitation,
                weather_code = EXCLUDED.weather_code,
                wind_speed_10m = EXCLUDED.wind_speed_10m,
                source = EXCLUDED.source,
                ingested_at = EXCLUDED.ingested_at;
        """

        with connection.cursor() as cursor:
            cursor.executemany(
                insert_sql,
                rows,
            )

        connection.commit()
        connection.close()

        print(
            f"{len(rows)} villes chargées "
            f"depuis Open-Meteo."
        )

        return {
            "rows_loaded": len(rows),
            "cities": [
                city["name"] for city in CITIES
            ],
            "ingested_at": ingested_at.isoformat(),
        }


    @task
    def validate_raw(ingestion_metadata):

        hook = PostgresHook(
            postgres_conn_id="data_postgres"
        )

        result = hook.get_first("""
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT city) AS cities_count,
                MAX(weather_time) AS latest_weather_time
            FROM raw.weather_snapshots;
        """)

        rows_count = result[0]
        cities_count = result[1]
        latest_weather_time = result[2]

        print(
            f"Total raw : {rows_count} lignes"
        )

        print(
            f"Villes distinctes : {cities_count}"
        )

        print(
            f"Dernière donnée météo : "
            f"{latest_weather_time}"
        )

        print(
            f"Lignes reçues pendant ce run : "
            f"{ingestion_metadata['rows_loaded']}"
        )

        if ingestion_metadata["rows_loaded"] != len(CITIES):
            raise ValueError(
                "Toutes les villes n'ont pas été chargées."
            )

        if cities_count < len(CITIES):
            raise ValueError(
                "Certaines villes sont absentes de raw."
            )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="""
            cd /opt/dbt &&
            /opt/dbt-venv/bin/dbt source freshness \
                --profiles-dir /opt/dbt_profiles \
                --target dev
        """,
    )

    dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command="""
        cd /opt/dbt &&
        /opt/dbt-venv/bin/dbt build \
            --profiles-dir /opt/dbt_profiles \
            --target dev
    """,
    )
    
        
    @task
    def validate_marts():

        hook = PostgresHook(
            postgres_conn_id="data_postgres"
        )

        result = hook.get_first("""
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT city) AS cities_count,
                MAX(observed_at) AS latest_observation
            FROM dbt_dev.fct_weather_hourly;
        """)

        rows_count = result[0]
        cities_count = result[1]
        latest_observation = result[2]

        print(
            f"Fact météo : {rows_count} observations"
        )

        print(
            f"Nombre de villes : {cities_count}"
        )

        print(
            f"Dernière observation : "
            f"{latest_observation}"
        )

        if rows_count == 0:
            raise ValueError(
                "La fact météo est vide."
            )

        if cities_count != len(CITIES):
            raise ValueError(
                f"Nombre de villes inattendu : "
                f"{cities_count}"
            )

        daily_rows = hook.get_first("""
            SELECT COUNT(*)
            FROM dbt_dev.mart_weather_daily;
        """)[0]

        print(
            f"Mart quotidien : {daily_rows} lignes"
        )

        if daily_rows == 0:
            raise ValueError(
                "Le mart quotidien est vide."
            )

    metadata = ingest_weather()

    raw_validated = validate_raw(metadata)

    marts_validated = validate_marts()


    raw_validated >> dbt_source_freshness
    dbt_source_freshness >> dbt_build
    dbt_build >> marts_validated


weather_pipeline()