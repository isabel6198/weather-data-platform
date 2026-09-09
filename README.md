# Weather Data Platform

Pipeline de données météo construit avec Airflow, PostgreSQL, dbt Core et Docker.

## Objectif

Collecter régulièrement des données météo depuis une API publique, les charger dans PostgreSQL, les transformer avec dbt et produire des tables analytiques prêtes à être exploitées.

## Architecture

API météo → Airflow → PostgreSQL Raw → dbt → Staging / Marts

## Stack

- Apache Airflow
- PostgreSQL
- dbt Core
- Docker / Docker Compose
- Python
- Git