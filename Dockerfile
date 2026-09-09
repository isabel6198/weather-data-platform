FROM apache/airflow:3.3.1

USER root

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
       dbt-core==1.12.3 \
       dbt-postgres==1.11.0

ENV PATH="/opt/dbt-venv/bin:${PATH}"

USER airflow