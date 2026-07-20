FROM apache/airflow:3.1.0-python3.12

COPY docker/airflow-requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt"


USER root
RUN python3 -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.0 dbt-duckdb==1.10.1
USER airflow