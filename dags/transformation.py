from datetime import datetime

from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator

from dags.ingestion import raw_table_asset


@dag(
    dag_id="nyc_taxi_transformation",
    schedule=[raw_table_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transformation", "dbt", "staging"],
)
def nyc_taxi_transformation():

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command="cd /opt/airflow/dbt && /opt/dbt-venv/bin/dbt run --select stg_yellow_taxi_trips",
    )

    run_dbt_staging


nyc_taxi_transformation()