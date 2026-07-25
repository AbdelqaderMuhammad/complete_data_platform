from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator

from dags.ingestion import raw_table_asset


@dag(
    dag_id="nyc_taxi_transformation",
    schedule=[raw_table_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "dbt"],
)
def nyc_taxi_transformation():

    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/airflow/dbt && /opt/dbt-venv/bin/dbt run",
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/airflow/dbt && /opt/dbt-venv/bin/dbt test",
    )

    run_dbt_transformations >> run_dbt_tests


nyc_taxi_transformation()
