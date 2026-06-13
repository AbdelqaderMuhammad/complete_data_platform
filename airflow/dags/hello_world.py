
from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="hello_world",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["manually_triggered_dag"],
)
def hello_world_dag():
    

    @task
    def print_hello():
        print("Hello, World!")

    @task
    def print_goodbye():
        print("Goodbye, World!")


    # task dependencies
    print_hello() >> print_goodbye()

    

hello_world_dag()