"""
#1 @task decorator 
#2 classics of tasks operator: PythonOperator, BashOperator
#3 @task.branch 
#4 @task.short_circuit
#5 @task_group
#6 tasks dependencies: set_upstream, set_downstream, >>, <<
"""
from airflow import DAG
from datetime import datetime
from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="tasks_features_dag",
    start_date=datetime(2026, 6, 22),
    schedule=None,
    catchup=False,
    tags=["tasks_features"]
) as dag:
    
    @task
    def extract_data():
        print("Extracting data...")
        return {"user_count": 150, "status": "active"}
    
        
    @task.short_circuit
    def verify_system_health():
        print("Verifying system health...")
        system_health = True
        return system_health
    
    log_status = BashOperator(
        task_id = "log_status_classic",
        bash_command="echo 'the status is: {{ ti.xcom_pull(task_ids=\'extract_data\')[\'status\'] }} \
                    and count {{ ti.xcom_pull(task_ids=\'extract_data\')[\'user_count\']}}'"
    )
    
    @task.branch
    def check_user_count(data):
        count = data['user_count']
        if count > 100:
            return "high_volumn_processing"
        return "process_low_volumn"
    
    
    @task_group(group_id="high_volumn_processing")
    def process_high_volumn_group():

        @task(task_id="process_high_volumn_1")
        def process_high_volumn():
            print("processing high volumn data .. ")

        @task(task_id="process_high_volumn_2")
        def process_high_volumn_v2():
            print("processing high volumn data v2 .. ")
        
    @task
    def process_low_volumn():
        print("Processing low volumn data ...")
        
    
    @task(trigger_rule="always")
    def finalize_processing():
        print("Finalizing processing ...")
        
    @task(trigger_rule="all_success")
    def success_processing():
        print("success ...")

    
    health_check = verify_system_health()
    extracted_info = extract_data()
    always_log = finalize_processing()
    always_success = success_processing()
    
    health_check >> extracted_info
    
    extracted_info >> log_status
    
    branch_decision = check_user_count(extracted_info)
    
    branch_decision >> process_high_volumn_group() >> always_log >> always_success
    branch_decision >> process_low_volumn() >> always_log >> always_success
