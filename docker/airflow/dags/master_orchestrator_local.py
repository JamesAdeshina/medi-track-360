from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'master_orchestrator_local_simple',
        default_args=default_args,
        description='Simple master DAG for LOCAL development',
        schedule_interval='@daily',
        catchup=False,
        tags=['orchestration', 'master', 'local', 'simple']
) as dag:
    start = DummyOperator(task_id='start')

    # Run PostgreSQL extraction
    run_postgres = BashOperator(
        task_id='run_postgres_extraction',
        bash_command='airflow dags trigger postgres_to_bronze_local',
    )

    # Run Pharmacy extraction
    run_pharmacy = BashOperator(
        task_id='run_pharmacy_extraction',
        bash_command='airflow dags trigger pharmacy_api_to_bronze_local',
    )

    # Run Lab extraction
    run_lab = BashOperator(
        task_id='run_lab_extraction',
        bash_command='airflow dags trigger lab_csv_to_bronze_local',
    )

    end = DummyOperator(task_id='end')

    # Define workflow - run in parallel
    start >> [run_postgres, run_pharmacy, run_lab] >> end