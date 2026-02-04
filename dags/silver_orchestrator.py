"""
Silver Layer Orchestrator
Triggers the Bronze to Silver transformation DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def log_silver_start():
    """Log the start of Silver transformation"""
    print("=" * 60)
    print("Starting Silver Layer Transformation")
    print("Bronze → Silver: Cleaning and standardizing data")
    print("=" * 60)
    return True


def log_silver_complete():
    """Log the completion of Silver transformation"""
    print("=" * 60)
    print("Silver Layer Transformation Complete")
    print("Clean data now available in S3 Silver layer")
    print("=" * 60)
    return True


with DAG(
        'silver_orchestrator',
        default_args=default_args,
        description='Orchestrator for Silver layer transformations',
        schedule_interval='@daily',
        catchup=False,
        tags=['orchestration', 'silver', 'transformation']
) as dag:
    start = DummyOperator(task_id='start')

    log_start = PythonOperator(
        task_id='log_silver_start',
        python_callable=log_silver_start,
    )

    # Trigger the main Silver transformation DAG
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_bronze_to_silver',
        trigger_dag_id='bronze_to_silver',
        wait_for_completion=True,
        poke_interval=30,
    )

    log_complete = PythonOperator(
        task_id='log_silver_complete',
        python_callable=log_silver_complete,
    )

    end = DummyOperator(task_id='end')

    # Define workflow
    start >> log_start >> trigger_silver >> log_complete >> end