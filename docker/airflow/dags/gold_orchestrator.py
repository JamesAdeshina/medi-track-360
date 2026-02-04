"""
Gold Layer Orchestrator
Triggers the Silver to Gold transformation DAG
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


def log_gold_start():
    """Log the start of Gold transformation"""
    print("=" * 60)
    print("Starting Gold Layer Transformation")
    print("Silver → Gold: Creating analytics-ready star schema")
    print("=" * 60)
    return True


def log_gold_complete():
    """Log the completion of Gold transformation"""
    print("=" * 60)
    print("Gold Layer Transformation Complete")
    print("Data warehouse ready for analytics in S3 Gold layer")
    print("=" * 60)
    return True


with DAG(
        'gold_orchestrator',
        default_args=default_args,
        description='Orchestrator for Gold layer transformations (data warehouse)',
        schedule_interval='@daily',
        catchup=False,
        tags=['orchestration', 'gold', 'data_warehouse']
) as dag:
    start = DummyOperator(task_id='start')

    log_start = PythonOperator(
        task_id='log_gold_start',
        python_callable=log_gold_start,
    )

    # Trigger the main Gold transformation DAG
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_silver_to_gold',
        trigger_dag_id='silver_to_gold',
        wait_for_completion=True,
        poke_interval=30,
    )

    log_complete = PythonOperator(
        task_id='log_gold_complete',
        python_callable=log_gold_complete,
    )

    end = DummyOperator(task_id='end')

    # Define workflow
    start >> log_start >> trigger_gold >> log_complete >> end