from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
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

with DAG(
        'master_silver_orchestrator',
        default_args=default_args,
        description='Master DAG to orchestrate Silver layer transformations',
        schedule_interval='@daily',
        catchup=False,
        tags=['orchestration', 'silver', 'transformation']
) as dag:
    start = DummyOperator(task_id='start')

    # Trigger Silver transformation DAG
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transformation',
        trigger_dag_id='bronze_to_silver',
        wait_for_completion=True,
        poke_interval=30,
    )

    end = DummyOperator(task_id='end')

    # Define workflow
    start >> trigger_silver >> end