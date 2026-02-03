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
        'master_orchestrator_local',
        default_args=default_args,
        description='Master DAG for LOCAL development (no AWS access)',
        schedule_interval='@daily',
        catchup=False,
        tags=['orchestration', 'master', 'local']
) as dag:
    start = DummyOperator(task_id='start')

    # Trigger individual LOCAL ingestion DAGs in parallel
    trigger_postgres = TriggerDagRunOperator(
        task_id='trigger_postgres_ingestion_local',
        trigger_dag_id='postgres_to_bronze_local',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_pharmacy = TriggerDagRunOperator(
        task_id='trigger_pharmacy_ingestion_local',
        trigger_dag_id='pharmacy_api_to_bronze_local',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_lab = TriggerDagRunOperator(
        task_id='trigger_lab_ingestion_local',
        trigger_dag_id='lab_csv_to_bronze_local',
        wait_for_completion=True,
        poke_interval=30,
    )

    end = DummyOperator(task_id='end')

    # Define workflow
    start >> [trigger_postgres, trigger_pharmacy, trigger_lab] >> end