from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'master_orchestrator',
        default_args=default_args,
        description='Master DAG to orchestrate all ingestion pipelines',
        schedule_interval='@daily',
        catchup=False,
) as dag:
    start = DummyOperator(task_id='start')

    # Trigger individual ingestion DAGs
    trigger_postgres = TriggerDagRunOperator(
        task_id='trigger_postgres_ingestion',
        trigger_dag_id='postgres_to_bronze',
        wait_for_completion=True,
    )

    trigger_pharmacy = TriggerDagRunOperator(
        task_id='trigger_pharmacy_ingestion',
        trigger_dag_id='pharmacy_api_to_bronze',
        wait_for_completion=True,
    )

    trigger_lab = TriggerDagRunOperator(
        task_id='trigger_lab_ingestion',
        trigger_dag_id='lab_csv_to_bronze',
        wait_for_completion=True,
    )

    end = DummyOperator(task_id='end')

    # Define workflow
    start >> [trigger_postgres, trigger_pharmacy, trigger_lab] >> end