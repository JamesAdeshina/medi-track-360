"""
Master Orchestrator DAG for MediTrack360
Triggers complete pipeline: Bronze → Silver → Gold
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

def log_start():
    """Log the start of the orchestration"""
    print("=" * 60)
    print("MediTrack360 Master Orchestrator - Starting Complete Pipeline")
    print("Bronze → Silver → Gold")
    print("=" * 60)
    return True

def log_bronze_complete():
    """Log completion of Bronze ingestion"""
    print("=" * 60)
    print("✅ Bronze Layer Complete")
    print("Raw data available in S3 Bronze layer")
    print("=" * 60)
    return True

def log_silver_complete():
    """Log completion of Silver transformation"""
    print("=" * 60)
    print("✅ Silver Layer Complete")
    print("Clean, standardized data available in S3 Silver layer")
    print("=" * 60)
    return True

def log_gold_complete():
    """Log completion of Gold transformation"""
    print("=" * 60)
    print("✅ Gold Layer Complete")
    print("Analytics-ready star schema available in S3 Gold layer")
    print("Data warehouse ready for analysis!")
    print("=" * 60)
    return True

def log_pipeline_complete():
    """Log completion of entire pipeline"""
    print("=" * 60)
    print("🎉 MediTrack360 Pipeline Complete!")
    print("All layers processed successfully")
    print("Data is ready for Redshift loading and Power BI dashboards")
    print("=" * 60)
    return True

with DAG(
    'master_orchestrator',
    default_args=default_args,
    description='Master DAG to orchestrate complete pipeline: Bronze → Silver → Gold',
    schedule_interval='@daily',
    catchup=False,
    tags=['orchestration', 'master', 'bronze', 'silver', 'gold', 'pipeline']
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Log start
    log_start_task = PythonOperator(
        task_id='log_start',
        python_callable=log_start,
    )

    # --- BRONZE LAYER ---
    # Trigger individual ingestion DAGs in parallel
    trigger_postgres = TriggerDagRunOperator(
        task_id='trigger_postgres_ingestion',
        trigger_dag_id='postgres_to_bronze',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_pharmacy = TriggerDagRunOperator(
        task_id='trigger_pharmacy_ingestion',
        trigger_dag_id='pharmacy_api_to_bronze',
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_lab = TriggerDagRunOperator(
        task_id='trigger_lab_ingestion',
        trigger_dag_id='lab_csv_to_bronze',
        wait_for_completion=True,
        poke_interval=30,
    )

    # Log Bronze completion
    log_bronze_complete_task = PythonOperator(
        task_id='log_bronze_complete',
        python_callable=log_bronze_complete,
    )

    # --- SILVER LAYER ---
    # Trigger Silver transformation
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transformation',
        trigger_dag_id='silver_orchestrator',
        wait_for_completion=True,
        poke_interval=30,
    )

    # Log Silver completion
    log_silver_complete_task = PythonOperator(
        task_id='log_silver_complete',
        python_callable=log_silver_complete,
    )

    # --- GOLD LAYER ---
    # Trigger Gold transformation
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_transformation',
        trigger_dag_id='gold_orchestrator',
        wait_for_completion=True,
        poke_interval=30,
    )

    # Log Gold completion
    log_gold_complete_task = PythonOperator(
        task_id='log_gold_complete',
        python_callable=log_gold_complete,
    )

    # Log pipeline completion
    log_pipeline_complete_task = PythonOperator(
        task_id='log_pipeline_complete',
        python_callable=log_pipeline_complete,
    )

    # End task
    end = DummyOperator(task_id='end')

    # Define complete workflow: Bronze → Silver → Gold
    start >> log_start_task >> [trigger_postgres, trigger_pharmacy, trigger_lab] >> log_bronze_complete_task >> trigger_silver >> log_silver_complete_task >> trigger_gold >> log_gold_complete_task >> log_pipeline_complete_task >> end