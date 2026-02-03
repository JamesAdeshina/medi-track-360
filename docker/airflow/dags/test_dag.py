from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    start >> end
