from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
import pandas as pd
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_postgres_data():
    """Extract data from Supabase PostgreSQL"""

    # Supabase connection details
    conn_params = {
        'host': 'aws-1-eu-central-1.pooler.supabase.com',
        'port': 5432,
        'database': 'postgres',
        'user': 'medi_reader.hceprxhtdgtbqmrfwymn',
        'password': 'medi_reader123'
    }

    # Connect to Supabase
    pg_hook = PostgresHook(postgres_conn_id=None, **conn_params)

    # Tables to extract
    tables = ['patients', 'admissions', 'wards', 'bed_assignments', 'triage_levels']

    # Initialize S3 client
    s3_client = boto3.client('s3')
    bucket_name = 'medi-track-360-data-lake'

    # Create today's date for partitioning
    today = datetime.now().strftime('%Y-%m-%d')

    for table in tables:
        try:
            print(f"Extracting data from {table}...")

            # Query all data from table
            sql = f"SELECT * FROM {table};"
            df = pg_hook.get_pandas_df(sql)

            # Save to local CSV first
            local_path = f"/tmp/{table}_{today}.csv"
            df.to_csv(local_path, index=False)

            # Upload to S3 Bronze layer
            s3_key = f"bronze/postgres/{today}/{table}.csv"
            s3_client.upload_file(local_path, bucket_name, s3_key)

            print(f"Successfully uploaded {table} to S3: {s3_key}")

            # Clean up local file
            os.remove(local_path)

        except Exception as e:
            print(f"Error extracting {table}: {str(e)}")
            continue


with DAG(
        'postgres_to_bronze',
        default_args=default_args,
        description='Extract data from PostgreSQL to S3 Bronze',
        schedule_interval='@daily',
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_postgres_data',
        python_callable=extract_postgres_data,
    )

    extract_task