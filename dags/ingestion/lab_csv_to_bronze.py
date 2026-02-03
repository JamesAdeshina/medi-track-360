from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os
from pathlib import Path

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_lab_data():
    """Extract lab CSV files to S3"""

    # Initialize S3 client
    s3_client = boto3.client('s3')
    bucket_name = 'medi-track-360-data-lake'

    # Path to lab CSV files
    lab_data_path = "data/lab_results"

    try:
        print(f"Processing lab CSV files from {lab_data_path}...")

        # Get list of CSV files
        csv_files = list(Path(lab_data_path).glob("*.csv"))

        if not csv_files:
            print(f"No CSV files found in {lab_data_path}")
            return

        # Process each CSV file
        for csv_file in csv_files:
            file_name = csv_file.name
            date_part = file_name.replace("lab_results_", "").replace(".csv", "")

            # Validate date format
            try:
                file_date = datetime.strptime(date_part, "%Y-%m-%d").strftime('%Y-%m-%d')
            except ValueError:
                print(f"Skipping file with invalid date format: {file_name}")
                continue

            # Upload to S3 Bronze layer
            s3_key = f"bronze/lab_results/{file_date}/lab_results.csv"
            s3_client.upload_file(str(csv_file), bucket_name, s3_key)

            print(f"Uploaded {file_name} to S3: {s3_key}")

        print(f"Processed {len(csv_files)} lab CSV files")

    except Exception as e:
        print(f"Error processing lab data: {str(e)}")
        raise


with DAG(
        'lab_csv_to_bronze',
        default_args=default_args,
        description='Extract lab CSV files to S3 Bronze',
        schedule_interval='@daily',
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_lab_data',
        python_callable=extract_lab_data,
    )

    extract_task