from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import boto3
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def extract_pharmacy_data():
    """Extract data from Pharmacy API"""

    # API endpoint
    api_url = "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json"

    # Initialize S3 client
    s3_client = boto3.client('s3')
    bucket_name = 'medi-track-360-data-lake'

    # Create today's date for partitioning
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        print(f"Fetching data from Pharmacy API...")

        # Make API request
        response = requests.get(api_url)
        response.raise_for_status()  # Raise error for bad status codes

        # Parse JSON data
        pharmacy_data = response.json()

        # Save to local JSON file first
        local_path = f"/tmp/pharmacy_inventory_{today}.json"
        with open(local_path, 'w') as f:
            json.dump(pharmacy_data, f)

        # Upload to S3 Bronze layer
        s3_key = f"bronze/pharmacy/{today}/pharmacy_inventory.json"
        s3_client.upload_file(local_path, bucket_name, s3_key)

        print(f"Successfully uploaded pharmacy data to S3: {s3_key}")

        # Also save as CSV for easier processing later
        if isinstance(pharmacy_data, list) and len(pharmacy_data) > 0:
            import pandas as pd
            df = pd.DataFrame(pharmacy_data)
            csv_path = f"/tmp/pharmacy_inventory_{today}.csv"
            df.to_csv(csv_path, index=False)

            s3_csv_key = f"bronze/pharmacy/{today}/pharmacy_inventory.csv"
            s3_client.upload_file(csv_path, bucket_name, s3_csv_key)

            os.remove(csv_path)

        # Clean up local file
        os.remove(local_path)

    except Exception as e:
        print(f"Error extracting pharmacy data: {str(e)}")
        raise


with DAG(
        'pharmacy_api_to_bronze',
        default_args=default_args,
        description='Extract data from Pharmacy API to S3 Bronze',
        schedule_interval='@daily',
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_pharmacy_data',
        python_callable=extract_pharmacy_data,
    )

    extract_task