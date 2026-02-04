"""
Pharmacy API to S3 Bronze Layer Ingestion
Fetches data from Pharmacy API and uploads directly to S3
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import json
import io
import sys

# Add config to Python path
sys.path.append('/opt/airflow')

try:
    from config.aws_config import aws_config
except ImportError:
    print("Warning: Could not import aws_config")
    aws_config = None

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}


def extract_pharmacy_to_s3():
    """
    Fetch data from Pharmacy API and upload to S3 Bronze layer
    Uses environment variables for AWS credentials
    """

    print("Starting Pharmacy API to S3 Bronze ingestion")

    # Check AWS configuration
    if not aws_config:
        raise ValueError("AWS configuration not found. Check config/aws_config.py")

    # Get S3 client
    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error getting S3 client: {e}")
        print("Make sure AWS credentials are set in environment variables")
        raise

    bucket = aws_config.bucket_name

    # API endpoint
    api_url = "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json"
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        # Fetch data from API
        print(f"Fetching data from: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Handle nested structure - extract 'drugs' array
        if isinstance(data, dict) and 'drugs' in data:
            drugs_list = data['drugs']
            print(f"Received {len(drugs_list)} pharmacy records from 'drugs' array")
        else:
            # If it's already a list, use it directly
            drugs_list = data if isinstance(data, list) else [data]
            print(f"Received {len(drugs_list)} pharmacy records")

        # Convert to DataFrame for processing
        df = pd.DataFrame(drugs_list)
        print(f"Data shape: {df.shape[0]} rows x {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")

        if len(df) > 0:
            # Upload raw JSON to S3 (preserve original format)
            json_key = f"bronze/pharmacy/{today}/pharmacy_inventory.json"

            # Convert JSON to bytes for S3 upload (save the entire response including 'drugs' wrapper)
            json_bytes = json.dumps(data, indent=2).encode('utf-8')
            json_buffer = io.BytesIO(json_bytes)

            s3.put_object(
                Bucket=bucket,
                Key=json_key,
                Body=json_buffer,
                ContentType='application/json'
            )
            print(f"Uploaded JSON to S3: s3://{bucket}/{json_key}")

            # Upload as Parquet to S3 (optimized for processing - flattened drugs array)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            parquet_key = f"bronze/pharmacy/{today}/pharmacy_inventory.parquet"
            s3.put_object(
                Bucket=bucket,
                Key=parquet_key,
                Body=buffer,
                ContentType='application/parquet'
            )
            print(f"Uploaded Parquet to S3: s3://{bucket}/{parquet_key}")

            # Upload as CSV to S3 (human readable - flattened drugs array)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            csv_key = f"bronze/pharmacy/{today}/pharmacy_inventory.csv"
            s3.put_object(
                Bucket=bucket,
                Key=csv_key,
                Body=csv_buffer.getvalue().encode(),
                ContentType='text/csv'
            )
            print(f"Uploaded CSV to S3: s3://{bucket}/{csv_key}")

            # Show sample data for verification
            print(f"\nSample data (first record):")
            if len(drugs_list) > 0:
                sample = drugs_list[0]
                print(json.dumps(sample, indent=2))

            # Show some stats
            print(f"\nData summary:")
            print(f"  - Total drugs: {len(df)}")
            print(f"  - Categories: {df['category'].unique().tolist() if 'category' in df.columns else 'N/A'}")
            print(f"  - Total stock: {df['current_stock'].sum() if 'current_stock' in df.columns else 'N/A'}")

            print("\nPharmacy API ingestion complete")
            return True
        else:
            print("Warning: No data received from API")
            return False

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        raise
    except Exception as e:
        print(f"Error during pharmacy ingestion: {e}")
        raise


with DAG(
        'pharmacy_api_to_bronze',
        default_args=default_args,
        description='Extract pharmacy inventory data from API to S3 Bronze layer',
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'bronze', 'api', 'pharmacy', 's3']
) as dag:
    extract_task = PythonOperator(
        task_id='extract_pharmacy_to_s3',
        python_callable=extract_pharmacy_to_s3,
    )

    extract_task