"""
Lab CSV to S3 Bronze Layer Ingestion
Reads lab CSV files and uploads directly to S3
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import io
from pathlib import Path
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_lab_to_s3():
    """
    Read lab CSV files and upload to S3 Bronze layer
    Uses environment variables for AWS credentials
    """

    print("Starting Lab CSV to S3 Bronze ingestion")

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

    # Lab CSV files location (mounted in Docker)
    lab_dir = Path("/opt/airflow/data/lab_results")

    try:
        # Check if directory exists
        if not lab_dir.exists():
            print(f"Error: Lab directory not found: {lab_dir}")
            print("Make sure data/lab_results is mounted in Docker")
            return False

        # Get all CSV files
        csv_files = list(lab_dir.glob("*.csv"))
        print(f"Found {len(csv_files)} lab CSV files")

        if not csv_files:
            print("Warning: No CSV files found in lab directory")
            return False

        uploaded_count = 0
        # Process each CSV file
        for csv_file in csv_files:
            try:
                file_name = csv_file.name
                print(f"Processing file: {file_name}")

                # Extract date from filename (format: lab_results_YYYY-MM-DD.csv)
                date_str = file_name.replace("lab_results_", "").replace(".csv", "")

                # Read CSV file
                df = pd.read_csv(csv_file)
                print(f"  Read {len(df)} rows")

                # Upload as Parquet to S3 (optimized format)
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                parquet_key = f"bronze/lab_results/{date_str}/lab_results.parquet"
                s3.put_object(
                    Bucket=bucket,
                    Key=parquet_key,
                    Body=buffer,
                    ContentType='application/parquet'
                )
                print(f"  Uploaded Parquet to S3: s3://{bucket}/{parquet_key}")

                # Also upload original CSV to S3 (preserve source)
                with open(csv_file, 'rb') as f:
                    csv_key = f"bronze/lab_results/{date_str}/lab_results.csv"
                    s3.put_object(
                        Bucket=bucket,
                        Key=csv_key,
                        Body=f,
                        ContentType='text/csv'
                    )
                    print(f"  Uploaded CSV to S3: s3://{bucket}/{csv_key}")

                uploaded_count += 1

                # Show sample data for verification
                if len(df) > 0:
                    print(f"  Columns: {list(df.columns)}")
                    sample_row = df.iloc[0].to_dict()
                    print(f"  First row sample: {sample_row}")

            except Exception as file_error:
                print(f"  Error processing {file_name}: {file_error}")
                continue

        print(f"Lab CSV ingestion complete: {uploaded_count} files uploaded to S3")
        return uploaded_count > 0

    except Exception as e:
        print(f"Error during lab CSV ingestion: {e}")
        raise


with DAG(
        'lab_csv_to_bronze',
        default_args=default_args,
        description='Extract lab results from CSV files to S3 Bronze layer',
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'bronze', 'csv', 'lab', 's3']
) as dag:
    extract_task = PythonOperator(
        task_id='extract_lab_to_s3',
        python_callable=extract_lab_to_s3,
    )

    extract_task