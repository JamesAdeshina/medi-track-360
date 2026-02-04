"""
PostgreSQL to S3 Bronze Layer Ingestion
Extracts data from PostgreSQL and uploads directly to S3
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd
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
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def extract_postgres_to_s3():
    """
    Extract data from PostgreSQL database and upload to S3 Bronze layer
    Uses environment variables for AWS credentials
    """

    print("Starting PostgreSQL to S3 Bronze ingestion")

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

    # PostgreSQL connection parameters
    pg_params = {
        'host': 'aws-1-eu-central-1.pooler.supabase.com',
        'port': 5432,
        'database': 'postgres',
        'user': 'medi_reader.hceprxhtdgtbqmrfwymn',
        'password': 'medi_reader123'
    }

    today = datetime.now().strftime('%Y-%m-%d')
    tables = ['patients', 'admissions', 'wards', 'bed_assignments']

    try:
        # Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**pg_params)

        extracted_count = 0
        for table in tables:
            try:
                print(f"Processing table: {table}")

                # Read data from PostgreSQL
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                print(f"  Retrieved {len(df)} rows")

                if len(df) > 0:
                    # Upload as Parquet to S3
                    buffer = io.BytesIO()
                    df.to_parquet(buffer, index=False)
                    buffer.seek(0)

                    s3_key = f"bronze/postgres/{today}/{table}.parquet"
                    s3.put_object(
                        Bucket=bucket,
                        Key=s3_key,
                        Body=buffer,
                        ContentType='application/parquet'
                    )
                    print(f"  Uploaded to S3: s3://{bucket}/{s3_key}")

                    # Also upload as CSV
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_buffer.seek(0)

                    csv_key = f"bronze/postgres/{today}/{table}.csv"
                    s3.put_object(
                        Bucket=bucket,
                        Key=csv_key,
                        Body=csv_buffer.getvalue().encode(),
                        ContentType='text/csv'
                    )
                    print(f"  Also uploaded as CSV")

                    extracted_count += 1
                else:
                    print(f"  Warning: Table {table} is empty")

            except Exception as table_error:
                print(f"  Error processing {table}: {table_error}")
                continue

        conn.close()
        print(f"PostgreSQL extraction complete: {extracted_count} tables processed")

    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        raise


with DAG(
        'postgres_to_bronze',
        default_args=default_args,
        description='Extract data from PostgreSQL database to S3 Bronze layer',
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'bronze', 'postgresql', 's3']
) as dag:
    extract_task = PythonOperator(
        task_id='extract_postgres_to_s3',
        python_callable=extract_postgres_to_s3,
    )

    extract_task