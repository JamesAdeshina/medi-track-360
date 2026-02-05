"""
Load Gold layer data from S3 to Redshift
"""

import boto3
import psycopg2
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import sys

# Add config to Python path
sys.path.append('/opt/airflow')

try:
    from config.aws_config import aws_config
except ImportError:
    print("Warning: Could not import aws_config")
    aws_config = None


class RedshiftDataLoader:
    """Load Gold layer data from S3 to Redshift"""

    def __init__(self, aws_conn_id='aws_default', redshift_conn_id='redshift_default'):
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # Get bucket name from aws_config
        if aws_config:
            self.bucket_name = aws_config.bucket_name
        else:
            self.bucket_name = "medi-track-360-data-lake"  # fallback

        # Get Redshift connection parameters
        self.redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn_params = self.redshift_hook.get_connection(self.redshift_conn_id)
        self.connection_params = {
            "host": conn_params.host,
            "port": conn_params.port or 5439,
            "database": conn_params.schema or "meditrackdb",
            "user": conn_params.login,
            "password": conn_params.password,
        }

        print(f"Redshift connection configured:")
        print(f"  Host: {self.connection_params['host']}")
        print(f"  Port: {self.connection_params['port']}")
        print(f"  Database: {self.connection_params['database']}")
        print(f"  User: {self.connection_params['user']}")

        # IAM Role for Redshift to access S3 (optional - may not exist)
        self.iam_role_arn = None  # Will be set if available

    def test_s3_access(self):
        """Check if files exist in the Gold layer"""
        try:
            print(f"Checking S3 bucket: {self.bucket_name}")
            keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix="gold/")
            if not keys:
                print(" No files found in Gold layer")
                return False
            print(f"✓ Found {len(keys)} files in Gold layer")
            for key in keys[:5]:  # Show first 5
                print(f"  - {key}")
            return True
        except Exception as e:
            print(f"✗ Error accessing S3: {e}")
            return False

    def test_redshift_connection(self):
        """Test Redshift connection"""
        try:
            conn = psycopg2.connect(**self.connection_params)
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"✓ Connected to Redshift: {version[:50]}...")
            conn.close()
            return True
        except Exception as e:
            print(f"✗ Error connecting to Redshift: {e}")
            return False

    def load_table_via_python(self, table_name, s3_key):
        """Load table by downloading from S3 and inserting via Python (no IAM role needed)"""
        print(f"\nLoading {table_name} from {s3_key}...")

        try:
            # Download parquet file from S3
            import pandas as pd
            import io

            obj = self.s3_hook.get_key(key=s3_key, bucket_name=self.bucket_name)
            parquet_bytes = obj.get()['Body'].read()
            df = pd.read_parquet(io.BytesIO(parquet_bytes))

            print(f"  Downloaded {len(df)} rows from S3")

            # Connect to Redshift and load data
            conn = psycopg2.connect(**self.connection_params)
            cur = conn.cursor()

            # Truncate table
            cur.execute(f"TRUNCATE TABLE {table_name};")
            print(f"  Truncated {table_name}")

            # Insert data in batches
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]

                # Build INSERT statement
                cols = ', '.join(batch.columns)
                placeholders = ', '.join(['%s'] * len(batch.columns))
                insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

                # Execute batch insert
                values = [tuple(row) for row in batch.values]
                cur.executemany(insert_sql, values)

                if (i + batch_size) % 5000 == 0:
                    print(f"  Inserted {i + batch_size} rows...")

            conn.commit()

            # Verify count
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows = cur.fetchone()[0]
            print(f"✓ Loaded {rows:,} rows into {table_name}")

            conn.close()
            return True

        except Exception as e:
            print(f"✗ Error loading {table_name}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def load_all_tables(self):
        """Load all tables from S3 Gold layer to Redshift"""

        # Map of table names to S3 keys
        tables = {
            "dim_date": "gold/dimensions/dim_date.parquet",
            "dim_patient": "gold/dimensions/dim_patient.parquet",
            "dim_ward": "gold/dimensions/dim_ward.parquet",
            "dim_drug": "gold/dimensions/dim_drug.parquet",
            "fact_admissions": "gold/facts/fact_admissions.parquet",
            "fact_lab_turnaround": "gold/facts/fact_lab_turnaround.parquet",
            "fact_pharmacy_stock": "gold/facts/fact_pharmacy_stock.parquet"
        }

        success_count = 0
        fail_count = 0

        for table, s3_key in tables.items():
            try:
                if self.load_table_via_python(table, s3_key):
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                print(f"✗ Failed to load {table}: {e}")
                fail_count += 1

        print("\n" + "="*60)
        print(f"Redshift Loading Summary:")
        print(f"  Success: {success_count}/{len(tables)} tables")
        print(f"  Failed: {fail_count}/{len(tables)} tables")
        print("="*60)

        if fail_count > 0:
            raise Exception(f"Failed to load {fail_count} tables")


def load_data_from_s3_to_redshift():
    """Main function to load data from S3 to Redshift"""

    print("="*60)
    print("Starting Redshift Data Loading")
    print("="*60)

    loader = RedshiftDataLoader()

    # Test connections
    if not loader.test_s3_access():
        raise Exception("S3 access test failed")

    if not loader.test_redshift_connection():
        raise Exception("Redshift connection test failed")

    # Load all tables
    loader.load_all_tables()

    print("\n✓ Redshift loading complete!")


# ---------------- DAG Definition ---------------- #
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'load_to_redshift',
    default_args=default_args,
    description='Load Gold layer data from S3 to Redshift',
    schedule_interval='@daily',
    catchup=False,
    tags=['redshift', 'loading', 'gold', 'data_warehouse']
)

load_data_task = PythonOperator(
    task_id='load_data_from_s3_to_redshift',
    python_callable=load_data_from_s3_to_redshift,
    dag=dag,
)

load_data_task