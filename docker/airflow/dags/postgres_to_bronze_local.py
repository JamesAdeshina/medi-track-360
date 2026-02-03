from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import pandas as pd
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


def extract_postgres_to_local():
    """Extract data from PostgreSQL and save locally (Bronze layer)"""

    print("Starting PostgreSQL extraction to LOCAL Bronze...")

    # PostgreSQL connection
    pg_params = {
        'host': 'aws-1-eu-central-1.pooler.supabase.com',
        'port': 5432,
        'database': 'postgres',
        'user': 'medi_reader.hceprxhtdgtbqmrfwymn',
        'password': 'medi_reader123'
    }

    # Local directory
    base_dir = "data/bronze"

    # Get today's date for partitioning
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        # Connect to PostgreSQL
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**pg_params)

        # List of tables to extract
        tables_to_extract = ['patients', 'admissions', 'wards', 'bed_assignments']

        for table in tables_to_extract:
            try:
                print(f"Extracting table: {table}")

                # Read data into pandas
                query = f"SELECT * FROM {table};"
                df = pd.read_sql_query(query, conn)

                print(f"  Retrieved {len(df)} rows")

                if len(df) > 0:
                    # Save locally as CSV (Bronze layer)
                    table_dir = f"{base_dir}/{table}"
                    os.makedirs(table_dir, exist_ok=True)

                    csv_path = f"{table_dir}/{table}_{today}.csv"
                    df.to_csv(csv_path, index=False)

                    print(f"  Saved to: {csv_path}")

                    # Also save as Parquet
                    parquet_path = f"{table_dir}/{table}_{today}.parquet"
                    df.to_parquet(parquet_path)
                    print(f"  Also saved as: {parquet_path}")

                    # Show sample of data
                    print(f"  Sample columns: {list(df.columns)[:5]}...")
                    print(f"  First row sample: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")
                else:
                    print(f"  Warning: Table {table} is empty")

            except Exception as table_error:
                print(f"  Error extracting {table}: {table_error}")
                continue

        conn.close()
        print("✓ PostgreSQL extraction to LOCAL Bronze complete!")

    except Exception as e:
        print(f"✗ PostgreSQL extraction failed: {e}")
        raise


with DAG(
        'postgres_to_bronze_local',
        default_args=default_args,
        description='Extract PostgreSQL data to LOCAL Bronze layer',
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'bronze', 'postgresql', 'local']
) as dag:
    extract_task = PythonOperator(
        task_id='extract_postgres_to_local',
        python_callable=extract_postgres_to_local,
    )

    extract_task