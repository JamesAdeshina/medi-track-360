import boto3
import psycopg2
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


class RedshiftDataLoader:
    """Load Gold layer data from S3 to Redshift"""

    def __init__(self, aws_conn_id='aws_default', redshift_conn_id='redshift_default'):
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        self.redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        conn_params = self.redshift_hook.get_conn_params()
        self.connection_params = {
            "host": conn_params['host'],
            "port": conn_params['port'],
            "database": conn_params['database'],
            "user": conn_params['user'],
            "password": conn_params['password'],
        }

        # IAM Role for Redshift to access S3
        self.iam_role_arn = "arn:aws:iam::819340487562:role/RedshiftS3AccessRole"

    def test_s3_access(self):
        """Check if files exist in the Gold layer"""
        keys = self.s3_hook.list_keys(bucket_name=self.s3_hook.bucket_name, prefix="gold/")
        if not keys:
            print(" No files found in Gold layer")
            return False
        print(f" Found {len(keys)} files in Gold layer")
        return True

    def load_table(self, table_name, s3_prefix):
        """Load a single table from S3 to Redshift"""
        print(f"\nLoading {table_name}...")
        copy_sql = f"""
            COPY {table_name}
            FROM 's3://{self.s3_hook.bucket_name}/{s3_prefix}'
            IAM_ROLE '{self.iam_role_arn}'
            FORMAT AS PARQUET;
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {table_name};")
            cur.execute(copy_sql)
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows = cur.fetchone()[0]
            conn.commit()
            print(f" Loaded {rows:,} rows into {table_name}")
        except Exception as e:
            print(f" Error loading {table_name}: {e}")
        finally:
            if conn:
                conn.close()

    def load_all_tables(self):
        """Load all tables from S3 Gold layer to Redshift"""
        tables = {
            "dim_date": "gold/dimensions/dim_date/",
            "dim_patient": "gold/dimensions/dim_patient/",
            "dim_ward": "gold/dimensions/dim_ward/",
            "dim_drug": "gold/dimensions/dim_drug/",
            "fact_admissions": "gold/facts/fact_admissions/",
            "fact_lab_turnaround": "gold/facts/fact_lab_turnaround/",
            "fact_pharmacy_stock": "gold/facts/fact_pharmacy_stock/"
        }
        for table, prefix in tables.items():
            self.load_table(table, prefix)


def load_data_from_s3_to_redshift():
    loader = RedshiftDataLoader()
    if loader.test_s3_access():
        loader.load_all_tables()


# ---------------- DAG Definition ---------------- #
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'load_to_redshift',
    default_args=default_args,
    description='Load Gold layer data from S3 to Redshift',
    schedule_interval='@daily',
)

load_data_task = PythonOperator(
    task_id='load_data_from_s3_to_redshift',
    python_callable=load_data_from_s3_to_redshift,
    dag=dag,
)

load_data_task