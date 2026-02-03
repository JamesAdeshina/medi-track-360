from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from pathlib import Path
import shutil

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_lab_to_local():
    """Process lab CSV files and save to local Bronze"""

    print("Starting Lab CSV processing to LOCAL...")

    # Source and destination directories
    source_dir = "data/lab_results"
    dest_base_dir = "data/bronze/lab_results"

    try:
        # Check if source directory exists
        if not os.path.exists(source_dir):
            print(f"✗ Source directory not found: {source_dir}")
            print("Creating sample lab data...")
            create_sample_lab_data(source_dir)

        # Get all CSV files
        csv_files = list(Path(source_dir).glob("*.csv"))
        print(f"Found {len(csv_files)} CSV files")

        if not csv_files:
            print("No CSV files found. Creating sample data...")
            create_sample_lab_data(source_dir)
            csv_files = list(Path(source_dir).glob("*.csv"))

        # Process each CSV file
        for csv_file in csv_files:
            try:
                file_name = csv_file.name
                print(f"Processing: {file_name}")

                # Extract date from filename
                date_str = file_name.replace("lab_results_", "").replace(".csv", "")

                # Read CSV
                df = pd.read_csv(csv_file)
                print(f"  Records: {len(df)}")

                # Create destination directory for this date
                dest_dir = f"{dest_base_dir}/{date_str}"
                os.makedirs(dest_dir, exist_ok=True)

                # Save copy of original CSV to Bronze
                bronze_csv_path = f"{dest_dir}/lab_results.csv"
                shutil.copy2(csv_file, bronze_csv_path)
                print(f"  Copied to Bronze: {bronze_csv_path}")

                # Also save as Parquet
                parquet_path = f"{dest_dir}/lab_results.parquet"
                df.to_parquet(parquet_path)
                print(f"  Saved as Parquet: {parquet_path}")

                # Show sample
                print(f"  Columns: {list(df.columns)}")
                if len(df) > 0:
                    print(f"  Sample row: {df.iloc[0].to_dict()}")

            except Exception as file_error:
                print(f"  Error processing {csv_file}: {file_error}")
                continue

        print("✓ Lab CSV processing to LOCAL complete!")

    except Exception as e:
        print(f"✗ Lab CSV processing failed: {e}")
        raise


def create_sample_lab_data(directory):
    """Create sample lab data if none exists"""

    os.makedirs(directory, exist_ok=True)

    # Create sample data for 3 days
    sample_data = {
        'test_id': range(1, 101),
        'patient_id': [i % 50 + 1 for i in range(100)],
        'test_name': ['Blood Test', 'Urine Test', 'X-Ray', 'MRI', 'CT Scan'] * 20,
        'result': ['Normal', 'Abnormal', 'Pending', 'Critical'] * 25,
        'sample_time': pd.date_range(start='2025-11-01', periods=100, freq='H').strftime('%Y-%m-%d %H:%M:%S'),
        'completed_time': pd.date_range(start='2025-11-01 01:00:00', periods=100, freq='H').strftime(
            '%Y-%m-%d %H:%M:%S'),
        'lab_technician': ['Tech_' + str(i % 5 + 1) for i in range(100)]
    }

    df = pd.DataFrame(sample_data)

    # Save for 3 days
    for day in range(1, 4):
        date_str = f"2025-11-{day:02d}"
        # Filter for this day
        day_df = df[df['sample_time'].str.startswith(date_str)]

        if len(day_df) > 0:
            file_path = f"{directory}/lab_results_{date_str}.csv"
            day_df.to_csv(file_path, index=False)
            print(f"Created sample file: {file_path} ({len(day_df)} records)")


with DAG(
        'lab_csv_to_bronze_local',
        default_args=default_args,
        description='Process lab CSV files to LOCAL Bronze',
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'bronze', 'csv', 'local']
) as dag:
    extract_task = PythonOperator(
        task_id='extract_lab_to_local',
        python_callable=extract_lab_to_local,
    )

    extract_task