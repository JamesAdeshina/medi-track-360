from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from pathlib import Path
import numpy as np

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def find_bronze_directory():
    """Find the bronze directory location"""
    possible_paths = [
        "data/bronze",
        "docker/data/bronze",
        "../data/bronze",
        "./bronze"
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None


def transform_patients():
    """Transform patients data from Bronze to Silver"""

    print("Transforming patients data...")

    bronze_path = find_bronze_directory()
    if not bronze_path:
        print("✗ Bronze directory not found!")
        return False

    patients_path = os.path.join(bronze_path, "patients")
    if not os.path.exists(patients_path):
        print(f"✗ Patients directory not found at {patients_path}")
        return False

    # Find latest patients file
    csv_files = list(Path(patients_path).glob("*.csv"))
    if not csv_files:
        print("✗ No patients CSV files found")
        return False

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Processing file: {latest_file}")

    try:
        # Read Bronze data
        df = pd.read_csv(latest_file)
        print(f"  Bronze records: {len(df)}")

        # Create a copy for Silver
        silver_df = df.copy()

        # 1. Standardize column names (to snake_case)
        silver_df.columns = [col.lower().replace(' ', '_') for col in silver_df.columns]
        print(f"  Columns standardized: {list(silver_df.columns)}")

        # 2. Handle date_of_birth
        if 'date_of_birth' in silver_df.columns or 'dob' in silver_df.columns:
            dob_col = 'date_of_birth' if 'date_of_birth' in silver_df.columns else 'dob'
            silver_df['date_of_birth'] = pd.to_datetime(silver_df[dob_col], errors='coerce')
            if dob_col != 'date_of_birth':
                silver_df = silver_df.drop(columns=[dob_col])

        # 3. Calculate age
        if 'date_of_birth' in silver_df.columns:
            today = pd.Timestamp.now()
            silver_df['age'] = ((today - silver_df['date_of_birth']).dt.days / 365.25).astype(int)
            silver_df['age'] = silver_df['age'].clip(lower=0, upper=120)  # Reasonable bounds

        # 4. Standardize gender
        if 'gender' in silver_df.columns:
            silver_df['gender'] = silver_df['gender'].str.upper().str.strip()
            valid_genders = ['M', 'F', 'OTHER']
            silver_df['gender'] = silver_df['gender'].apply(
                lambda x: x if x in valid_genders else 'OTHER'
            )

        # 5. Clean name fields
        for col in ['first_name', 'last_name', 'full_name']:
            if col in silver_df.columns:
                silver_df[col] = silver_df[col].str.strip().str.title()

        # 6. Add metadata columns
        silver_df['silver_loaded_at'] = datetime.now()
        silver_df['data_source'] = 'postgres'

        # 7. Remove duplicates
        orig_count = len(silver_df)
        silver_df = silver_df.drop_duplicates(subset=['patient_id'] if 'patient_id' in silver_df.columns else [])
        dup_count = orig_count - len(silver_df)
        if dup_count > 0:
            print(f"  Removed {dup_count} duplicate patient records")

        # 8. Data quality checks
        issues = []
        if 'patient_id' in silver_df.columns:
            null_ids = silver_df['patient_id'].isnull().sum()
            if null_ids > 0:
                issues.append(f"{null_ids} null patient_ids")

        if issues:
            print(f"  ⚠️ Data quality issues: {', '.join(issues)}")

        # Save to Silver layer
        silver_dir = "data/silver/patients"
        os.makedirs(silver_dir, exist_ok=True)

        silver_file = os.path.join(silver_dir, f"patients_{datetime.now().strftime('%Y-%m-%d')}.parquet")
        silver_df.to_parquet(silver_file)

        print(f"✓ Silver patients created: {silver_file}")
        print(f"  Silver records: {len(silver_df)}")
        print(f"  Sample columns: {list(silver_df.columns)[:10]}")

        return True

    except Exception as e:
        print(f"✗ Error transforming patients: {e}")
        return False


def transform_admissions():
    """Transform admissions data from Bronze to Silver"""

    print("Transforming admissions data...")

    bronze_path = find_bronze_directory()
    if not bronze_path:
        print("✗ Bronze directory not found!")
        return False

    admissions_path = os.path.join(bronze_path, "admissions")
    if not os.path.exists(admissions_path):
        print(f"✗ Admissions directory not found at {admissions_path}")
        return False

    # Find latest admissions file
    csv_files = list(Path(admissions_path).glob("*.csv"))
    if not csv_files:
        print("✗ No admissions CSV files found")
        return False

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Processing file: {latest_file}")

    try:
        # Read Bronze data
        df = pd.read_csv(latest_file)
        print(f"  Bronze records: {len(df)}")

        # Create Silver DataFrame
        silver_df = df.copy()

        # 1. Standardize column names
        silver_df.columns = [col.lower().replace(' ', '_') for col in silver_df.columns]

        # 2. Convert timestamps
        time_cols = ['admission_time', 'discharge_time', 'admission_date', 'discharge_date']
        for col in time_cols:
            if col in silver_df.columns:
                silver_df[col] = pd.to_datetime(silver_df[col], errors='coerce')

        # 3. Calculate length of stay
        if 'admission_time' in silver_df.columns and 'discharge_time' in silver_df.columns:
            silver_df['length_of_stay_hours'] = (
                                                        silver_df['discharge_time'] - silver_df['admission_time']
                                                ).dt.total_seconds() / 3600

            # Handle ongoing admissions (null discharge)
            silver_df['is_active'] = silver_df['discharge_time'].isnull()
            silver_df['length_of_stay_hours'] = silver_df['length_of_stay_hours'].fillna(0)

        # 4. Standardize triage levels
        if 'triage_level' in silver_df.columns:
            # Convert to standardized categories
            triage_mapping = {
                '1': 'RED', '2': 'ORANGE', '3': 'YELLOW', '4': 'GREEN', '5': 'BLUE',
                'red': 'RED', 'orange': 'ORANGE', 'yellow': 'YELLOW', 'green': 'GREEN', 'blue': 'BLUE',
                'critical': 'RED', 'urgent': 'ORANGE', 'semi-urgent': 'YELLOW', 'non-urgent': 'GREEN'
            }
            silver_df['triage_level'] = silver_df['triage_level'].astype(str).str.upper()
            silver_df['triage_level'] = silver_df['triage_level'].map(triage_mapping).fillna('UNKNOWN')

        # 5. Add metadata
        silver_df['silver_loaded_at'] = datetime.now()
        silver_df['data_source'] = 'postgres'

        # 6. Save to Silver
        silver_dir = "data/silver/admissions"
        os.makedirs(silver_dir, exist_ok=True)

        silver_file = os.path.join(silver_dir, f"admissions_{datetime.now().strftime('%Y-%m-%d')}.parquet")
        silver_df.to_parquet(silver_file)

        print(f"✓ Silver admissions created: {silver_file}")
        print(f"  Silver records: {len(silver_df)}")

        # Show statistics
        if 'is_active' in silver_df.columns:
            active = silver_df['is_active'].sum()
            print(f"  Active admissions: {active}")

        if 'length_of_stay_hours' in silver_df.columns:
            avg_stay = silver_df['length_of_stay_hours'].mean()
            print(f"  Average stay: {avg_stay:.1f} hours")

        return True

    except Exception as e:
        print(f"✗ Error transforming admissions: {e}")
        return False


def transform_pharmacy():
    """Transform pharmacy data from Bronze to Silver"""

    print("Transforming pharmacy data...")

    bronze_path = find_bronze_directory()
    if not bronze_path:
        print("✗ Bronze directory not found!")
        return False

    pharmacy_path = os.path.join(bronze_path, "pharmacy")
    if not os.path.exists(pharmacy_path):
        print(f"✗ Pharmacy directory not found at {pharmacy_path}")
        return False

    # Find latest pharmacy file
    csv_files = list(Path(pharmacy_path).glob("*.csv"))
    if not csv_files:
        print("✗ No pharmacy CSV files found")
        return False

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Processing file: {latest_file}")

    try:
        # Read Bronze data
        df = pd.read_csv(latest_file)
        print(f"  Bronze records: {len(df)}")

        # Create Silver DataFrame
        silver_df = df.copy()

        # 1. Standardize column names
        silver_df.columns = [col.lower().replace(' ', '_') for col in silver_df.columns]

        # 2. Convert dates
        if 'expiry_date' in silver_df.columns:
            silver_df['expiry_date'] = pd.to_datetime(silver_df['expiry_date'], errors='coerce')

        if 'last_updated' in silver_df.columns:
            silver_df['last_updated'] = pd.to_datetime(silver_df['last_updated'], errors='coerce')

        # 3. Calculate derived fields
        if 'current_stock' in silver_df.columns and 'threshold' in silver_df.columns:
            silver_df['stockout_risk'] = silver_df['current_stock'] < silver_df['threshold']
            silver_df['stock_percentage'] = (silver_df['current_stock'] / silver_df['threshold'] * 100).round(1)

        if 'expiry_date' in silver_df.columns:
            today = pd.Timestamp.now()
            silver_df['days_until_expiry'] = (silver_df['expiry_date'] - today).dt.days
            silver_df['expiry_risk'] = silver_df['days_until_expiry'] < 30  # Less than 30 days

        # 4. Clean drug names
        if 'drug_name' in silver_df.columns:
            silver_df['drug_name'] = silver_df['drug_name'].str.strip().str.upper()

        # 5. Categorize drugs
        if 'category' in silver_df.columns:
            silver_df['category'] = silver_df['category'].str.strip().str.title()

        # 6. Add metadata
        silver_df['silver_loaded_at'] = datetime.now()
        silver_df['data_source'] = 'pharmacy_api'

        # 7. Save to Silver
        silver_dir = "data/silver/pharmacy"
        os.makedirs(silver_dir, exist_ok=True)

        silver_file = os.path.join(silver_dir, f"pharmacy_{datetime.now().strftime('%Y-%m-%d')}.parquet")
        silver_df.to_parquet(silver_file)

        print(f"✓ Silver pharmacy created: {silver_file}")
        print(f"  Silver records: {len(silver_df)}")

        # Show statistics
        if 'stockout_risk' in silver_df.columns:
            at_risk = silver_df['stockout_risk'].sum()
            print(f"  Drugs at stockout risk: {at_risk}/{len(silver_df)}")

        if 'expiry_risk' in silver_df.columns:
            expiring = silver_df['expiry_risk'].sum()
            print(f"  Drugs expiring soon (<30 days): {expiring}")

        return True

    except Exception as e:
        print(f"✗ Error transforming pharmacy: {e}")
        return False


def transform_lab_results():
    """Transform lab results data from Bronze to Silver"""

    print("Transforming lab results data...")

    bronze_path = find_bronze_directory()
    if not bronze_path:
        print("✗ Bronze directory not found!")
        return False

    lab_path = os.path.join(bronze_path, "lab_results")
    if not os.path.exists(lab_path):
        print(f"✗ Lab results directory not found at {lab_path}")
        return False

    # Find all lab CSV files
    csv_files = list(Path(lab_path).rglob("*.csv"))
    if not csv_files:
        print("✗ No lab CSV files found")
        return False

    print(f"Found {len(csv_files)} lab files")

    try:
        # Read and combine all lab files
        all_dfs = []
        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            df['source_file'] = os.path.basename(csv_file)
            all_dfs.append(df)

        # Combine all data
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"  Total Bronze records: {len(combined_df)}")

        # Create Silver DataFrame
        silver_df = combined_df.copy()

        # 1. Standardize column names
        silver_df.columns = [col.lower().replace(' ', '_') for col in silver_df.columns]

        # 2. Convert timestamps
        if 'sample_time' in silver_df.columns:
            silver_df['sample_time'] = pd.to_datetime(silver_df['sample_time'], errors='coerce')

        if 'completed_time' in silver_df.columns:
            silver_df['completed_time'] = pd.to_datetime(silver_df['completed_time'], errors='coerce')

        # 3. Calculate turnaround time
        if 'sample_time' in silver_df.columns and 'completed_time' in silver_df.columns:
            silver_df['turnaround_hours'] = (
                                                    silver_df['completed_time'] - silver_df['sample_time']
                                            ).dt.total_seconds() / 3600

            # Handle missing completed times
            silver_df['is_pending'] = silver_df['completed_time'].isnull()
            silver_df['turnaround_hours'] = silver_df['turnaround_hours'].fillna(-1)  # -1 for pending

        # 4. Categorize results
        if 'result' in silver_df.columns:
            # Standardize result values
            result_mapping = {
                'normal': 'NORMAL',
                'abnormal': 'ABNORMAL',
                'critical': 'CRITICAL',
                'pending': 'PENDING',
                'positive': 'POSITIVE',
                'negative': 'NEGATIVE'
            }

            silver_df['result'] = silver_df['result'].astype(str).str.lower().str.strip()
            silver_df['result_category'] = silver_df['result'].map(result_mapping)
            silver_df['result_category'] = silver_df['result_category'].fillna('OTHER')

        # 5. Clean test names
        if 'test_name' in silver_df.columns:
            silver_df['test_name'] = silver_df['test_name'].str.strip().str.title()

        # 6. Add metadata
        silver_df['silver_loaded_at'] = datetime.now()
        silver_df['data_source'] = 'lab_csv'

        # 7. Save to Silver
        silver_dir = "data/silver/labs"
        os.makedirs(silver_dir, exist_ok=True)

        silver_file = os.path.join(silver_dir, f"labs_{datetime.now().strftime('%Y-%m-%d')}.parquet")
        silver_df.to_parquet(silver_file)

        print(f"✓ Silver lab results created: {silver_file}")
        print(f"  Silver records: {len(silver_df)}")

        # Show statistics
        if 'turnaround_hours' in silver_df.columns:
            avg_turnaround = silver_df[silver_df['turnaround_hours'] >= 0]['turnaround_hours'].mean()
            print(f"  Average turnaround: {avg_turnaround:.1f} hours")

        if 'is_pending' in silver_df.columns:
            pending = silver_df['is_pending'].sum()
            print(f"  Pending tests: {pending}")

        if 'result_category' in silver_df.columns:
            result_counts = silver_df['result_category'].value_counts()
            print(f"  Result categories: {result_counts.to_dict()}")

        return True

    except Exception as e:
        print(f"✗ Error transforming lab results: {e}")
        return False


def transform_wards():
    """Transform wards data from Bronze to Silver"""

    print("Transforming wards data...")

    bronze_path = find_bronze_directory()
    if not bronze_path:
        print("✗ Bronze directory not found!")
        return False

    wards_path = os.path.join(bronze_path, "wards")
    if not os.path.exists(wards_path):
        print(f"✗ Wards directory not found at {wards_path}")
        return False

    # Find latest wards file
    csv_files = list(Path(wards_path).glob("*.csv"))
    if not csv_files:
        print("✗ No wards CSV files found")
        return False

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Processing file: {latest_file}")

    try:
        # Read Bronze data
        df = pd.read_csv(latest_file)
        print(f"  Bronze records: {len(df)}")

        # Create Silver DataFrame
        silver_df = df.copy()

        # 1. Standardize column names
        silver_df.columns = [col.lower().replace(' ', '_') for col in silver_df.columns]

        # 2. Clean ward names
        if 'ward_name' in silver_df.columns:
            silver_df['ward_name'] = silver_df['ward_name'].str.strip().str.title()

        # 3. Validate capacity
        if 'capacity' in silver_df.columns:
            silver_df['capacity'] = pd.to_numeric(silver_df['capacity'], errors='coerce')
            silver_df['capacity'] = silver_df['capacity'].fillna(0).astype(int)
            silver_df['capacity'] = silver_df['capacity'].clip(lower=0, upper=1000)  # Reasonable bounds

        # 4. Add ward type if missing
        if 'ward_type' not in silver_df.columns and 'ward_name' in silver_df.columns:
            # Infer ward type from name
            def infer_ward_type(name):
                name_lower = str(name).lower()
                if any(word in name_lower for word in ['icu', 'intensive', 'critical']):
                    return 'ICU'
                elif any(word in name_lower for word in ['surgical', 'surgery']):
                    return 'SURGICAL'
                elif any(word in name_lower for word in ['medical', 'medicine']):
                    return 'MEDICAL'
                elif any(word in name_lower for word in ['pediatric', 'children', 'child']):
                    return 'PEDIATRIC'
                elif any(word in name_lower for word in ['maternity', 'obgyn', 'labor']):
                    return 'MATERNITY'
                elif any(word in name_lower for word in ['emergency', 'er', 'a&e']):
                    return 'EMERGENCY'
                else:
                    return 'GENERAL'

            silver_df['ward_type'] = silver_df['ward_name'].apply(infer_ward_type)

        # 5. Add metadata
        silver_df['silver_loaded_at'] = datetime.now()
        silver_df['data_source'] = 'postgres'

        # 6. Save to Silver
        silver_dir = "data/silver/wards"
        os.makedirs(silver_dir, exist_ok=True)

        silver_file = os.path.join(silver_dir, f"wards_{datetime.now().strftime('%Y-%m-%d')}.parquet")
        silver_df.to_parquet(silver_file)

        print(f"✓ Silver wards created: {silver_file}")
        print(f"  Silver records: {len(silver_df)}")

        # Show statistics
        if 'capacity' in silver_df.columns:
            total_capacity = silver_df['capacity'].sum()
            print(f"  Total hospital capacity: {total_capacity} beds")

        if 'ward_type' in silver_df.columns:
            ward_types = silver_df['ward_type'].value_counts()
            print(f"  Ward types: {ward_types.to_dict()}")

        return True

    except Exception as e:
        print(f"✗ Error transforming wards: {e}")
        return False


with DAG(
        'bronze_to_silver',
        default_args=default_args,
        description='Transform Bronze layer data to Silver layer',
        schedule_interval='@daily',
        catchup=False,
        tags=['transformation', 'silver']
) as dag:
    # Create tasks for each transformation
    patients_task = PythonOperator(
        task_id='transform_patients',
        python_callable=transform_patients,
    )

    admissions_task = PythonOperator(
        task_id='transform_admissions',
        python_callable=transform_admissions,
    )

    pharmacy_task = PythonOperator(
        task_id='transform_pharmacy',
        python_callable=transform_pharmacy,
    )

    labs_task = PythonOperator(
        task_id='transform_lab_results',
        python_callable=transform_lab_results,
    )

    wards_task = PythonOperator(
        task_id='transform_wards',
        python_callable=transform_wards,
    )

    # Define dependencies (run in parallel)
    [patients_task, admissions_task, pharmacy_task, labs_task, wards_task]