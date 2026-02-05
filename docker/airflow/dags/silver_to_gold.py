"""
Silver to Gold Transformation DAG
Transforms cleaned Silver data into analytics-ready Gold layer (star schema)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import io
import sys
from pathlib import Path

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


def get_todays_date():
    """Get today's date for partitioning"""
    return datetime.now().strftime('%Y-%m-%d')


def create_date_dimension():
    """
    Create date dimension table for time-based analytics
    Covers several years for historical analysis
    """

    print("Creating date dimension table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Create date range (2 years back, 1 year forward)
        start_date = pd.Timestamp.now() - pd.DateOffset(years=2)
        end_date = pd.Timestamp.now() + pd.DateOffset(years=1)

        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # Create date dimension DataFrame
        date_dim = pd.DataFrame({'full_date': date_range})

        # Extract date components
        date_dim['date_id'] = date_dim['full_date'].dt.strftime('%Y%m%d').astype(int)
        date_dim['year'] = date_dim['full_date'].dt.year
        date_dim['quarter'] = date_dim['full_date'].dt.quarter
        date_dim['month'] = date_dim['full_date'].dt.month
        date_dim['month_name'] = date_dim['full_date'].dt.strftime('%B')
        date_dim['day'] = date_dim['full_date'].dt.day
        date_dim['day_of_week'] = date_dim['full_date'].dt.day_name()
        date_dim['day_of_week_num'] = date_dim['full_date'].dt.dayofweek
        date_dim['is_weekend'] = date_dim['day_of_week_num'].isin([5, 6])
        date_dim['is_holiday'] = False  # Would need holiday calendar

        # Add financial periods
        date_dim['financial_year'] = date_dim.apply(
            lambda x: x['year'] if x['month'] >= 4 else x['year'] - 1, axis=1
        )
        date_dim['financial_quarter'] = date_dim.apply(
            lambda x: ((x['month'] - 1) // 3 + 2) % 4 + 1, axis=1
        )

        # Save to Gold layer
        gold_key = f"gold/dimensions/dim_date.parquet"

        buffer = io.BytesIO()
        date_dim.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Date dimension saved to: s3://{bucket}/{gold_key}")
        print(f"  Date range: {start_date.date()} to {end_date.date()}")
        print(f"  Total dates: {len(date_dim)}")

        return True

    except Exception as e:
        print(f"Error creating date dimension: {e}")
        raise


def create_patient_dimension():
    """
    Create patient dimension table from Silver patients data
    """

    print("Creating patient dimension table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Find latest Silver patients file
        silver_prefix = "silver/patients/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)

        patients_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                patients_key = obj['Key']
                break

        if not patients_key:
            print("No Silver patients file found")
            return False

        print(f"Found Silver patients file: {patients_key}")

        # Read patients data from S3
        obj = s3.get_object(Bucket=bucket, Key=patients_key)
        patients_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        print(f"Loaded {len(patients_df)} patient records")

        # Create patient dimension
        patient_dim = patients_df.copy()

        # Select and rename columns for dimension table
        column_mapping = {
            'patient_id': 'patient_id',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'date_of_birth': 'date_of_birth',
            'gender': 'gender',
            'age': 'age'
        }

        # Keep only columns that exist in the DataFrame
        keep_columns = [col for col in column_mapping.keys() if col in patient_dim.columns]
        patient_dim = patient_dim[keep_columns]

        # Rename columns
        patient_dim = patient_dim.rename(columns={k: v for k, v in column_mapping.items() if k in patient_dim.columns})

        # Add age groups for analysis
        if 'age' in patient_dim.columns:
            bins = [0, 18, 30, 45, 60, 75, 100]
            labels = ['0-18', '19-30', '31-45', '46-60', '61-75', '75+']
            patient_dim['age_group'] = pd.cut(patient_dim['age'], bins=bins, labels=labels, right=False)

        # Add metadata
        patient_dim['dim_loaded_at'] = datetime.now()
        patient_dim['is_active'] = True

        # Remove any duplicates
        if 'patient_id' in patient_dim.columns:
            patient_dim = patient_dim.drop_duplicates(subset=['patient_id'], keep='first')

        # Save to Gold layer
        gold_key = f"gold/dimensions/dim_patient.parquet"

        buffer = io.BytesIO()
        patient_dim.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Patient dimension saved to: s3://{bucket}/{gold_key}")
        print(f"  Patient count: {len(patient_dim)}")
        print(f"  Columns: {list(patient_dim.columns)}")

        return True

    except Exception as e:
        print(f"Error creating patient dimension: {e}")
        raise


def create_ward_dimension():
    """
    Create ward dimension table from Silver wards data
    """

    print("Creating ward dimension table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Find latest Silver wards file
        silver_prefix = "silver/wards/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)

        wards_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                wards_key = obj['Key']
                break

        if not wards_key:
            print("No Silver wards file found")
            return False

        print(f"Found Silver wards file: {wards_key}")

        # Read wards data from S3
        obj = s3.get_object(Bucket=bucket, Key=wards_key)
        wards_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        print(f"Loaded {len(wards_df)} ward records")

        # Create ward dimension
        ward_dim = wards_df.copy()

        # Select and rename columns
        column_mapping = {
            'ward_id': 'ward_id',
            'ward_name': 'ward_name',
            'ward_type': 'ward_type',
            'capacity': 'capacity'
        }

        # Keep only columns that exist
        keep_columns = [col for col in column_mapping.keys() if col in ward_dim.columns]
        ward_dim = ward_dim[keep_columns]

        # Rename columns
        ward_dim = ward_dim.rename(columns={k: v for k, v in column_mapping.items() if k in ward_dim.columns})

        # Add derived fields
        if 'capacity' in ward_dim.columns:
            # Categorize ward size
            def categorize_capacity(capacity):
                if capacity <= 10:
                    return 'SMALL'
                elif capacity <= 30:
                    return 'MEDIUM'
                else:
                    return 'LARGE'

            ward_dim['capacity_category'] = ward_dim['capacity'].apply(categorize_capacity)

        # Add metadata
        ward_dim['dim_loaded_at'] = datetime.now()
        ward_dim['is_active'] = True

        # Remove duplicates
        if 'ward_id' in ward_dim.columns:
            ward_dim = ward_dim.drop_duplicates(subset=['ward_id'], keep='first')

        # Save to Gold layer
        gold_key = f"gold/dimensions/dim_ward.parquet"

        buffer = io.BytesIO()
        ward_dim.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Ward dimension saved to: s3://{bucket}/{gold_key}")
        print(f"  Ward count: {len(ward_dim)}")
        print(f"  Total capacity: {ward_dim['capacity'].sum() if 'capacity' in ward_dim.columns else 'N/A'}")

        return True

    except Exception as e:
        print(f"Error creating ward dimension: {e}")
        raise


def create_drug_dimension():
    """
    Create drug dimension table from Silver pharmacy data
    """

    print("Creating drug dimension table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Find latest Silver pharmacy file
        silver_prefix = "silver/pharmacy/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)

        pharmacy_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                pharmacy_key = obj['Key']
                break

        if not pharmacy_key:
            print("No Silver pharmacy file found")
            return False

        print(f"Found Silver pharmacy file: {pharmacy_key}")

        # Read pharmacy data from S3
        obj = s3.get_object(Bucket=bucket, Key=pharmacy_key)
        pharmacy_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        print(f"Loaded {len(pharmacy_df)} drug records")

        # Create drug dimension
        drug_dim = pharmacy_df.copy()

        # Select and rename columns
        column_mapping = {
            'drug_id': 'drug_id',
            'drug_name': 'drug_name',
            'category': 'drug_category',
            'threshold': 'reorder_level'
        }

        # Keep only columns that exist
        keep_columns = [col for col in column_mapping.keys() if col in drug_dim.columns]
        drug_dim = drug_dim[keep_columns]

        # Rename columns
        drug_dim = drug_dim.rename(columns={k: v for k, v in column_mapping.items() if k in drug_dim.columns})

        # Clean and standardize drug names
        if 'drug_name' in drug_dim.columns:
            drug_dim['drug_name'] = drug_dim['drug_name'].str.strip().str.title()

        # Categorize drugs by type
        if 'drug_category' in drug_dim.columns:
            # Map to standard categories
            category_map = {
                'analgesic': 'PAIN_RELIEF',
                'antibiotic': 'ANTIBIOTIC',
                'antidiabetic': 'DIABETES',
                'antihypertensive': 'BLOOD_PRESSURE',
                'antidepressant': 'MENTAL_HEALTH',
                'vitamin': 'SUPPLEMENT'
            }

            drug_dim['drug_category'] = drug_dim['drug_category'].str.lower()
            drug_dim['drug_category'] = drug_dim['drug_category'].map(category_map).fillna('OTHER')

        # Add metadata
        drug_dim['dim_loaded_at'] = datetime.now()
        drug_dim['is_active'] = True

        # Remove duplicates
        if 'drug_id' in drug_dim.columns:
            drug_dim = drug_dim.drop_duplicates(subset=['drug_id'], keep='first')

        # Save to Gold layer
        gold_key = f"gold/dimensions/dim_drug.parquet"

        buffer = io.BytesIO()
        drug_dim.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Drug dimension saved to: s3://{bucket}/{gold_key}")
        print(f"  Drug count: {len(drug_dim)}")
        print(
            f"  Categories: {drug_dim['drug_category'].value_counts().to_dict() if 'drug_category' in drug_dim.columns else 'N/A'}")

        return True

    except Exception as e:
        print(f"Error creating drug dimension: {e}")
        raise


def create_admissions_fact():
    """
    Create admissions fact table from Silver admissions data
    Links to patient and ward dimensions
    """

    print("Creating admissions fact table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Find latest Silver admissions file
        silver_prefix = "silver/admissions/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)

        admissions_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                admissions_key = obj['Key']
                break

        if not admissions_key:
            print("No Silver admissions file found")
            return False

        print(f"Found Silver admissions file: {admissions_key}")

        # Read admissions data from S3
        obj = s3.get_object(Bucket=bucket, Key=admissions_key)
        admissions_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        print(f"Loaded {len(admissions_df)} admission records")

        # Create admissions fact table
        fact_admissions = admissions_df.copy()

        # Select relevant columns
        keep_columns = []
        if 'admission_id' in fact_admissions.columns:
            keep_columns.append('admission_id')
        if 'patient_id' in fact_admissions.columns:
            keep_columns.append('patient_id')
        if 'ward_id' in fact_admissions.columns:
            keep_columns.append('ward_id')
        if 'admission_time' in fact_admissions.columns:
            keep_columns.append('admission_time')
        if 'discharge_time' in fact_admissions.columns:
            keep_columns.append('discharge_time')
        if 'triage_level' in fact_admissions.columns:
            keep_columns.append('triage_level')
        if 'length_of_stay_hours' in fact_admissions.columns:
            keep_columns.append('length_of_stay_hours')
        if 'is_active' in fact_admissions.columns:
            keep_columns.append('is_active')

        fact_admissions = fact_admissions[keep_columns]

        # Extract date keys for time dimension
        if 'admission_time' in fact_admissions.columns:
            fact_admissions['admission_date'] = fact_admissions['admission_time'].dt.date
            fact_admissions['admission_date_id'] = fact_admissions['admission_time'].dt.strftime('%Y%m%d').astype(int)

        if 'discharge_time' in fact_admissions.columns:
            fact_admissions['discharge_date'] = fact_admissions['discharge_time'].dt.date
            fact_admissions['discharge_date_id'] = fact_admissions['discharge_time'].dt.strftime('%Y%m%d').astype(int)

        # Add calculated metrics
        if 'length_of_stay_hours' in fact_admissions.columns:
            # Convert hours to days for reporting
            fact_admissions['length_of_stay_days'] = (fact_admissions['length_of_stay_hours'] / 24).round(2)

            # Categorize length of stay
            def categorize_stay_days(days):
                if days < 1:
                    return 'LESS_THAN_1_DAY'
                elif days <= 3:
                    return '1-3_DAYS'
                elif days <= 7:
                    return '4-7_DAYS'
                elif days <= 14:
                    return '8-14_DAYS'
                else:
                    return 'OVER_14_DAYS'

            fact_admissions['stay_category'] = fact_admissions['length_of_stay_days'].apply(categorize_stay_days)

        # Add metadata
        fact_admissions['fact_loaded_at'] = datetime.now()

        # Save to Gold layer
        gold_key = f"gold/facts/fact_admissions.parquet"

        buffer = io.BytesIO()
        fact_admissions.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Admissions fact saved to: s3://{bucket}/{gold_key}")
        print(f"  Admission count: {len(fact_admissions)}")

        # Show statistics
        if 'is_active' in fact_admissions.columns:
            active = fact_admissions['is_active'].sum()
            print(f"  Active admissions: {active}")

        if 'length_of_stay_days' in fact_admissions.columns:
            avg_stay = fact_admissions['length_of_stay_days'].mean()
            print(f"  Average length of stay: {avg_stay:.1f} days")

        return True

    except Exception as e:
        print(f"Error creating admissions fact: {e}")
        raise


def create_lab_turnaround_fact():
    """
    Create lab turnaround fact table from Silver lab results
    Measures lab test efficiency
    """

    print("Creating lab turnaround fact table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Find latest Silver labs file
        silver_prefix = "silver/labs/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)

        labs_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                labs_key = obj['Key']
                break

        if not labs_key:
            print("No Silver labs file found")
            return False

        print(f"Found Silver labs file: {labs_key}")

        # Read labs data from S3
        obj = s3.get_object(Bucket=bucket, Key=labs_key)
        labs_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        print(f"Loaded {len(labs_df)} lab records")

        # Create lab fact table
        fact_labs = labs_df.copy()

        # Select relevant columns
        keep_columns = []
        if 'test_id' in fact_labs.columns:
            keep_columns.append('test_id')
        if 'patient_id' in fact_labs.columns:
            keep_columns.append('patient_id')
        if 'test_name' in fact_labs.columns:
            keep_columns.append('test_name')
        if 'result' in fact_labs.columns:
            keep_columns.append('result')
        if 'result_category' in fact_labs.columns:
            keep_columns.append('result_category')
        if 'sample_time' in fact_labs.columns:
            keep_columns.append('sample_time')
        if 'completed_time' in fact_labs.columns:
            keep_columns.append('completed_time')
        if 'turnaround_hours' in fact_labs.columns:
            keep_columns.append('turnaround_hours')
        if 'is_pending' in fact_labs.columns:
            keep_columns.append('is_pending')

        fact_labs = fact_labs[keep_columns]

        # Extract date keys
        if 'sample_time' in fact_labs.columns:
            fact_labs['sample_date'] = fact_labs['sample_time'].dt.date
            fact_labs['sample_date_id'] = fact_labs['sample_time'].dt.strftime('%Y%m%d').astype(int)

        if 'completed_time' in fact_labs.columns:
            fact_labs['completed_date'] = fact_labs['completed_time'].dt.date
            fact_labs['completed_date_id'] = fact_labs['completed_time'].dt.strftime('%Y%m%d').astype(int)

        # Categorize turnaround time
        if 'turnaround_hours' in fact_labs.columns:
            def categorize_turnaround(hours):
                if hours < 0:  # Pending
                    return 'PENDING'
                elif hours <= 1:
                    return 'UNDER_1_HOUR'
                elif hours <= 4:
                    return '1-4_HOURS'
                elif hours <= 12:
                    return '4-12_HOURS'
                elif hours <= 24:
                    return '12-24_HOURS'
                else:
                    return 'OVER_24_HOURS'

            fact_labs['turnaround_category'] = fact_labs['turnaround_hours'].apply(categorize_turnaround)

        # Add test type categorization
        if 'test_name' in fact_labs.columns:
            def categorize_test_type(test_name):
                test_lower = str(test_name).lower()
                if any(word in test_lower for word in ['blood', 'cbc', 'hemoglobin']):
                    return 'BLOOD_TEST'
                elif any(word in test_lower for word in ['urine', 'urinalysis']):
                    return 'URINE_TEST'
                elif any(word in test_lower for word in ['x-ray', 'xray', 'radiography']):
                    return 'IMAGING'
                elif any(word in test_lower for word in ['mri', 'ct', 'scan']):
                    return 'ADVANCED_IMAGING'
                elif any(word in test_lower for word in ['culture', 'microbiology']):
                    return 'MICROBIOLOGY'
                else:
                    return 'OTHER'

            fact_labs['test_type'] = fact_labs['test_name'].apply(categorize_test_type)

        # Add metadata
        fact_labs['fact_loaded_at'] = datetime.now()

        # Save to Gold layer
        gold_key = f"gold/facts/fact_lab_turnaround.parquet"

        buffer = io.BytesIO()
        fact_labs.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Lab turnaround fact saved to: s3://{bucket}/{gold_key}")
        print(f"  Lab test count: {len(fact_labs)}")

        # Show statistics
        if 'is_pending' in fact_labs.columns:
            pending = fact_labs['is_pending'].sum()
            print(f"  Pending tests: {pending}")

        if 'turnaround_hours' in fact_labs.columns:
            completed = fact_labs[fact_labs['turnaround_hours'] >= 0]
            if len(completed) > 0:
                avg_turnaround = completed['turnaround_hours'].mean()
                print(f"  Average turnaround: {avg_turnaround:.1f} hours")

        return True

    except Exception as e:
        print(f"Error creating lab turnaround fact: {e}")
        raise


def create_pharmacy_stock_fact():
    """
    Create pharmacy stock fact table from Silver pharmacy data
    Tracks drug inventory levels and risks
    """

    print("Creating pharmacy stock fact table")

    if not aws_config:
        raise ValueError("AWS configuration not found")

    try:
        s3 = aws_config.get_s3_client()
    except ValueError as e:
        print(f"Error: {e}")
        raise

    bucket = aws_config.bucket_name
    today = get_todays_date()

    try:
        # Find latest Silver pharmacy file
        silver_prefix = "silver/pharmacy/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=silver_prefix)

        pharmacy_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                pharmacy_key = obj['Key']
                break

        if not pharmacy_key:
            print("No Silver pharmacy file found")
            return False

        print(f"Found Silver pharmacy file: {pharmacy_key}")

        # Read pharmacy data from S3
        obj = s3.get_object(Bucket=bucket, Key=pharmacy_key)
        pharmacy_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        print(f"Loaded {len(pharmacy_df)} drug records")

        # Create pharmacy stock fact table
        fact_pharmacy = pharmacy_df.copy()

        # Select relevant columns
        keep_columns = []
        if 'drug_id' in fact_pharmacy.columns:
            keep_columns.append('drug_id')
        if 'current_stock' in fact_pharmacy.columns:
            keep_columns.append('current_stock')
        if 'threshold' in fact_pharmacy.columns:
            keep_columns.append('threshold')
        if 'expiry_date' in fact_pharmacy.columns:
            keep_columns.append('expiry_date')
        if 'last_updated' in fact_pharmacy.columns:
            keep_columns.append('last_updated')
        if 'stockout_risk' in fact_pharmacy.columns:
            keep_columns.append('stockout_risk')
        if 'stock_percentage' in fact_pharmacy.columns:
            keep_columns.append('stock_percentage')
        if 'days_until_expiry' in fact_pharmacy.columns:
            keep_columns.append('days_until_expiry')
        if 'expiry_risk' in fact_pharmacy.columns:
            keep_columns.append('expiry_risk')

        fact_pharmacy = fact_pharmacy[keep_columns]

        # Extract date keys
        if 'expiry_date' in fact_pharmacy.columns:
            fact_pharmacy['expiry_date_id'] = fact_pharmacy['expiry_date'].dt.strftime('%Y%m%d').astype(int)

        if 'last_updated' in fact_pharmacy.columns:
            fact_pharmacy['last_updated_date'] = fact_pharmacy['last_updated'].dt.date
            fact_pharmacy['last_updated_date_id'] = fact_pharmacy['last_updated'].dt.strftime('%Y%m%d').astype(int)

        # Calculate additional metrics
        if 'current_stock' in fact_pharmacy.columns and 'threshold' in fact_pharmacy.columns:
            # Calculate days of supply (assuming average daily usage)
            # For simplicity, using threshold as proxy for daily usage
            fact_pharmacy['days_of_supply'] = (fact_pharmacy['current_stock'] / fact_pharmacy['threshold'] * 30).round(
                1)

            # Categorize stock level
            def categorize_stock_level(stock_percentage):
                if stock_percentage < 50:
                    return 'CRITICAL'
                elif stock_percentage < 80:
                    return 'LOW'
                elif stock_percentage < 120:
                    return 'NORMAL'
                else:
                    return 'HIGH'

            if 'stock_percentage' in fact_pharmacy.columns:
                fact_pharmacy['stock_level_category'] = fact_pharmacy['stock_percentage'].apply(categorize_stock_level)

        # Add metadata
        fact_pharmacy['fact_loaded_at'] = datetime.now()

        # Save to Gold layer
        gold_key = f"gold/facts/fact_pharmacy_stock.parquet"

        buffer = io.BytesIO()
        fact_pharmacy.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=gold_key,
            Body=buffer,
            ContentType='application/parquet'
        )

        print(f"✓ Pharmacy stock fact saved to: s3://{bucket}/{gold_key}")
        print(f"  Drug count: {len(fact_pharmacy)}")

        # Show statistics
        if 'stockout_risk' in fact_pharmacy.columns:
            at_risk = fact_pharmacy['stockout_risk'].sum()
            print(f"  Drugs at stockout risk: {at_risk}")

        if 'expiry_risk' in fact_pharmacy.columns:
            expiring = fact_pharmacy['expiry_risk'].sum()
            print(f"  Drugs expiring soon: {expiring}")

        return True

    except Exception as e:
        print(f"Error creating pharmacy stock fact: {e}")
        raise


with DAG(
        'silver_to_gold',
        default_args=default_args,
        description='Transform Silver layer data to Gold layer (analytics-ready star schema)',
        schedule_interval='@daily',
        catchup=False,
        tags=['transformation', 'gold', 'data_warehouse', 'star_schema']
) as dag:
    # Dimension tables (run first)
    date_dim_task = PythonOperator(
        task_id='create_date_dimension',
        python_callable=create_date_dimension,
    )

    patient_dim_task = PythonOperator(
        task_id='create_patient_dimension',
        python_callable=create_patient_dimension,
    )

    ward_dim_task = PythonOperator(
        task_id='create_ward_dimension',
        python_callable=create_ward_dimension,
    )

    drug_dim_task = PythonOperator(
        task_id='create_drug_dimension',
        python_callable=create_drug_dimension,
    )

    # Fact tables (run after dimensions)
    admissions_fact_task = PythonOperator(
        task_id='create_admissions_fact',
        python_callable=create_admissions_fact,
    )

    lab_fact_task = PythonOperator(
        task_id='create_lab_turnaround_fact',
        python_callable=create_lab_turnaround_fact,
    )

    pharmacy_fact_task = PythonOperator(
        task_id='create_pharmacy_stock_fact',
        python_callable=create_pharmacy_stock_fact,
    )

    # Each dimension goes to each fact
    for dim in [patient_dim_task, ward_dim_task, drug_dim_task]:
        for fact in [admissions_fact_task, lab_fact_task, pharmacy_fact_task]:
            dim >> fact
