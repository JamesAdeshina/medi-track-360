from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import os
import json

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}


def extract_pharmacy_to_local():
    """Extract pharmacy data from API and save locally"""

    print("Starting Pharmacy API extraction to LOCAL...")

    # API endpoint
    api_url = "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json"

    # Local directory
    base_dir = "data/bronze/pharmacy"
    os.makedirs(base_dir, exist_ok=True)

    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        # Fetch data from API
        print(f"Fetching data from: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()

        data = response.json()
        print(f"API Response type: {type(data)}")
        print(f"API Response keys: {list(data.keys())}")

        # Check if data is a dictionary with 'drugs' key
        if isinstance(data, dict) and 'drugs' in data:
            drugs_data = data['drugs']
            print(f"Found 'drugs' key with type: {type(drugs_data)}")

            if isinstance(drugs_data, list):
                print(f"Number of drug records: {len(drugs_data)}")
                df = pd.DataFrame(drugs_data)
            else:
                print("Warning: 'drugs' is not a list, creating DataFrame from dict")
                df = pd.DataFrame([drugs_data])
        elif isinstance(data, list):
            print(f"API returned list with {len(data)} items")
            df = pd.DataFrame(data)
        else:
            print(f"Unexpected data structure: {type(data)}")
            df = pd.DataFrame([data])

        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        if len(df) > 0:
            # Save as JSON locally (full response)
            json_path = f"{base_dir}/pharmacy_inventory_{today}.json"
            with open(json_path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Saved full JSON to: {json_path}")

            # Save as CSV locally (just the drugs data)
            csv_path = f"{base_dir}/pharmacy_inventory_{today}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Saved CSV to: {csv_path}")

            # Save as Parquet locally
            parquet_path = f"{base_dir}/pharmacy_inventory_{today}.parquet"
            df.to_parquet(parquet_path)
            print(f"Saved Parquet to: {parquet_path}")

            # Show data sample
            print(f"\nSample data (first record):")
            if len(df) > 0:
                print(json.dumps(df.iloc[0].to_dict(), indent=2))
            else:
                print("No data in DataFrame")

            # Show summary statistics
            print(f"\nData summary:")
            print(f"  Total records: {len(df)}")
            print(f"  Columns: {len(df.columns)}")
            for col in df.columns[:5]:  # Show first 5 columns
                print(f"  - {col}: {df[col].dtype}")
                if df[col].dtype == 'object':
                    unique_vals = df[col].nunique()
                    print(f"    Unique values: {unique_vals}")

        print("✓ Pharmacy API extraction to LOCAL complete!")

    except Exception as e:
        print(f"✗ Pharmacy API extraction failed: {e}")
        print(f"Error details: {str(e)}")
        raise


with DAG(
        'pharmacy_api_to_bronze_local',
        default_args=default_args,
        description='Extract pharmacy data from API to LOCAL Bronze',
        schedule_interval='@daily',
        catchup=False,
        tags=['ingestion', 'bronze', 'api', 'local']
) as dag:
    extract_task = PythonOperator(
        task_id='extract_pharmacy_to_local',
        python_callable=extract_pharmacy_to_local,
    )

    extract_task