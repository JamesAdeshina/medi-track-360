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

def extract_pharmacy_to_local_fixed():
    """Extract pharmacy data from API and save inside container"""
    
    print("Starting Pharmacy API extraction (FIXED)...")
    
    # API endpoint
    api_url = "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json"
    
    # Write INSIDE container (not to host filesystem)
    base_dir = "/tmp/bronze/pharmacy"
    os.makedirs(base_dir, exist_ok=True)
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    
    try:
        # Fetch data from API
        print(f"Fetching data from: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"Received {len(data)} records")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        print(f"Data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        if len(df) > 0:
            # Save as JSON inside container
            json_path = f"{base_dir}/pharmacy_inventory_{today}.json"
            with open(json_path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Saved JSON to: {json_path}")
            
            # Save as CSV inside container
            csv_path = f"{base_dir}/pharmacy_inventory_{today}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Saved CSV to: {csv_path}")
            
            # Show data sample
            print(f"\nSample data (first record):")
            print(json.dumps(data[0], indent=2) if data else "No data")
        
        print("✓ Pharmacy API extraction complete!")
        
    except Exception as e:
        print(f"✗ Pharmacy API extraction failed: {e}")
        raise

with DAG(
    'pharmacy_api_to_bronze_local_fixed',
    default_args=default_args,
    description='FIXED: Extract pharmacy data from API (writes inside container)',
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'bronze', 'api', 'local', 'fixed']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_pharmacy_to_local_fixed',
        python_callable=extract_pharmacy_to_local_fixed,
    )
    
    extract_task
