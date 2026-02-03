# verify_bronze_data.py
import os
import pandas as pd
import json
from pathlib import Path

print("=" * 60)
print("BRONZE LAYER VERIFICATION")
print("=" * 60)

base_dir = "docker/data/bronze"
total_records = 0

# Check each source
sources = {
    "Pharmacy": "pharmacy/*.csv",
    "Patients": "patients/*.csv",
    "Admissions": "admissions/*.csv",
    "Wards": "wards/*.csv",
    "Lab Results": "lab_results/*/*.csv"
}

for source_name, pattern in sources.items():
    files = list(Path(base_dir).glob(pattern))

    if files:
        latest_file = sorted(files)[-1]
        try:
            df = pd.read_csv(latest_file)
            print(f"✓ {source_name}:")
            print(f"  File: {latest_file.name}")
            print(f"  Records: {len(df):,}")
            print(f"  Columns: {len(df.columns)}")
            print(f"  Sample columns: {list(df.columns)[:5]}...")

            total_records += len(df)

        except Exception as e:
            print(f"✗ {source_name}: Error reading {latest_file} - {e}")
    else:
        print(f"✗ {source_name}: No files found")

print("\n" + "=" * 60)
print(f"TOTAL RECORDS IN BRONZE LAYER: {total_records:,}")
print("=" * 60)

# Show data samples
print("\nDATA SAMPLES:")
print("-" * 40)

# Pharmacy sample
pharmacy_files = list(Path("data/bronze/pharmacy").glob("*.csv"))
if pharmacy_files:
    df_pharmacy = pd.read_csv(sorted(pharmacy_files)[-1])
    print(f"\nPharmacy (first record):")
    print(json.dumps(df_pharmacy.iloc[0].to_dict(), indent=2))

# Patients sample
patient_files = list(Path("data/bronze/patients").glob("*.csv"))
if patient_files:
    df_patients = pd.read_csv(sorted(patient_files)[-1])
    print(f"\nPatients (first record):")
    print(json.dumps(df_patients.iloc[0].to_dict(), indent=2))