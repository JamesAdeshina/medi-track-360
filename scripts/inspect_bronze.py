import pandas as pd
import json
import os
from pathlib import Path
import numpy as np


def inspect_bronze():
    """Inspect Bronze layer data structure and quality"""

    print("=" * 60)
    print("BRONZE LAYER DATA INSPECTION")
    print("=" * 60)

    base_path = "docker/data/bronze"

    # Check each data source
    sources = {
        'postgres': ['patients', 'admissions', 'wards', 'bed_assignments'],
        'pharmacy': ['pharmacy_inventory'],
        'lab_results': ['lab_results']
    }

    for source, tables in sources.items():
        print(f"\n {source.upper()} DATA:")
        print("-" * 40)

        source_path = f"{base_path}/{source}"

        if not os.path.exists(source_path):
            print(f"  ✗ Directory not found: {source_path}")
            continue

        # Find all files
        all_files = []
        for root, dirs, files in os.walk(source_path):
            for file in files:
                if file.endswith(('.csv', '.parquet', '.json')):
                    all_files.append(os.path.join(root, file))

        if not all_files:
            print(f"  ️ No data files found in {source_path}")
            continue

        # Analyze each file
        for file_path in all_files[:5]:  # Limit to first 5 files per source
            try:
                file_name = os.path.basename(file_path)
                print(f"\n   File: {file_name}")
                print(f"     Path: {file_path}")

                # Read based on file type
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                    file_type = "CSV"
                elif file_path.endswith('.parquet'):
                    df = pd.read_parquet(file_path)
                    file_type = "Parquet"
                elif file_path.endswith('.json'):
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    else:
                        df = pd.DataFrame([data])
                    file_type = "JSON"
                else:
                    continue

                print(f"     Type: {file_type}")
                print(f"     Shape: {df.shape[0]} rows × {df.shape[1]} columns")

                # Column analysis
                print(f"     Columns ({len(df.columns)}):")
                for i, col in enumerate(df.columns[:10], 1):  # Show first 10 columns
                    col_type = str(df[col].dtype)
                    null_count = df[col].isnull().sum()
                    null_pct = (null_count / len(df)) * 100

                    if df[col].dtype in ['int64', 'float64']:
                        min_val = df[col].min()
                        max_val = df[col].max()
                        print(f"       {i:2d}. {col:30s} [{col_type:10s}] "
                              f"Nulls: {null_count:4d} ({null_pct:5.1f}%) "
                              f"Range: [{min_val:.2f} - {max_val:.2f}]")
                    else:
                        # For string/object columns
                        unique_count = df[col].nunique()
                        sample_values = df[col].dropna().unique()[:3]
                        sample_str = ', '.join([str(v) for v in sample_values])[:50]

                        print(f"       {i:2d}. {col:30s} [{col_type:10s}] "
                              f"Nulls: {null_count:4d} ({null_pct:5.1f}%) "
                              f"Unique: {unique_count:4d}")
                        if sample_str:
                            print(f"            Sample: {sample_str}...")

                if len(df.columns) > 10:
                    print(f"       ... and {len(df.columns) - 10} more columns")

                # Data quality issues
                issues = []
                if df.isnull().sum().sum() > 0:
                    total_nulls = df.isnull().sum().sum()
                    issues.append(f"{total_nulls} null values")

                # Check for duplicate rows
                duplicates = df.duplicated().sum()
                if duplicates > 0:
                    issues.append(f"{duplicates} duplicate rows")

                # Check date columns
                date_cols = [col for col in df.columns if any(word in col.lower()
                                                              for word in
                                                              ['date', 'time', 'dob', 'admission', 'discharge'])]
                if date_cols:
                    print(f"      Date-like columns: {date_cols}")

                if issues:
                    print(f"     ️  Data quality issues: {', '.join(issues)}")

                # Show first 2 rows
                print(f"\n      First 2 rows:")
                for idx, row in df.head(2).iterrows():
                    row_data = {k: v for k, v in row.items()
                                if not pd.isna(v) and str(v).strip() != ''}
                    row_str = ', '.join([f"{k}: {v}" for k, v in list(row_data.items())[:5]])
                    if len(row_data) > 5:
                        row_str += f"... (+{len(row_data) - 5} more)"
                    print(f"       Row {idx}: {row_str}")

            except Exception as e:
                print(f"      Error reading {file_path}: {e}")
                continue

    print("\n" + "=" * 60)
    print("SUMMARY OF BRONZE LAYER")
    print("=" * 60)

    # Count total records
    total_records = 0
    total_files = 0

    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(('.csv', '.parquet')):
                total_files += 1
                try:
                    if file.endswith('.csv'):
                        df = pd.read_csv(os.path.join(root, file), nrows=1)
                    else:
                        df = pd.read_parquet(os.path.join(root, file))
                    total_records += len(df)
                except:
                    pass

    print(f"\n Total Files: {total_files}")
    print(f" Total Records: {total_records:,}")

    # List all folders
    print(f"\n Folder Structure:")
    for root, dirs, files in os.walk(base_path):
        level = root.replace(base_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f'{indent}├── {os.path.basename(root) or "bronze"}/')
        sub_indent = ' ' * 2 * (level + 1)
        for file in files[:3]:  # Show first 3 files per folder
            print(f'{sub_indent}├── {file}')
        if len(files) > 3:
            print(f'{sub_indent}└── ... (+{len(files) - 3} more)')


if __name__ == "__main__":
    inspect_bronze()
