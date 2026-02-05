"""
Push Gold layer data from S3 directly into Redshift (row inserts).

Notes
1) This uses boto3 to fetch Parquet from S3 and psycopg2 to insert into Redshift.
2) It is slower than Redshift COPY for large volumes, but fine for your current small dataset.
3) Credentials must come from environment variables or AWS profile, not hardcoded.
"""

import os
import io
import boto3
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

S3_BUCKET = os.getenv("S3_BUCKET", "medi-track-360-data-lake")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

TABLES = {
    "dim_date": "gold/dimensions/dim_date.parquet",
    "dim_patient": "gold/dimensions/dim_patient.parquet",
    "dim_ward": "gold/dimensions/dim_ward.parquet",
    "dim_drug": "gold/dimensions/dim_drug.parquet",
    "fact_admissions": "gold/facts/fact_admissions.parquet",
    "fact_lab_turnaround": "gold/facts/fact_lab_turnaround.parquet",
    "fact_pharmacy_stock": "gold/facts/fact_pharmacy_stock.parquet",
}

REDSHIFT_CONFIG = {
    "host": os.environ["REDSHIFT_HOST"],
    "port": int(os.getenv("REDSHIFT_PORT", "5439")),
    "database": os.environ["REDSHIFT_DB"],
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PASSWORD"],
    "sslmode": os.getenv("REDSHIFT_SSLMODE", "require"),
}


def read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    print(f"  Reading s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


def get_redshift_columns(table_name: str, conn) -> list:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]


def clean_for_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    if table_name == "fact_lab_turnaround":
        print("  Columns in parquet:", list(df.columns))

        if "test_id" not in df.columns:
            print("  Adding missing test_id with sentinel -1")
            df["test_id"] = -1

        null_count = int(df["test_id"].isna().sum())
        if null_count > 0:
            before = len(df)
            df = df[df["test_id"].notna()].copy()
            after = len(df)
            print(f"  Dropped {before - after:,} rows where test_id was NULL")

        try:
            df["test_id"] = df["test_id"].astype("Int64")
        except Exception:
            pass

    return df


def push_dataframe_to_redshift(df: pd.DataFrame, table_name: str, conn) -> None:
    if df.empty:
        print(f"  Skipping {table_name} because dataframe is empty")
        return

    df = df.where(pd.notna(df), None)

    target_cols = get_redshift_columns(table_name, conn)
    df_cols = list(df.columns)

    use_cols = [c for c in target_cols if c in df_cols]
    extra_in_df = [c for c in df_cols if c not in target_cols]
    missing_in_df = [c for c in target_cols if c not in df_cols]

    if extra_in_df:
        print(f"  Ignoring parquet-only columns (not in Redshift): {extra_in_df}")
    if missing_in_df:
        print(f"  Warning: Redshift columns missing in parquet: {missing_in_df}")

    if not use_cols:
        raise ValueError(f"No matching columns between parquet and Redshift for {table_name}")

    df = df[use_cols]

    values = [tuple(row) for row in df.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({','.join(use_cols)}) VALUES %s"

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name};")
        execute_values(cur, insert_sql, values, page_size=2000)

    print(f"  Inserted {len(values):,} rows into {table_name}")


def main() -> None:
    print("=" * 60)
    print("Pushing Gold data → Redshift (no Airflow)")
    print("=" * 60)

    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = False

    try:
        for table, s3_key in TABLES.items():
            print(f"\nProcessing {table}")
            df = read_parquet_from_s3(S3_BUCKET, s3_key)
            df = clean_for_table(df, table)
            push_dataframe_to_redshift(df, table, conn)
            conn.commit()
            print(f"  Committed {table}")

        print("\nALL TABLES LOADED SUCCESSFULLY")

    except Exception as e:
        conn.rollback()
        print(f"\nFAILED: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()
