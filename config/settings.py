import os
from enum import Enum


class StorageMode(Enum):
    LOCAL = "local"
    S3 = "s3"


class Config:
    def __init__(self):
        # Default to local, can be overridden
        self.storage_mode = StorageMode.LOCAL

        # Local paths
        self.local_base_path = "data"

        # S3 configuration (when available)
        self.s3_bucket = "medi-track-360-data-lake"
        self.s3_region = "eu-north-1"

        # PostgreSQL connection
        self.pg_host = "aws-1-eu-central-1.pooler.supabase.com"
        self.pg_port = 5432
        self.pg_db = "postgres"
        self.pg_user = "medi_reader.hceprxhtdgtbqmrfwymn"
        self.pg_password = "medi_reader123"

        # Pharmacy API
        self.pharmacy_api_url = "https://raw.githubusercontent.com/RofiatAbdulkareem/data-repo/refs/heads/main/data/pharmacy_inventory.json"

    def set_storage_mode(self, mode: StorageMode):
        """Switch between local and S3 storage"""
        self.storage_mode = mode
        print(f"Storage mode set to: {mode.value}")

    def get_storage_handler(self):
        """Get appropriate storage handler based on mode"""
        if self.storage_mode == StorageMode.LOCAL:
            return LocalStorage(self.local_base_path)
        else:
            return S3Storage(self.s3_bucket, self.s3_region)

    def get_path(self, layer, source, filename=None):
        """Get path for any file in the data lake"""
        if filename:
            return f"{layer}/{source}/{filename}"
        else:
            return f"{layer}/{source}/"


# Storage Interface
class StorageHandler:
    def save_file(self, df, path, format='parquet'):
        raise NotImplementedError

    def read_file(self, path, format='parquet'):
        raise NotImplementedError

    def list_files(self, prefix):
        raise NotImplementedError


# Local Storage Implementation
class LocalStorage(StorageHandler):
    def __init__(self, base_path):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def save_file(self, df, path, format='parquet'):
        full_path = f"{self.base_path}/{path}"
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        if format == 'parquet':
            df.to_parquet(full_path)
        elif format == 'csv':
            df.to_csv(full_path, index=False)
        return full_path

    def read_file(self, path, format='parquet'):
        full_path = f"{self.base_path}/{path}"
        if format == 'parquet':
            return pd.read_parquet(full_path)
        elif format == 'csv':
            return pd.read_csv(full_path)

    def list_files(self, prefix):
        import glob
        search_path = f"{self.base_path}/{prefix}/**"
        files = glob.glob(search_path, recursive=True)
        return [f.replace(f"{self.base_path}/", "") for f in files if os.path.isfile(f)]


# S3 Storage Implementation (for when AWS is available)
class S3Storage(StorageHandler):
    def __init__(self, bucket, region):
        import boto3
        self.bucket = bucket
        self.s3 = boto3.client('s3', region_name=region)

    def save_file(self, df, path, format='parquet'):
        import io
        buffer = io.BytesIO()

        if format == 'parquet':
            df.to_parquet(buffer)
        elif format == 'csv':
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer = io.BytesIO(buffer.getvalue().encode())

        buffer.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=buffer)
        return f"s3://{self.bucket}/{path}"

    def read_file(self, path, format='parquet'):
        import io
        import pandas as pd

        obj = self.s3.get_object(Bucket=self.bucket, Key=path)
        buffer = io.BytesIO(obj['Body'].read())

        if format == 'parquet':
            return pd.read_parquet(buffer)
        elif format == 'csv':
            return pd.read_csv(buffer)

    def list_files(self, prefix):
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        return []


# Global config instance
config = Config()