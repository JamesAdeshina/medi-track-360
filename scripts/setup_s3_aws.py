import boto3
from botocore.exceptions import ClientError
from config.aws_credentials import aws_creds


def create_s3_bucket():
    """Create S3 bucket if it doesn't exist"""

    print("=" * 60)
    print("SETTING UP AWS S3 FOR MEDITRACK360")
    print("=" * 60)

    # Get S3 client
    s3 = aws_creds.get_s3_client()
    if not s3:
        return False

    bucket_name = aws_creds.bucket_name

    try:
        # Check if bucket exists
        print(f"Checking bucket: {bucket_name}")
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket already exists: {bucket_name}")
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                print(f"Creating bucket: {bucket_name} in {aws_creds.region}")

                if aws_creds.region == 'us-east-1':
                    s3.create_bucket(Bucket=bucket_name)
                else:
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': aws_creds.region
                        }
                    )

                print(f"Bucket created: {bucket_name}")
                return True

            except Exception as create_error:
                print(f" Error creating bucket: {create_error}")
                return False
        else:
            print(f" Error checking bucket: {e}")
            return False


def create_s3_folder_structure():
    """Create folder structure in S3 bucket"""

    print("\n" + "=" * 60)
    print("CREATING S3 FOLDER STRUCTURE")
    print("=" * 60)

    s3 = aws_creds.get_s3_client()
    if not s3:
        return False

    bucket_name = aws_creds.bucket_name

    # Bronze layer folders (raw data)
    bronze_folders = [
        'bronze/postgres/',
        'bronze/pharmacy/',
        'bronze/lab_results/',
        'bronze/admissions/',
        'bronze/wards/',
        'bronze/patients/',
        'bronze/bed_assignments/',
    ]

    # Silver layer folders (cleaned data)
    silver_folders = [
        'silver/admissions/',
        'silver/pharmacy/',
        'silver/labs/',
        'silver/patients/',
        'silver/wards/',
        'silver/dimensions/',
    ]

    # Gold layer folders (analytics ready)
    gold_folders = [
        'gold/facts/',
        'gold/dimensions/',
        'gold/aggregated/',
        'gold/fact_admissions/',
        'gold/fact_lab_turnaround/',
        'gold/fact_pharmacy_stock/',
        'gold/dim_patient/',
        'gold/dim_ward/',
        'gold/dim_drug/',
        'gold/dim_date/',
    ]

    all_folders = bronze_folders + silver_folders + gold_folders

    created_count = 0
    error_count = 0

    for folder in all_folders:
        try:
            # Create folder by putting an empty object
            s3.put_object(Bucket=bucket_name, Key=folder)
            print(f" Created: {folder}")
            created_count += 1

        except Exception as e:
            print(f" Error creating {folder}: {e}")
            error_count += 1

    print(f"\n Created {created_count} folders")
    if error_count > 0:
        print(f" {error_count} folders failed")

    return error_count == 0


def verify_s3_access():
    """Verify we can read/write to S3"""

    print("\n" + "=" * 60)
    print("VERIFYING S3 ACCESS")
    print("=" * 60)

    s3 = aws_creds.get_s3_client()
    if not s3:
        return False

    bucket_name = aws_creds.bucket_name

    try:
        # List buckets
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        print(f"Available buckets: {len(buckets)}")
        for bucket in buckets[:5]:  # Show first 5
            print(f"  - {bucket}")

        if len(buckets) > 5:
            print(f"  ... and {len(buckets) - 5} more")

        # Check our bucket exists
        if bucket_name in buckets:
            print(f"\n Our bucket exists: {bucket_name}")

            # Test write access
            test_key = 'test_access.txt'
            test_content = b'MediTrack360 S3 Access Test - ' + str.encode(str(pd.Timestamp.now()))

            s3.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_content
            )
            print(f" Write test successful: s3://{bucket_name}/{test_key}")

            # Test read access
            response = s3.get_object(Bucket=bucket_name, Key=test_key)
            content = response['Body'].read().decode('utf-8')
            print(f" Read test successful: {content[:50]}...")

            # Clean up test file
            s3.delete_object(Bucket=bucket_name, Key=test_key)
            print(f" Cleanup test file successful")

            return True
        else:
            print(f"\n Our bucket not found: {bucket_name}")
            return False

    except Exception as e:
        print(f"S3 access verification failed: {e}")
        return False


def list_s3_structure():
    """List current S3 structure"""

    print("\n" + "=" * 60)
    print("CURRENT S3 STRUCTURE")
    print("=" * 60)

    s3 = aws_creds.get_s3_client()
    if not s3:
        return

    bucket_name = aws_creds.bucket_name

    try:
        # List objects with pagination
        paginator = s3.get_paginator('list_objects_v2')
        folder_structure = {}

        for page in paginator.paginate(Bucket=bucket_name, Delimiter='/'):
            # Common prefixes are "folders"
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    folder = prefix['Prefix']
                    folder_structure[folder] = []

                    # List files in this folder
                    folder_objects = s3.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=folder,
                        MaxKeys=10
                    )

                    if 'Contents' in folder_objects:
                        for obj in folder_objects['Contents']:
                            if obj['Key'] != folder:  # Skip the folder itself
                                file_name = obj['Key'].replace(folder, '')
                                size_mb = obj['Size'] / (1024 * 1024)
                                folder_structure[folder].append({
                                    'name': file_name,
                                    'size_mb': round(size_mb, 2)
                                })

        # Print structure
        for folder, files in folder_structure.items():
            print(f"\n {folder}")
            if files:
                for file in files[:5]:  # Show first 5 files
                    print(f"  ├── {file['name']} ({file['size_mb']} MB)")
                if len(files) > 5:
                    print(f"  └── ... (+{len(files) - 5} more)")
            else:
                print(f"  └── (empty)")

    except Exception as e:
        print(f"Error listing S3 structure: {e}")


if __name__ == "__main__":
    import pandas as pd

    # Test AWS credentials first
    session = aws_creds.setup_session()
    if not session:
        print("AWS setup failed. Check credentials.")
        exit(1)

    # Create bucket if needed
    if create_s3_bucket():
        # Create folder structure
        create_s3_folder_structure()

        # Verify access
        verify_s3_access()

        # List structure
        list_s3_structure()

        print("\n" + "=" * 60)
        print("AWS S3 SETUP COMPLETE!")
        print("=" * 60)
        print(f"\nBucket: {aws_creds.bucket_name}")
        print(f"Region: {aws_creds.region}")
        print(f"\nReady to upload Bronze layer data to S3!")
    else:
        print("\n" + "=" * 60)
        print("AWS S3 SETUP FAILED")
        print("=" * 60)