import boto3
from botocore.exceptions import ClientError
import os


def create_s3_bucket_if_not_exists(bucket_name, region='us-east-1'):
    """Create S3 bucket if it doesn't exist"""

    s3_client = boto3.client('s3', region_name=region)

    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"Created bucket: {bucket_name} in region: {region}")
                return True
            except Exception as create_error:
                print(f"Error creating bucket: {create_error}")
                return False
        else:
            print(f"Error checking bucket: {e}")
            return False


def setup_aws_connection():
    """Setup AWS connection and create bucket"""

    # Bucket name
    bucket_name = "medi-track-360-data-lake"

    # Check AWS credentials
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"AWS Account ID: {identity['Account']}")
        print(f"AWS User ARN: {identity['Arn']}")
    except Exception as e:
        print(f"AWS credentials error: {e}")
        print("Please configure AWS CLI: aws configure")
        return False

    # Create S3 bucket
    if create_s3_bucket_if_not_exists(bucket_name):
        print("AWS setup complete!")
        return True
    else:
        print("AWS setup failed!")
        return False


if __name__ == "__main__":
    setup_aws_connection()