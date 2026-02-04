"""
AWS Configuration for MediTrack360
Loads credentials from environment variables for security
"""

import boto3
import os
from botocore.exceptions import ClientError

class AWSConfig:
    """
    Manages AWS configuration and S3 client creation
    Credentials are loaded from environment variables for security
    """

    def __init__(self):
        # S3 bucket name for our project
        self.bucket_name = 'medi-track-360-data-lake'

        # AWS credentials will be loaded from environment variables
        self.access_key = None
        self.secret_key = None
        self.region = None

        # Cache for S3 client
        self._s3_client = None

        # Load credentials on initialization
        self._load_credentials()

    def _load_credentials(self):
        """
        Load AWS credentials from environment variables
        Environment variables should be set in Docker Compose .env file
        """
        self.access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'eu-north-1')

        # Log status (but not the actual keys)
        if self.access_key and self.secret_key:
            print("AWS credentials loaded from environment variables")
        else:
            print("WARNING: AWS credentials not found in environment variables")
            print("Please ensure these environment variables are set:")
            print("  - AWS_ACCESS_KEY_ID")
            print("  - AWS_SECRET_ACCESS_KEY")
            print("  - AWS_DEFAULT_REGION (optional, defaults to eu-north-1)")

    def get_s3_client(self):
        """
        Get or create S3 client using credentials from environment variables

        Returns:
            boto3 S3 client

        Raises:
            ValueError: If credentials are not configured
        """
        if self._s3_client is None:
            # Check if credentials are available
            if not self.access_key or not self.secret_key:
                raise ValueError(
                    "AWS credentials not configured. "
                    "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
                    "in your environment variables."
                )

            # Create S3 client with loaded credentials
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )

        return self._s3_client

    def test_connection(self):
        """
        Test AWS connection by listing S3 buckets

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            s3 = self.get_s3_client()

            # Try to list buckets (minimal permission check)
            response = s3.list_buckets()

            # Check if our bucket exists
            bucket_names = [bucket['Name'] for bucket in response['Buckets']]
            if self.bucket_name in bucket_names:
                print(f"AWS connection successful. Bucket '{self.bucket_name}' exists.")
            else:
                print(f"AWS connection successful, but bucket '{self.bucket_name}' not found.")
                print(f"Available buckets: {bucket_names}")

            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidAccessKeyId':
                print("AWS connection failed: Invalid access key")
            elif error_code == 'SignatureDoesNotMatch':
                print("AWS connection failed: Invalid secret key")
            elif error_code == 'AccessDenied':
                print("AWS connection failed: Access denied (check permissions)")
            else:
                print(f"AWS connection failed: {e}")
            return False
        except Exception as e:
            print(f"AWS connection failed: {e}")
            return False

# Create global instance for use in other modules
aws_config = AWSConfig()

# Test function when run directly
if __name__ == "__main__":
    print("Testing AWS Configuration...")
    print("=" * 50)

    if aws_config.test_connection():
        print("\n✓ AWS configuration is working correctly")
    else:
        print("\n✗ AWS configuration test failed")
        print("\nTroubleshooting steps:")
        print("1. Check that .env file exists in docker/ directory")
        print("2. Verify AWS credentials in .env file")
        print("3. Ensure Docker Compose is loading the .env file")
        print("4. Check AWS permissions for the IAM user")