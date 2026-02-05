"""
Create Redshift Cluster for MediTrack360 (Simplified)
This script creates a Redshift cluster, waits for it to become available,
and prints its endpoint and connection info using dc2.large nodes.
"""

import boto3
from botocore.exceptions import ClientError
import time
import os

# ---------------- Configuration ----------------
CLUSTER_IDENTIFIER = "medi-ttrack360-cluster"
DATABASE_NAME = "testdb"
MASTER_USERNAME = "admin"
MASTER_PASSWORD = "MediTrack360!2024"
NODE_TYPE = "ra3.large"  # simplified for testing, works without subnet group
CLUSTER_TYPE = "single-node"
REGION = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
RETRY_DELAY = 30  # seconds between status checks
TIMEOUT_MINUTES = 30  # max wait for cluster to become available

# Optional: IAM role ARN (if needed for S3 COPY)
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN", None)


def create_redshift_cluster():
    redshift = boto3.client("redshift", region_name=REGION)

    # Check if cluster already exists
    try:
        response = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)
        cluster = response["Clusters"][0]
        print(f"Cluster already exists: {cluster['ClusterIdentifier']}")
        return cluster
    except redshift.exceptions.ClusterNotFoundFault:
        print(f"Cluster '{CLUSTER_IDENTIFIER}' does not exist. Creating...")

    # Cluster creation parameters
    params = dict(
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        NodeType=NODE_TYPE,
        MasterUsername=MASTER_USERNAME,
        MasterUserPassword=MASTER_PASSWORD,
        DBName=DATABASE_NAME,
        ClusterType=CLUSTER_TYPE,
        PubliclyAccessible=True,
        NumberOfNodes=1,
        AutomatedSnapshotRetentionPeriod=1,
    )

    if IAM_ROLE_ARN:
        params["IamRoles"] = [IAM_ROLE_ARN]

    try:
        response = redshift.create_cluster(**params)
        cluster = response["Cluster"]
        print(f"Cluster creation initiated: {cluster['ClusterIdentifier']}")
        return cluster
    except ClientError as e:
        print(f"Error creating cluster: {e}")
        return None


def wait_for_cluster_available(redshift_client, cluster_id):
    print(f"Waiting for cluster '{cluster_id}' to become available...")
    start_time = time.time()
    timeout_seconds = TIMEOUT_MINUTES * 60

    while time.time() - start_time < timeout_seconds:
        try:
            response = redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
            cluster = response["Clusters"][0]
            status = cluster["ClusterStatus"]
            print(f"  Status: {status}")

            if status == "available":
                endpoint = cluster["Endpoint"]["Address"]
                port = cluster["Endpoint"]["Port"]
                print("\nCluster is now available!")
                print(f"Endpoint: {endpoint}")
                print(f"Port: {port}")
                print(f"Database: {cluster['DBName']}")
                print(f"Connection string: postgresql://{MASTER_USERNAME}:{MASTER_PASSWORD}@{endpoint}:{port}/{DATABASE_NAME}")
                return cluster

            time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"Error checking cluster status: {e}")
            time.sleep(RETRY_DELAY)

    print("Timeout waiting for cluster to become available.")
    return None


def main():
    redshift_client = boto3.client("redshift", region_name=REGION)
    cluster = create_redshift_cluster()
    if cluster:
        wait_for_cluster_available(redshift_client, CLUSTER_IDENTIFIER)


if __name__ == "__main__":
    main()
