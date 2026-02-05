"""
Delete Redshift Cluster for MediTrack360
This script deletes an existing Redshift cluster without taking a final snapshot.
"""

import boto3
from botocore.exceptions import ClientError
import os

# Configuration
CLUSTER_IDENTIFIER = "medii-track360-cluster"
REGION = os.getenv('AWS_DEFAULT_REGION', 'eu-north-1')

def delete_redshift_cluster():
    redshift = boto3.client("redshift", region_name=REGION)

    try:
        response = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)
        print(f"Cluster '{CLUSTER_IDENTIFIER}' exists. Deleting...")
    except redshift.exceptions.ClusterNotFoundFault:
        print(f"Cluster '{CLUSTER_IDENTIFIER}' does not exist. Nothing to delete.")
        return

    try:
        redshift.delete_cluster(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True  # Set False if you want a backup
        )
        print(f"Deletion initiated for cluster '{CLUSTER_IDENTIFIER}'.")
    except ClientError as e:
        print(f"Error deleting cluster: {e}")
        return

if __name__ == "__main__":
    delete_redshift_cluster()
