import boto3
from botocore.exceptions import ClientError


def list_redshift_clusters():
    # Create Redshift client
    client = boto3.client('redshift')

    try:
        response = client.describe_clusters()
        clusters = response.get('Clusters', [])

        if not clusters:
            print("No Redshift clusters found.")
            return

        print(f"Found {len(clusters)} cluster(s):\n")
        for cluster in clusters:
            print(f"Cluster Identifier: {cluster['ClusterIdentifier']}")
            print(f"Database Name: {cluster['DBName']}")
            print(f"Endpoint: {cluster.get('Endpoint', {}).get('Address', 'N/A')}")
            print(f"Port: {cluster.get('Endpoint', {}).get('Port', 'N/A')}")
            print(f"Cluster Status: {cluster['ClusterStatus']}")
            print("-" * 40)

    except ClientError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    list_redshift_clusters()
