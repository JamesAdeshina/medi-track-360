import boto3
from botocore.exceptions import ClientError

client = boto3.client("redshift", region_name="eu-north-1")

try:
    response = client.create_cluster(
        ClusterIdentifier="medi-track360-cluster",
        NodeType="ra3.large",  # updated node type
        MasterUsername="admin",
        MasterUserPassword="MediTrack360!2024!",  # change this!
        DBName="testdb",
        ClusterType="single-node",
        PubliclyAccessible=True
    )
    print("Cluster creation initiated:")
    print("Cluster Identifier:", response['Cluster']['ClusterIdentifier'])
    print("Status:", response['Cluster']['ClusterStatus'])
except ClientError as e:
    print("Error occurred:")
    print(e)
