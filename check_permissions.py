import boto3
import json

# Create IAM client
iam = boto3.client('iam')

# Get current user
user = iam.get_user()['User']['UserName']
print(f"Current IAM user: {user}\n")

# List attached managed policies
attached_policies = iam.list_attached_user_policies(UserName=user)['AttachedPolicies']
print("Attached Managed Policies:")
for p in attached_policies:
    print(f"  - {p['PolicyName']} ({p['PolicyArn']})")

# List inline policies
inline_policies = iam.list_user_policies(UserName=user)['PolicyNames']
print("\nInline Policies:")
for p in inline_policies:
    policy = iam.get_user_policy(UserName=user, PolicyName=p)
    print(f"  - {p}: {json.dumps(policy['PolicyDocument'], indent=2)}")

# Optional: simulate some actions
sim = boto3.client('iam')
actions_to_test = [
    'iam:CreateRole',
    'redshift:CreateCluster',
    's3:ListBucket'
]

for action in actions_to_test:
    response = sim.simulate_principal_policy(
        PolicySourceArn=user,
        ActionNames=[action]
    )
    allowed = response['EvaluationResults'][0]['EvalDecision']
    print(f"\nPermission check for {action}: {allowed}")
