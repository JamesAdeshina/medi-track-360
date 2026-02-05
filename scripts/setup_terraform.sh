#!/bin/bash
# Setup and run Terraform for Redshift cluster

set -e  # Exit on error

echo "Setting up Terraform for MediTrack360 Redshift cluster..."
echo "========================================================="

cd terraform

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "Error: AWS credentials not configured."
    echo "Please run: aws configure"
    echo "Or set environment variables:"
    echo "  export AWS_ACCESS_KEY_ID=your_key"
    echo "  export AWS_SECRET_ACCESS_KEY=your_secret"
    exit 1
fi

# Initialize Terraform
echo "Initializing Terraform..."
terraform init

# Plan the infrastructure
echo "Planning infrastructure..."
terraform plan

# Ask for confirmation
read -p "Do you want to apply this Terraform plan? (yes/no): " -r
echo
if [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Applying Terraform configuration..."
    terraform apply -auto-approve

    echo ""
    echo " Redshift cluster created successfully!"
    echo ""

    # Show outputs
    echo "Connection details:"
    terraform output redshift_cluster_details

    echo ""
    echo "To connect to Redshift:"
    echo "  Host: $(terraform output -raw redshift_endpoint | cut -d: -f1)"
    echo "  Port: 5439"
    echo "  Database: meditrackdb"
    echo "  Username: admin"
    echo "  Password: [Your chosen password]"

else
    echo "Terraform apply cancelled."
fi