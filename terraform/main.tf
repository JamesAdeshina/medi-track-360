provider "aws" {
  region = "eu-north-1"  # Same region as your S3 bucket

  default_tags {
    tags = {
      Project     = "meditrack360-data-lake"
      Environment = "dev"
      ManagedBy   = "Terraform"
    }
  }
}

# Create IAM Role from scratch (you have permission to CREATE roles)
resource "aws_iam_role" "redshift_role" {
  name = "meditrack360-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })

  tags = {
    Name = "meditrack360-redshift-role"
  }
}

# Attach S3 read policy to the role we just created
resource "aws_iam_role_policy_attachment" "redshift_s3_read" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# Create Redshift Cluster with the IAM role
resource "aws_redshift_cluster" "meditrack360_cluster" {
  cluster_identifier = "meditrack360-cluster"
  database_name      = "meditrackdb"
  master_username    = var.redshift_master_username
  master_password    = var.redshift_master_password

  node_type          = "dc2.large"
  cluster_type       = "single-node"
  publicly_accessible = true
  skip_final_snapshot = true

  # Use default VPC security settings (no custom security group needed)
  # IAM role for S3 access
  iam_roles = [aws_iam_role.redshift_role.arn]

  tags = {
    Name = "meditrack360-redshift-cluster"
  }
}

# Output the connection details
output "redshift_endpoint" {
  value = aws_redshift_cluster.meditrack360_cluster.endpoint
}

output "redshift_connection_string" {
  value = "postgresql://${var.redshift_master_username}:${var.redshift_master_password}@${aws_redshift_cluster.meditrack360_cluster.endpoint}/meditrackdb"
  sensitive = true
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift_role.arn
}

output "connection_instructions" {
  value = <<-EOT
    ✅ Redshift cluster created successfully!

    Connection details:
    Host: ${aws_redshift_cluster.meditrack360_cluster.endpoint}
    Port: 5439
    Database: meditrackdb
    Username: ${var.redshift_master_username}
    Password: [your password from terraform.tfvars]

    IAM Role for S3 access: ${aws_iam_role.redshift_role.arn}

    To test connection:
    psql -h ${aws_redshift_cluster.meditrack360_cluster.endpoint} -p 5439 -U ${var.redshift_master_username} -d meditrackdb

    Wait 10-15 minutes for cluster to be fully ready.
  EOT
}