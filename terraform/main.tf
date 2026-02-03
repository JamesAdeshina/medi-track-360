provider "aws" {
  region = "us-east-1"
}

# Simple S3 bucket for data lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "medi-track-360-data-lake"

  tags = {
    Name        = "MediTrack360 Data Lake"
    Environment = "development"
  }
}

# IAM role for Redshift
resource "aws_iam_role" "redshift_role" {
  name = "medi-track-360-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}