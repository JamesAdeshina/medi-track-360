variable "aws_region" {
  description = "AWS region"
  default     = "eu-north-1"  # Changed to Stockholm
}

variable "project_name" {
  description = "Project name for resource naming"
  default     = "medi-track-360"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for data lake"
  default     = "medi-track-360-data-lake"
}

variable "redshift_cluster_identifier" {
  description = "Existing Redshift cluster identifier"
  default     = "redshift-cluster-1"
}