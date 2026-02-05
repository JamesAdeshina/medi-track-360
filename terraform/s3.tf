# # Data Lake S3 Bucket
# resource "aws_s3_bucket" "data_lake" {
#   bucket = var.project_name
# }
#
# # Enable versioning
# resource "aws_s3_bucket_versioning" "data_lake_versioning" {
#   bucket = aws_s3_bucket.data_lake.id
#
#   versioning_configuration {
#     status = "Enabled"
#   }
# }
#
# # Encryption
# resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
#   bucket = aws_s3_bucket.data_lake.id
#
#   rule {
#     apply_server_side_encryption_by_default {
#       sse_algorithm = "AES256"
#     }
#   }
# }
#
# # Block public access
# resource "aws_s3_bucket_public_access_block" "data_lake_block" {
#   bucket = aws_s3_bucket.data_lake.id
#
#   block_public_acls       = true
#   block_public_policy     = true
#   ignore_public_acls      = true
#   restrict_public_buckets = true
# }