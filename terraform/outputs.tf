output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  value = aws_s3_bucket.data_lake.arn
}

output "airflow_role_arn" {
  value = aws_iam_role.airflow_role.arn
}

output "redshift_role_arn" {
  value = aws_iam_role.redshift_role.arn
}