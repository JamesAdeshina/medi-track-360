# ========== AIRFLOW IAM ROLE ==========

resource "aws_iam_role" "airflow_role" {
  name = "${var.project_name}-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

# S3 Access Policy
resource "aws_iam_policy" "airflow_s3_policy" {
  name = "${var.project_name}-airflow-s3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*"
      ]
    }]
  })
}

# Glue Access Policy
resource "aws_iam_policy" "airflow_glue_policy" {
  name = "${var.project_name}-airflow-glue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:*"
      ]
      Resource = "*"
    }]
  })
}

# Athena Access Policy
resource "aws_iam_policy" "airflow_athena_policy" {
  name = "${var.project_name}-airflow-athena"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "athena:*"
      ]
      Resource = "*"
    }]
  })
}

# Attach policies
resource "aws_iam_role_policy_attachment" "airflow_s3" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_glue" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_glue_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_athena" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_athena_policy.arn
}

# ========== REDSHIFT IAM ROLE ==========

resource "aws_iam_role" "redshift_role" {
  name = "${var.project_name}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "redshift.amazonaws.com"
      }
    }]
  })
}

# Redshift S3 Access
resource "aws_iam_policy" "redshift_s3_policy" {
  name = "${var.project_name}-redshift-s3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = aws_iam_policy.redshift_s3_policy.arn
}