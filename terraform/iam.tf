# # IAM Role for Redshift to access S3
# resource "aws_iam_role" "redshift_role" {
#   name = "meditrack360-redshift-role"
#
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Action = "sts:AssumeRole"
#         Effect = "Allow"
#         Principal = {
#           Service = "redshift.amazonaws.com"
#         }
#       }
#     ]
#   })
#
#   tags = {
#     Name = "meditrack360-redshift-role"
#   }
# }
#
# # Attach S3 read policy to Redshift role
# resource "aws_iam_role_policy_attachment" "redshift_s3_read" {
#   role       = aws_iam_role.redshift_role.name
#   policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
# }
