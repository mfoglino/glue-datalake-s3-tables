# IAM Role for Glue Jobs
resource "aws_iam_role" "glue_role" {
  name = "datalake-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.account_id}:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_370d0a9b30d49146"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "lakeformation_data_admin" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"
}

resource "aws_iam_role_policy_attachment" "s3_tables_full_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3TablesFullAccess"
}

resource "aws_iam_role_policy_attachment" "s3_full_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy" "dynamodb_checkpoints_policy" {
  name = "dynamodb-checkpoints-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:DescribeTable"
        ]
        Resource = "arn:aws:dynamodb:us-east-1:*:table/etl_table_checkpoints"
      }
    ]
  })
}

# resource "aws_iam_role_policy" "s3tables_federation_policy" {
#   name = "s3tables-federation-access"
#   role = aws_iam_role.glue_role.id
#
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Sid    = "S3TableAccessViaGlueFederation"
#         Effect = "Allow"
#         Action = [
#           "glue:GetTable",
#           "glue:GetDatabase",
#           "glue:UpdateTable"
#         ]
#         Resource = [
#           "arn:aws:glue:${local.region}:${local.account_id}:catalog/s3tablescatalog/*",
#           "arn:aws:glue:${local.region}:${local.account_id}:catalog/s3tablescatalog",
#           "arn:aws:glue:${local.region}:${local.account_id}:catalog",
#           "arn:aws:glue:${local.region}:${local.account_id}:database/*",
#           "arn:aws:glue:${local.region}:${local.account_id}:table/*/*"
#         ]
#       },
#       {
#         Sid    = "RequiredWhenDoingMetadataReadsANDDataAndMetadataWriteViaLakeformation"
#         Effect = "Allow"
#         Action = [
#           "lakeformation:GetDataAccess"
#         ]
#         Resource = "*"
#       },
#       {
#         Sid    = "LoggingInCloudWatch"
#         Effect = "Allow"
#         Action = [
#           "logs:PutLogEvents"
#         ]
#         Resource = [
#           "arn:aws:logs:${local.region}:${local.account_id}:log-group:*"
#         ]
#       }
#     ]
#   })
# }