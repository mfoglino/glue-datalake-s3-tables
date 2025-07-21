terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 Bucket for S3 Tables
resource "aws_s3_bucket" "table_bucket" {
  bucket = var.s3tables_bucket
}

# S3 Tables Namespace
resource "aws_s3tables_namespace" "datalake" {
  namespace        = "s3tables"
  table_bucket_arn = aws_s3_bucket.table_bucket.arn
}

# S3 Tables Table for people data
resource "aws_s3tables_table" "people" {
  namespace        = aws_s3tables_namespace.datalake.namespace
  name             = "people"
  format           = "ICEBERG"
  table_bucket_arn = aws_s3_bucket.table_bucket.arn
}

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
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "datalake-glue-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.datalake_bucket}/*",
          "arn:aws:s3:::${var.datalake_bucket}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3tables:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue Job for Initial Load
resource "aws_glue_job" "initial_load" {
  name     = "people-initial-load"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${var.datalake_bucket}/scripts/initial_load.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--enable-metrics"      = ""
    "--enable-observability-metrics" = "true"
    "--source-bucket"       = var.datalake_bucket
    "--table-namespace"     = "s3tables"
    "--table-name"         = "people"
    "--warehouse-path"      = "s3://${var.datalake_bucket}/warehouse"
    "--conf"               = <<EOT
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.defaultCatalog=s3tablesbucket
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog
--conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:${local.region}:${local.account_id}:bucket/${aws_s3_bucket.table_bucket.id}
EOT
  }

  glue_version = "5.0"
}

# Glue Job for CDC Processing
resource "aws_glue_job" "cdc_processing" {
  name     = "people-cdc-processing"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${var.datalake_bucket}/scripts/cdc_processing.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--enable-metrics"      = ""
    "--enable-observability-metrics" = "true"
    "--source-bucket"       = var.datalake_bucket
    "--table-name"         = "people"
    "--warehouse-path"      = "s3://${var.datalake_bucket}/warehouse"
    "--conf"               = <<EOT
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.defaultCatalog=s3tablesbucket
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog
--conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:${local.region}:${local.account_id}:bucket/${aws_s3_bucket.table_bucket.id}
EOT
  }

  glue_version = "5.0"
}