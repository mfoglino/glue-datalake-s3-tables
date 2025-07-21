variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "datalake_bucket" {
  description = "S3 bucket for datalake"
  type        = string
  default     = "datalake-example"
}

variable "s3tables_bucket" {
  description = "S3 bucket for S3 Tables"
  type        = string
  default     = "datalake-example-s3tables"
}