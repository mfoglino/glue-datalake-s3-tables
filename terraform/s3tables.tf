# S3 Tables Bucket
resource "aws_s3tables_table_bucket" "table_bucket" {
  name = var.s3tables_bucket
}

# S3 Tables Namespace
resource "aws_s3tables_namespace" "datalake" {
  namespace        = "s3tablesmarcos"
  table_bucket_arn = aws_s3tables_table_bucket.table_bucket.arn
}

# # S3 Tables Table for people data
# resource "aws_s3tables_table" "people" {
#   namespace        = aws_s3tables_namespace.datalake.namespace
#   name             = "people"
#   format           = "ICEBERG"
#   table_bucket_arn = aws_s3tables_table_bucket.table_bucket.arn
# }