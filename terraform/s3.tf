# S3 Bucket for DMS Landing Zone
resource "aws_s3_bucket" "dms_landing" {
  bucket = "${var.datalake_bucket}-landing"
}