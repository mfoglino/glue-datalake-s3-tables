# S3 Bucket for Glue Scripts
resource "aws_s3_bucket" "scripts" {
  bucket = "${var.datalake_bucket}-scripts"
}

# Upload Initial Load Script
resource "aws_s3_object" "initial_load_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/initial_load.py"
  source = "../initial_load.py"
  etag   = filemd5("../initial_load.py")
}

# Upload CDC Processing Script
resource "aws_s3_object" "cdc_processing_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue_jobs/cdc_processing.py"
  source = "../cdc_processing.py"
  etag   = filemd5("../cdc_processing.py")
}

# Glue Job for Initial Load

resource "aws_glue_job" "initial_load" {
  name     = "people-initial-load"
  role_arn = aws_iam_role.glue_role.arn
  depends_on = [aws_s3_object.initial_load_script]

  command {
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue_jobs/initial_load.py"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-jars"                       = "s3://${aws_s3_bucket.scripts.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--job-language"        = "python"
    "--enable-metrics"      = ""
    "--enable-observability-metrics" = "true"
    "--source_bucket"       = aws_s3_bucket.dms_landing.id
    "--table_namespace"     = "s3tablesmarcos"
    "--table_name"         = "people"
#     "--conf"               = <<EOT
# spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# --conf spark.sql.defaultCatalog=s3tablesbucket
# --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog
# --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog
# --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:${local.region}:${local.account_id}:bucket/${aws_s3tables_table_bucket.table_bucket.name}
# EOT
  }

  glue_version = "5.0"
}

# Glue Job for CDC Processing
resource "aws_glue_job" "cdc_processing" {
  name     = "people-cdc-processing"
  role_arn = aws_iam_role.glue_role.arn
  depends_on = [aws_s3_object.cdc_processing_script]

  command {
    script_location = "s3://${aws_s3_bucket.scripts.id}/glue_jobs/cdc_processing.py"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-jars"                       = "s3://${aws_s3_bucket.scripts.id}/s3_tables_jars/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--job-language"        = "python"
    "--enable-metrics"      = ""
    "--enable-observability-metrics" = "true"
    "--source_bucket"       = aws_s3_bucket.dms_landing.id
    "--table_name"         = "people"
#     "--conf"               = <<EOT
# spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# --conf spark.sql.defaultCatalog=s3tablesbucket
# --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog
# --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog
# --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:${local.region}:${local.account_id}:bucket/${aws_s3tables_table_bucket.table_bucket.name}
# EOT
  }

  glue_version = "5.0"
}