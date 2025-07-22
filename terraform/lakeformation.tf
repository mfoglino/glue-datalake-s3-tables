# resource "aws_lakeformation_resource" "s3tables_data_location" {
#   #arn = "arn:aws:s3tables:${local.region}:${local.account_id}:bucket/${aws_s3tables_table_bucket.table_bucket.name}"
#   arn = "arn:aws:s3tables:us-east-1:131578276461:bucket/datalake-example-s3tables"
# }
#
# resource "aws_lakeformation_permissions" "glue_data_location_access" {
#   principal = aws_iam_role.glue_role.arn
#
#   data_location {
#     arn = aws_lakeformation_resource.s3tables_data_location.arn
#   }
#
#   permissions = ["DATA_LOCATION_ACCESS"]
# }
#
# resource "aws_lakeformation_permissions" "glue_table_access" {
#   principal = aws_iam_role.glue_role.arn
#
#   table {
#     catalog_id    = local.account_id
#     database_name = "s3tables"
#     name          = "people"
#   }
#
#   permissions = ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"]
# }