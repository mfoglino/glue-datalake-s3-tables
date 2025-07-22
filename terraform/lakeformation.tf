resource "aws_glue_catalog_database" "s3tablesmarcos" {
  name = "s3tablesmarcos"
}

resource "aws_lakeformation_permissions" "glue_database_access" {
  principal = aws_iam_role.glue_role.arn

  database {
    name = aws_glue_catalog_database.s3tablesmarcos.name
  }

  permissions = ["DESCRIBE"]

  depends_on = [aws_glue_catalog_database.s3tablesmarcos]
}

resource "aws_lakeformation_permissions" "glue_all_tables_access" {
  principal = aws_iam_role.glue_role.arn

  table {
    database_name = aws_glue_catalog_database.s3tablesmarcos.name
    wildcard      = true
  }

  permissions = ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"]

  depends_on = [aws_glue_catalog_database.s3tablesmarcos]
}