resource "aws_glue_catalog_database" "s3tablesmarcos" {
  name = "s3tablesmarcos"
}

locals {
  principals = {
    glue_role = aws_iam_role.glue_role.arn
    sso_user  = "arn:aws:iam::131578276461:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_370d0a9b30d49146"
  }
}

resource "aws_lakeformation_permissions" "database_access" {
  for_each = local.principals

  principal = each.value

  database {
    name = aws_glue_catalog_database.s3tablesmarcos.name
  }

  permissions = ["ALL"]
  depends_on = [aws_glue_catalog_database.s3tablesmarcos]
}

resource "aws_lakeformation_permissions" "all_tables_access" {
  for_each = local.principals

  principal = each.value

  table {
    database_name = aws_glue_catalog_database.s3tablesmarcos.name
    wildcard      = true
  }

  permissions = ["ALL"]
  depends_on = [aws_glue_catalog_database.s3tablesmarcos]
}