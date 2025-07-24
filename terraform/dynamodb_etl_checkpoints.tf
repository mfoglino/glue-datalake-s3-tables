resource "aws_dynamodb_table" "etl_table_checkpoints" {
  name           = "etl_table_checkpoints"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "table_name"

  attribute {
    name = "table_name"
    type = "S"
  }

  tags = {
    Name = "etl_table_checkpoints"
  }
}