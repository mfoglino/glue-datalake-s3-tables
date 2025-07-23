# Datalake Solution Pipeline

## Architecture
- **Landing Zone**: s3://datalake-example/landing
- **Raw Layer**: S3 Tables with namespace 's3tablesmarcos'
- **Processing**: AWS Glue Jobs

## Data Flow
1. Initial Load: `LOAD00000001.parquet` → S3 Tables
2. CDC: `cdc-*.parquet` → S3 Tables (merge operations)

## Deployment
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

## Usage
- Populate S3 with: `test_data_generation.py`
- Initial Load Job: `people-initial-load`
- CDC Processing Job: `people-cdc-processing`


## Important note:
`When creating tables, make sure that you use all lowercase letters in your table names and table definitions. For example, make sure that your column names are all lowercase. If your table name or table definition contains capital letters, the table isn't supported by AWS Lake Formation or the AWS Glue Data Catalog. In this case, your table won't be visible to AWS analytics services such as Amazon Athena, even if your table buckets are integrated with AWS analytics services.`

Even silent errors can occur:
For example the written table appears in the S3 Table Console, but it doesn't appear in Athena.

Sources:
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html#table-integration-procedures
- https://repost.aws/pt/questions/QUB87eOCjxTAmiomX7U525pw/s3-bucket-data-migration-to-s3-table?sc_ichannel=ha&sc_ilang=en&sc_isite=repost&sc_iplace=hp&sc_icontent=QUB87eOCjxTAmiomX7U525pw&sc_ipos=4
