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
- Initial Load Job: `people-initial-load`
- CDC Processing Job: `people-cdc-processing` (requires `--cdc-path` parameter)



# export PYTHONPATH=$PYTHONPATH:/home/hadoop/workspace