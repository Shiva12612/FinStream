# FinStream

## Real-Time Banking Data Engineering Platform

FinStream is an end-to-end batch and real-time data engineering platform designed to process banking transaction data.

### Technologies

- AWS S3
- AWS Glue
- Glue Data Catalog
- Glue Crawlers
- PySpark
- Amazon Athena
- Amazon Aurora PostgreSQL
- Apache Kafka
- Apache Airflow
- AWS Lambda
- Amazon DynamoDB
- Amazon SNS
- Amazon CloudWatch
- Boto3
- Git & GitHub
- GitHub Actions
- Docker

### Architecture

The platform supports both batch and real-time transaction ingestion with automated ETL, data quality validation, orchestration, monitoring, auditing and CI/CD.

### Data Flow

Aurora / Kafka
→ S3 Raw
→ Glue Crawler
→ Glue Data Catalog
→ PySpark Transformation
→ S3 Curated
→ S3 Aggregated
→ Athena / DynamoDB

Airflow is used for orchestration and SNS is used for pipeline notifications.
