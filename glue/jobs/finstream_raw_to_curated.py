import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --------------------------------------------------
# Configuration
# --------------------------------------------------

RAW_TRANSACTIONS = (
    "s3://finstream-banking-datalake-shiva-2026/"
    "raw/transactions/"
)

RAW_CUSTOMERS = (
    "s3://finstream-banking-datalake-shiva-2026/"
    "raw/customers/"
)

CURATED_PATH = (
    "s3://finstream-banking-datalake-shiva-2026/"
    "curated/transactions/"
)

QUARANTINE_PATH = (
    "s3://finstream-banking-datalake-shiva-2026/"
    "quarantine/invalid_records/"
)


# --------------------------------------------------
# Spark / Glue initialization
# --------------------------------------------------

args = {}

for i, arg in enumerate(sys.argv):
    if arg == "--JOB_NAME" and i + 1 < len(sys.argv):
        args["JOB_NAME"] = sys.argv[i + 1]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)

if "JOB_NAME" in args:
    job.init(args["JOB_NAME"], args)


# --------------------------------------------------
# Read Raw Transactions
# --------------------------------------------------

print("Reading transactions...")

transactions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_TRANSACTIONS)
)

print(f"Transaction records: {transactions.count()}")

transactions.printSchema()


# --------------------------------------------------
# Read Raw Customers
# --------------------------------------------------

print("Reading customers...")

customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_CUSTOMERS)
)

print(f"Customer records before cleaning: {customers.count()}")

customers.printSchema()


# --------------------------------------------------
# Clean Customer Data
# --------------------------------------------------

# Remove accidental header row if it appears as data
customers = customers.filter(
    F.col("customer_id") != "customer_id"
)

# Remove records with missing customer ID
invalid_customers = customers.filter(
    F.col("customer_id").isNull()
)

valid_customers = customers.filter(
    F.col("customer_id").isNotNull()
)


# --------------------------------------------------
# Clean Transaction Data
# --------------------------------------------------

transactions = transactions.filter(
    F.col("transaction_id") != "transaction_id"
)

transactions = transactions.withColumn(
    "amount",
    F.col("amount").cast(DoubleType())
)

transactions = transactions.withColumn(
    "transaction_date",
    F.to_timestamp(
        "transaction_date",
        "yyyy-MM-dd HH:mm:ss"
    )
)


# --------------------------------------------------
# Data Quality Rules
# --------------------------------------------------

invalid_transactions = transactions.filter(
    F.col("transaction_id").isNull()
    | F.col("customer_id").isNull()
    | F.col("amount").isNull()
    | (F.col("amount") <= 0)
    | F.col("transaction_date").isNull()
)

valid_transactions = transactions.filter(
    F.col("transaction_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("amount").isNotNull()
    & (F.col("amount") > 0)
    & F.col("transaction_date").isNotNull()
)


# --------------------------------------------------
# Join Transactions + Customers
# --------------------------------------------------

print("Joining transactions with customers...")

curated = (
    valid_transactions.alias("t")
    .join(
        valid_customers.alias("c"),
        F.col("t.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .select(
        F.col("t.transaction_id"),
        F.col("t.customer_id"),
        F.col("c.customer_name"),
        F.col("c.account_type"),
        F.col("t.transaction_date"),
        F.col("t.amount"),
        F.col("t.transaction_type"),
        F.col("t.merchant"),
        F.col("t.city"),
        F.col("t.status")
    )
)


# --------------------------------------------------
# Add partition columns
# --------------------------------------------------

curated = (
    curated
    .withColumn(
        "transaction_date_only",
        F.to_date("transaction_date")
    )
    .withColumn(
        "year",
        F.year("transaction_date")
    )
    .withColumn(
        "month",
        F.month("transaction_date")
    )
)


# --------------------------------------------------
# Write Curated Data
# --------------------------------------------------

print("Writing curated Parquet data...")

(
    curated.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy("year", "month")
    .save(CURATED_PATH)
)


# --------------------------------------------------
# Write Invalid Records
# --------------------------------------------------

invalid_count = invalid_transactions.count()

if invalid_count > 0:

    print(
        f"Writing {invalid_count} invalid "
        "transaction records to quarantine..."
    )

    (
        invalid_transactions.write
        .mode("append")
        .format("parquet")
        .option("compression", "snappy")
        .save(QUARANTINE_PATH)
    )


# --------------------------------------------------
# Summary
# --------------------------------------------------

print("==============================================")
print("FinStream ETL Completed")
print("==============================================")

print(
    f"Valid transactions : {valid_transactions.count()}"
)

print(
    f"Invalid transactions : {invalid_count}"
)

print(
    f"Curated records : {curated.count()}"
)

print(
    f"Curated path : {CURATED_PATH}"
)

print("==============================================")


job.commit()