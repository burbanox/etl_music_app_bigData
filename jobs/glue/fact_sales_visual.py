import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_CONNECTION_NAME", "TARGET_BUCKET", "CURATED_PREFIX"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

connection_name = args["SOURCE_CONNECTION_NAME"]
target_bucket = args["TARGET_BUCKET"]
curated_prefix = args["CURATED_PREFIX"].rstrip("/")


def read_table(table_name):
    return glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": connection_name,
            "dbtable": table_name,
        },
        transformation_ctx=f"read_{table_name}",
    ).toDF()


# Glue Studio Visual equivalent:
# Source invoice + invoice_line + customer -> Join -> Derived columns -> S3 Parquet partitioned by year/month/day.
invoice = read_table("invoice")
invoice_line = read_table("invoice_line")
customer = read_table("customer").select("customer_id", "support_rep_id")

fact_sales = (
    invoice_line.join(invoice, "invoice_id", "inner")
    .join(customer, "customer_id", "left")
    .withColumn("invoice_date_only", F.to_date("invoice_date"))
    .withColumn("InvoiceDateKey", F.date_format("invoice_date_only", "yyyyMMdd").cast("int"))
    .withColumn("TotalAmount", (F.col("unit_price") * F.col("quantity")).cast("decimal(10,2)"))
    .withColumn("partition_year", F.year("invoice_date_only"))
    .withColumn("partition_month", F.month("invoice_date_only"))
    .withColumn("partition_day", F.dayofmonth("invoice_date_only"))
    .select(
        F.col("customer_id").alias("CustomerKey"),
        F.col("track_id").alias("TrackKey"),
        "InvoiceDateKey",
        F.col("support_rep_id").alias("EmployeeKey"),
        F.col("quantity").alias("Quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("UnitPrice"),
        "TotalAmount",
        "partition_year",
        "partition_month",
        "partition_day",
    )
)

glue_context.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(fact_sales, glue_context, "fact_sales"),
    connection_type="s3",
    connection_options={
        "path": f"s3://{target_bucket}/{curated_prefix}/fact_sales",
        "partitionKeys": ["partition_year", "partition_month", "partition_day"],
    },
    format="glueparquet",
    transformation_ctx="write_fact_sales",
)

job.commit()
