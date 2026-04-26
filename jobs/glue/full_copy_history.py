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
snapshot_date = F.current_date()


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


customer_history = read_table("customer").withColumn("snapshot_date", snapshot_date)
employee = read_table("employee")
employee_history = (
    employee.alias("e")
    .join(employee.alias("m"), F.col("e.reports_to") == F.col("m.employee_id"), "left")
    .select(
        F.col("e.employee_id").alias("EmployeeKey"),
        F.col("e.first_name").alias("FirstName"),
        F.col("e.last_name").alias("LastName"),
        F.col("e.title").alias("Title"),
        F.col("e.reports_to").alias("ReportsToEmployeeKey"),
        F.concat_ws(" ", F.col("m.first_name"), F.col("m.last_name")).alias("ReportsToName"),
        snapshot_date.alias("snapshot_date"),
    )
)

for name, frame in {
    "customer_history": customer_history,
    "employee_reports_history": employee_history,
}.items():
    glue_context.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(frame, glue_context, name),
        connection_type="s3",
        connection_options={
            "path": f"s3://{target_bucket}/{curated_prefix}/{name}",
            "partitionKeys": ["snapshot_date"],
        },
        format="glueparquet",
        transformation_ctx=f"write_{name}",
    )

job.commit()
