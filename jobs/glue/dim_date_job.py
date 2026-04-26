import sys
from datetime import date, datetime

import holidays
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "TARGET_BUCKET", "CURATED_PREFIX", "START_DATE", "END_DATE"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_key(value):
    return int(value.strftime("%Y%m%d"))


start_date = parse_date(args["START_DATE"])
end_date = parse_date(args["END_DATE"])
co_holidays = holidays.country_holidays("CO", years=range(start_date.year, end_date.year + 1))

rows = []
current = start_date
while current <= end_date:
    rows.append(
        Row(
            DateKey=date_key(current),
            FullDate=current.isoformat(),
            Year=current.year,
            Quarter=((current.month - 1) // 3) + 1,
            Month=current.month,
            Day=current.day,
            DayOfWeek=current.strftime("%A"),
            IsHoliday=current in co_holidays,
            partition_year=current.year,
            partition_month=current.month,
            partition_day=current.day,
        )
    )
    current = date.fromordinal(current.toordinal() + 1)

target_bucket = args["TARGET_BUCKET"]
curated_prefix = args["CURATED_PREFIX"].rstrip("/")
(
    spark.createDataFrame(rows)
    .write.mode("overwrite")
    .partitionBy("partition_year", "partition_month", "partition_day")
    .parquet(f"s3://{target_bucket}/{curated_prefix}/dim_date")
)

job.commit()
