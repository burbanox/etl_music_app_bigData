import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
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


# Glue Studio Visual equivalent: Source customer -> ApplyMapping -> S3 Parquet.
customers = read_table("customer")
dim_customer = ApplyMapping.apply(
    frame=DynamicFrame.fromDF(customers, glue_context, "customer_source"),
    mappings=[
        ("customer_id", "int", "CustomerKey", "int"),
        ("first_name", "string", "FirstName", "string"),
        ("last_name", "string", "LastName", "string"),
        ("company", "string", "Company", "string"),
        ("country", "string", "Country", "string"),
        ("city", "string", "City", "string"),
        ("state", "string", "State", "string"),
        ("email", "string", "Email", "string"),
    ],
    transformation_ctx="map_dim_customer",
)
glue_context.write_dynamic_frame.from_options(
    frame=dim_customer,
    connection_type="s3",
    connection_options={"path": f"s3://{target_bucket}/{curated_prefix}/dim_customer"},
    format="glueparquet",
    transformation_ctx="write_dim_customer",
)

# Glue Studio Visual equivalent: Source track/album/artist/genre/media_type -> Join -> ApplyMapping -> S3 Parquet.
tracks = read_table("track")
albums = read_table("album").select(F.col("album_id"), F.col("title").alias("Album"), F.col("artist_id"))
artists = read_table("artist").select(F.col("artist_id"), F.col("name").alias("Artist"))
genres = read_table("genre").select(F.col("genre_id"), F.col("name").alias("Genre"))
media_types = read_table("media_type").select(F.col("media_type_id"), F.col("name").alias("MediaType"))

dim_track_df = (
    tracks.join(albums, "album_id", "left")
    .join(artists, "artist_id", "left")
    .join(genres, "genre_id", "left")
    .join(media_types, "media_type_id", "left")
    .select(
        F.col("track_id").alias("TrackKey"),
        F.col("name").alias("Name"),
        "Album",
        "Artist",
        "Genre",
        "MediaType",
        F.col("composer").alias("Composer"),
        F.col("milliseconds").alias("Milliseconds"),
    )
)
glue_context.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(dim_track_df, glue_context, "dim_track"),
    connection_type="s3",
    connection_options={"path": f"s3://{target_bucket}/{curated_prefix}/dim_track"},
    format="glueparquet",
    transformation_ctx="write_dim_track",
)

job.commit()
