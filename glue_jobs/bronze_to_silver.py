import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_PATH",
        "SILVER_PATH",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


def string_col(name: str):
    if name in df.columns:
        return F.col(name).cast(StringType())
    return F.lit(None).cast(StringType())


df = spark.read.json(args["BRONZE_PATH"])

silver = (
    df.select(
        F.to_timestamp(string_col("event_time")).alias("event_time"),
        string_col("sensor_id").alias("sensor_id"),
        string_col("source_type").alias("source_type"),
        F.upper(string_col("log_level")).alias("log_level"),
        string_col("metric_name").alias("metric_name"),
        F.col("metric_value").cast(DoubleType()).alias("metric_value"),
        string_col("unit").alias("unit"),
        string_col("status").alias("status"),
        string_col("message").alias("message"),
        F.current_timestamp().alias("silver_processed_at"),
    )
    .filter(F.col("event_time").isNotNull())
    .filter(F.col("sensor_id").isNotNull())
    .filter(F.col("metric_name").isNotNull())
    .withColumn("year", F.date_format("event_time", "yyyy"))
    .withColumn("month", F.date_format("event_time", "MM"))
    .withColumn("day", F.date_format("event_time", "dd"))
    .withColumn("hour", F.date_format("event_time", "HH"))
    .dropDuplicates(["event_time", "sensor_id", "metric_name", "metric_value", "message"])
)

(
    silver.write
    .mode("append")
    .partitionBy("year", "month", "day", "hour")
    .parquet(args["SILVER_PATH"])
)
