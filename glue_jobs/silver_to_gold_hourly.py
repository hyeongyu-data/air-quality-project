import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_PATH",
        "GOLD_PATH",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

silver = spark.read.parquet(args["SILVER_PATH"])

gold = (
    silver
    .withColumn("event_hour", F.date_trunc("hour", F.col("event_time")))
    .groupBy("event_hour", "sensor_id", "metric_name", "unit")
    .agg(
        F.avg("metric_value").alias("avg_value"),
        F.max("metric_value").alias("max_value"),
        F.min("metric_value").alias("min_value"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("log_level") == "ERROR", 1).otherwise(0)).alias("error_count"),
        F.sum(F.when(F.col("log_level") == "WARN", 1).otherwise(0)).alias("warning_count"),
        F.max("silver_processed_at").alias("last_silver_processed_at"),
    )
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("year", F.date_format("event_hour", "yyyy"))
    .withColumn("month", F.date_format("event_hour", "MM"))
    .withColumn("day", F.date_format("event_hour", "dd"))
    .withColumn("hour", F.date_format("event_hour", "HH"))
)

(
    gold.write
    .mode("append")
    .partitionBy("year", "month", "day", "hour")
    .parquet(args["GOLD_PATH"])
)
