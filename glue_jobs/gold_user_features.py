import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    count,
    when,
    max as spark_max,
    current_date,
    row_number,
    to_date,
    datediff,
    lit,
)

# Init
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Paths
silver_path = "s3://ai-lakehouse-project/silver/user_events/"
gold_path = "s3://ai-lakehouse-project/gold/user_features/"

# Make overwrites partition-safe (only overwrites partitions present in the write DF)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Load Silver
silver_df = spark.read.format("parquet").load(silver_path)

# Silver should already be "current-state" (latest-wins per pk, deletes removed),
# but keep a defensive filter in case older files linger.
if "op" in silver_df.columns:
    silver_df = silver_df.filter(col("op") != lit("D"))

# --- 1) Last event per user (correctly aligned event_type with max timestamp) ---
w_last = Window.partitionBy("user_id").orderBy(col("event_timestamp").desc())

last_event_df = (
    silver_df
    .withColumn("rn", row_number().over(w_last))
    .filter(col("rn") == 1)
    .select(
        "user_id",
        col("event_timestamp").alias("last_event_timestamp"),
        col("event_type").alias("last_event_type"),
        col("feature_hash").alias("last_feature_hash"),
    )
)

# --- 2) Aggregate counts per user ---
counts_df = (
    silver_df
    .groupBy("user_id")
    .agg(
        count(when(col("event_type") == "click", True)).alias("click_count"),
        count(when(col("event_type") == "purchase", True)).alias("purchase_count"),
        spark_max(col("event_timestamp")).alias("max_event_timestamp"),
    )
)

# --- 3) Join + compute recency ---
gold_df = (
    counts_df
    .join(last_event_df, on="user_id", how="left")
    .withColumn("days_since_last_event", datediff(current_date(), to_date(col("last_event_timestamp"))))
    .drop("max_event_timestamp")
)

# Snapshot date for reproducible runs
gold_df = gold_df.withColumn("training_date", current_date())

# --- 4) Write Gold partitioned by training_date ---
# Rerunning same day replaces that day's partition only.
(
    gold_df
    .write
    .mode("overwrite")
    .partitionBy("training_date")
    .parquet(gold_path)
)

# Keep DB creation (harmless), but avoid auto table creation to prevent schema/partition issues.
spark.sql("CREATE DATABASE IF NOT EXISTS ai_lakehouse_db")

job.commit()