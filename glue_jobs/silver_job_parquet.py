import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, row_number, to_timestamp
from awsglue.context import GlueContext
from awsglue.job import Job

# Init
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Paths
bronze_path = "s3://ai-lakehouse-project/bronze/user_events_parquet/"
silver_path = "s3://ai-lakehouse-project/silver/user_events/"

# Enable dynamic partition overwrite (overwrite only partitions present in the write DF)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Load Bronze
bronze_df = spark.read.format("parquet").load(bronze_path)

# Preprocess / validate required fields
df = (
    bronze_df
    .filter(col("user_id").isNotNull() & col("event_type").isNotNull())
)

# Ensure required CDC columns exist (Bronze should already provide these)
# If event_timestamp or event_date aren't present, derive them defensively.
if "event_timestamp" not in df.columns:
    df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))

if "event_date" not in df.columns:
    df = df.withColumn("event_date", to_date(col("event_timestamp")))

# CDC resolution: latest-wins per pk ordered by updated_at
# (pk + updated_at should exist from Bronze CDC contract)
windowSpec = Window.partitionBy("pk").orderBy(col("updated_at").desc())

df_ranked = df.withColumn("rn", row_number().over(windowSpec))

# Keep latest version per pk
df_latest = df_ranked.filter(col("rn") == 1).drop("rn")

# Remove tombstones (op='D') → current/active records only
df_active = df_latest.filter(col("op") != "D")

# Select Silver columns (keep CDC fields for downstream merge/history later)
silver_out = df_active.select(
    "pk",
    "user_id",
    "event_type",
    "event_timestamp",
    "event_date",
    "updated_at",
    "op",
    "feature_hash",
    "model_input_flag",
    "run_id",
    "ingestion_ts"
)

# Write to Silver (overwrite only touched partitions due to dynamic mode)
(
    silver_out.write
    .mode("overwrite")
    .partitionBy("event_type", "event_date")
    .parquet(silver_path)
)

job.commit()