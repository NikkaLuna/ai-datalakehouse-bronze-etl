import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, row_number
from awsglue.context import GlueContext
from awsglue.job import Job

# Init
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
bronze_path = "s3://ai-lakehouse-project/bronze/user_events_parquet/"
silver_path = "s3://ai-lakehouse-project/silver/user_events/"

# Load Bronze
bronze_df = spark.read.format("parquet").load(bronze_path)

# Preprocess
df = bronze_df \
    .filter(col("user_id").isNotNull() & col("event_type").isNotNull()) \
    .withColumn("event_timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("event_date", to_date("event_timestamp"))

# Deduplication: keep latest by user_id + timestamp
windowSpec = Window.partitionBy("user_id", "event_timestamp").orderBy(col("ingestion_ts").desc())
dedup_df = df.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).drop("rn")

# Extract partitions
partitions = [row["event_date"] for row in dedup_df.select("event_date").distinct().collect()]

# Overwrite only impacted partitions
for part in partitions:
    part_df = dedup_df.filter(col("event_date") == part)
    part_df.select(
        "user_id",
        "event_type",
        "event_timestamp",
        "event_date",
        "feature_hash",
        "model_input_flag"
    ).write.mode("overwrite") \
     .partitionBy("event_type", "event_date") \
     .parquet(silver_path)

# Done
job.commit()
