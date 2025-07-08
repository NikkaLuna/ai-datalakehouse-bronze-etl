from pyspark.sql.functions import col, max, count, when, current_date, datediff, row_number
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import sys

# Init
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
silver_path = "s3://ai-lakehouse-project/silver/user_events/"
gold_path = "s3://ai-lakehouse-project/gold/user_features/"

# Load Silver
silver_df = spark.read.format("parquet").load(silver_path)

# Optional: Deduplicate (safety net)
windowSpec = Window.partitionBy("user_id", "event_timestamp").orderBy(col("event_timestamp").desc())
dedup_df = silver_df.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).drop("rn")

# Aggregate Features
gold_df = dedup_df.groupBy("user_id").agg(
    max("event_timestamp").alias("last_event_timestamp"),
    max("event_type").alias("last_event_type"),
    max("feature_hash").alias("last_feature_hash"),
    count(when(col("event_type") == "click", True)).alias("click_count"),
    count(when(col("event_type") == "purchase", True)).alias("purchase_count"),
    datediff(current_date(), max("event_timestamp")).alias("days_since_last_event")
)

# Optional: Add 'training_date' column if you want to partition the output
gold_df = gold_df.withColumn("training_date", current_date())

# Write to S3 (overwrite OR partitioned)
gold_df.write.mode("overwrite") \
    .partitionBy("training_date") \
    .parquet(gold_path)

# Register table in Athena (optional but recommended)
spark.sql("CREATE DATABASE IF NOT EXISTS ai_lakehouse_db")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ai_lakehouse_db.gold_user_features (
        user_id STRING,
        last_event_timestamp TIMESTAMP,
        last_event_type STRING,
        last_feature_hash STRING,
        click_count INT,
        purchase_count INT,
        days_since_last_event INT
    )
    PARTITIONED BY (training_date DATE)
    STORED AS PARQUET
    LOCATION '{gold_path}'
""")

job.commit()
