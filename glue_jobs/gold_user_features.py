from pyspark.sql.functions import col, max, count, when
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import sys

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
silver_path = "s3://ai-lakehouse-project/silver/user_events/"
gold_path = "s3://ai-lakehouse-project/gold/user_features/"

# Load Silver layer
silver_df = spark.read.format("parquet").load(silver_path)

# Transform to Gold layer (aggregate features per user)
gold_df = silver_df.groupBy("user_id").agg(
    max("event_timestamp").alias("last_event_timestamp"),
    max("event_type").alias("last_event_type"),
    max("feature_hash").alias("last_feature_hash"),
    count(when(col("event_type") == "click", True)).alias("click_count"),
    count(when(col("event_type") == "purchase", True)).alias("purchase_count")
)

# Write to S3 as Gold
gold_df.write.mode("overwrite").parquet(gold_path)

job.commit()