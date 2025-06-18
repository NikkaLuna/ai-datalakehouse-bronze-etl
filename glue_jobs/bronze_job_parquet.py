import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit

# Define job arguments first
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark context and Glue context (only once)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read all JSON files from S3 folder
df_raw = spark.read \
    .option("multiline", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("s3://ai-lakehouse-project/raw/")

# Enrich data for AI-readiness
df_enriched = df_raw.withColumn("ingestion_ts", current_timestamp()) \
                    .withColumn("source", lit("crawler")) \
                    .withColumn("model_input_flag", lit(True)) \
                    .withColumn("inference_ts", current_timestamp()) \
                    .withColumn("feature_hash", lit("v1-user-hash"))

# Write as Parquet table to bronze layer
df_enriched.write.format("parquet") \
    .mode("overwrite") \
    .save("s3://ai-lakehouse-project/bronze/user_events_parquet/")

# Create database first, then register table
spark.sql("CREATE DATABASE IF NOT EXISTS ai_lakehouse_db")

# Register Parquet table in Data Catalog
spark.sql("""
CREATE TABLE IF NOT EXISTS ai_lakehouse_db.bronze_user_events_parquet
USING PARQUET
LOCATION 's3://ai-lakehouse-project/bronze/user_events_parquet/'
""")

# Commit the job
job.commit()