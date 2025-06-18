import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
bronze_path = "s3://ai-lakehouse-project/bronze/user_events_parquet/"
silver_path = "s3://ai-lakehouse-project/silver/user_events/"

# 1. Load Bronze Parquet
bronze_df = spark.read.format("parquet").load(bronze_path)

# 2. Transformations: Dedup, Clean, Normalize
silver_df = bronze_df.dropDuplicates(["user_id", "event_type", "timestamp"]) \
    .filter(col("user_id").isNotNull() & col("event_type").isNotNull()) \
    .withColumn("event_timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("event_date", to_date("event_timestamp")) \
    .select(
        "user_id",
        "event_type",
        "event_timestamp",
        "event_date",
        "feature_hash",
        "model_input_flag"
    )

# 3. Write to Silver: Partitioned Parquet
silver_df.write.mode("overwrite") \
    .partitionBy("event_type", "event_date") \
    .parquet(silver_path)

# Done
job.commit()