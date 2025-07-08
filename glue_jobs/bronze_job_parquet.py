import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws, col
from pyspark.sql.types import TimestampType

# Get job args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
bronze_path = "s3://ai-lakehouse-project/bronze/user_events_parquet/"
raw_path = "s3://ai-lakehouse-project/raw/"

# 1. Determine watermark (max timestamp in Bronze)
try:
    bronze_df = spark.read.parquet(bronze_path)
    max_ts = bronze_df.selectExpr("max(timestamp) as max_ts").collect()[0]["max_ts"]
    print(f"Watermark loaded: {max_ts}")
except Exception as e:
    print(f"No existing Bronze data found. Full load will be performed.")
    max_ts = None

# 2. Read Raw JSON with schema safety
df_raw = spark.read \
    .option("multiline", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json(raw_path)

# 3. Filter new data only (Incremental)
if max_ts:
    df_filtered = df_raw.filter(col("timestamp").cast(TimestampType()) > lit(max_ts))
else:
    df_filtered = df_raw

# 4. Enrich Data
df_enriched = df_filtered \
    .withColumn("ingestion_ts", current_timestamp()) \
    .withColumn("source", lit("crawler")) \
    .withColumn("model_input_flag", lit(True)) \
    .withColumn("inference_ts", current_timestamp()) \
    .withColumn("feature_hash", sha2(concat_ws("||", *df_filtered.columns), 256))

# 5. Write to Bronze (append, not overwrite!)
df_enriched.write.format("parquet").mode("append").save(bronze_path)

# 6. Register if needed
spark.sql("CREATE DATABASE IF NOT EXISTS ai_lakehouse_db")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ai_lakehouse_db.bronze_user_events_parquet
    USING PARQUET
    LOCATION '{bronze_path}'
""")

# Commit
job.commit()
