import sys
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, count, countDistinct, expr

# Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
silver_path = "s3://ai-lakehouse-project/silver/user_events/"
dq_output_path = "s3://ai-lakehouse-project/reports/dq_report.json"

# Load Silver
df = spark.read.format("parquet").load(silver_path)

# Total record count
total_rows = df.count()

# Null counts and percentages for key fields
columns_to_check = ['user_id', 'event_type', 'event_timestamp']

null_checks = {}
for col_name in columns_to_check:
    null_count = df.filter(col(col_name).isNull()).count()
    null_checks[col_name] = {
        "null_count": null_count,
        "null_percent": round(null_count / total_rows * 100, 2) if total_rows else 0.0
    }

# Duplicate check: user_id + event_timestamp
duplicate_count = df.groupBy("user_id", "event_timestamp") \
    .count().filter("count > 1").count()

# Distinct user count
distinct_users = df.select("user_id").distinct().count()

# Assemble summary
dq_summary = {
    "timestamp": datetime.utcnow().isoformat(),
    "total_rows": total_rows,
    "distinct_users": distinct_users,
    "null_checks": null_checks,
    "duplicate_records": duplicate_count,
    "passed": duplicate_count == 0 and all(v["null_count"] == 0 for v in null_checks.values())
}

# Convert to single-row DF and write as JSON
dq_df = spark.createDataFrame([dq_summary])
dq_df.coalesce(1).write.mode("overwrite").json(dq_output_path)

# Done
job.commit()
