AI-Ready Bronze ETL Pipeline (AWS Glue → S3 → Parquet → Athena)
==================================================================

Build an AI-ready lakehouse bronze layer pipeline using AWS Glue to transform raw JSON data into enriched Parquet files. The data is registered in AWS Glue Catalog and made queryable via Amazon Athena.

![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazon-aws)
![Glue](https://img.shields.io/badge/Glue-Spark-blue?logo=apache-spark)
![Athena](https://img.shields.io/badge/Athena-SQL-blue)
![S3](https://img.shields.io/badge/S3-Parquet-yellow?logo=amazon-s3)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

* * * * *

🚀 Overview
-----------

This project demonstrates a real-time ELT workflow that ingests raw JSON data, enriches it with AI metadata fields, stores it in optimized Parquet format on Amazon S3, and exposes it via Athena for downstream ML workloads.

* * * * *

Architecture
---------------

```
Raw JSON (S3)
     ↓
AWS Glue Job (PySpark)
     ↓
Bronze Layer (Parquet in S3)
     ↓
Glue Catalog Table
     ↓
Athena Query Layer (SQL)
```

* * * * *

Tech Stack
-------------

| Layer | Technology |
| Cloud Platform | AWS |
| Processing Engine | AWS Glue (Spark 3.5 / PySpark) |
| Storage Format | Parquet on S3 |
| Query Layer | Athena (Trino SQL) |
| Workflow/Metadata | AWS Glue Crawlers, Glue Catalog |

* * * * *

📂 Project Structure
--------------------

```
├── glue_jobs/
│   └── bronze_job_parquet.py
├── queries/
│   └── athena_sample_query.sql
├── screenshots/
│   ├── glue_job_success.png
│   ├── crawler_complete.png
│   ├── s3_bronze_output.png
│   └── athena_query_success.png
└── README.md
```

* * * * *

Screenshots
---------------

### Bronze Job Success (Glue ETL)

![Glue Job Success Screenshot](screenshots/glue_job_success.png)

This screenshot shows the successful execution of the `bronze_job_parquet` ETL job, which ingests and enriches raw JSON from S3 and writes AI-ready data to the bronze layer in Parquet format.  
It confirms the job ran with **2 DPUs** on **AWS Glue 5.0** and completed in **under 2 minutes**

### Glue Data Catalog: Registered Bronze Table

![Glue Data Catalog Screenshot](screenshots/glue_catalog_bronze_table.png)

Shows the successfully registered `bronze_user_events_parquet` table in the `ai_lakehouse_db` database.  
This table references the Parquet files written by the AWS Glue job and is now **queryable in Athena** for downstream AI analytics.


### Bronze Layer Output (Parquet Format)

![S3 Bronze Output](screenshots/s3_bronze_parquet_output.png)

This screenshot shows enriched event data successfully written by the Glue ETL job to the  
`s3://ai-lakehouse-project/bronze/user_events_parquet/` S3 folder in partitioned **Parquet format**.  
Timestamps confirm the files are fresh and reflect schema-enriched, AI-ready ingestion,  
suitable for downstream querying via **Athena** or **Redshift Spectrum**.


### Athena Query Success: Bronze Layer Output

![Athena Query Result](screenshots/athena_query_parquet_success.png)

This Athena query validates that the `bronze_user_events_parquet` table is registered and queryable.  
It filters for records marked as `model_input_flag = true` and selects AI-relevant fields such as  
`user_id`, `event_type`, and `feature_hash`.

The query succeeded in under 2 seconds, demonstrating low-latency access to enriched event data  
in Parquet format — optimized for downstream **AI analytics** and **feature engineering**.


* * * * *

Sample Athena Query
----------------------

```
SELECT
  user_id,
  event_type,
  ingestion_ts,
  model_input_flag,
  inference_ts,
  feature_hash
FROM ai_lakehouse_db.bronze_user_events_parquet
WHERE model_input_flag = true
LIMIT 10;
```

* * * * *


Contact
----------

For collaboration, mentorship, or questions, feel free to connect via GitHub Discussions or Issues.