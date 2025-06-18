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
