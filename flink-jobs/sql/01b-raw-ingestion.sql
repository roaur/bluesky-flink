-- 01-raw-ingestion.sql
-- Stream all raw events straight to the Iceberg Data Lake

INSERT INTO analytics.raw_events
SELECT 
    did, 
    cid, 
    text, 
    hashtags,
    TO_TIMESTAMP_LTZ(created_at, 3) AS created_at,
    TO_TIMESTAMP_LTZ(obtained_at, 3) AS obtained_at
FROM raw_events;
