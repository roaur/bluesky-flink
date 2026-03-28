-- 00-init-catalogs.sql
-- Initialize Flink catalogs: Iceberg (Nessie) and Kafka source

-- Iceberg catalog with Nessie (Git for Data)
CREATE CATALOG nessie_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://nessie:19120',
    'warehouse' = 's3://bluesky-lakehouse',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key-id' = 'blueskyadmin',
    's3.secret-access-key' = 'blueskysecret',
    'format' = 'parquet'
);

USE CATALOG nessie_catalog;
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_events (
    did           STRING,
    cid           STRING,
    text          STRING,
    hashtags      ARRAY<STRING>,
    created_at    TIMESTAMP(3),
    obtained_at   TIMESTAMP(3)
)
PARTITIONED BY (days(obtained_at));

CREATE TABLE IF NOT EXISTS analytics.post_keywords (
    word          STRING,
    `count`       BIGINT,
    window_start  TIMESTAMP(3),
    obtained_at   TIMESTAMP(3),
    created_at    TIMESTAMP(3)
)
PARTITIONED BY (days(window_start));

CREATE TABLE IF NOT EXISTS analytics.trending_topics (
    hashtag       STRING,
    `count`       BIGINT,
    window_start  TIMESTAMP(3),
    obtained_at   TIMESTAMP(3),
    created_at    TIMESTAMP(3)
)
PARTITIONED BY (days(window_start));

CREATE TABLE IF NOT EXISTS analytics.keyword_counts_sink (
    word          STRING,
    `count`       BIGINT,
    window_start  TIMESTAMP(3),
    obtained_at   TIMESTAMP(3),
    created_at    TIMESTAMP(3)
)
WITH (
    'connector' = 'kafka', 
    'topic' = 'keyword-counts', 
    'properties.bootstrap.servers' = 'redpanda:9092', 
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://redpanda:8081'
);

CREATE TABLE IF NOT EXISTS analytics.trending_topics_sink (
    hashtag       STRING,
    `count`       BIGINT,
    window_start  TIMESTAMP(3),
    obtained_at   TIMESTAMP(3),
    created_at    TIMESTAMP(3)
)
WITH (
    'connector' = 'kafka', 
    'topic' = 'trending-topics', 
    'properties.bootstrap.servers' = 'redpanda:9092', 
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://redpanda:8081'
);

CREATE TEMPORARY TABLE raw_events (
    did         STRING,
    cid         STRING,
    text        STRING,
    hashtags    ARRAY<STRING>,
    created_at  BIGINT,
    obtained_at BIGINT,
    event_time  AS TO_TIMESTAMP_LTZ(created_at, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'raw-events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-sql-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://redpanda:8081'
);
