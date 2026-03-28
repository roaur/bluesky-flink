# Bluesky Firehose Analytics Pipeline — Implementation Plan

## Overview

Real-time analytics pipeline that streams data from the Bluesky Jetstream firehose, buffers in Redpanda (for Kafka API & Avro Schema Registry), processes with Apache Flink (SQL), and persists to a MinIO-backed Iceberg data lake. ClickHouse is also heavily utilized for real-time dashboarding.

**Goal:** Track keyword frequency and hashtag usage across the Bluesky network in 5-minute tumbling windows. All data is written with Avro encoding and LZ4 compression. Flink has two responsibilities: store the **raw data** directly into the Iceberg data lake for deep historic analysis, and compute 5-minute aggregations that are written back to Kafka, allowing ClickHouse to ingest the aggregated metrics for low-latency dashboards.

## Architecture

```text
[Bluesky Jetstream Firehose]  wss://jetstream1.us-east.bsky.network/subscribe
           |
           v
[Firehose Consumer]  (Python 3.13 + uv)
  - Jetstream WebSocket consumer (Lightweight JSON)
  - Tokenizes: plain words + hashtags (#tag)
  - Injects `obtained_at` timestamp
  - Serializes to Avro (via Redpanda Schema Registry)
  - LZ4 compressed produce to Redpanda
           |
           v  [Avro + LZ4]
[Redpanda Broker]  (Kafka API + Schema Registry)
  Topic: raw-events
           |
           v  [Avro + LZ4, Flink checkpointing]
[Flink Jobs]  (Apache Flink 1.18, SQL)
  1. RawIngestionJob
     - Streams raw events directly to Iceberg
  2. KeywordCountJob + TrendingTopicsJob
     - 5-min tumbling windows
     - Injects Flink `processing_time` / `window_start`
     - Write back to Redpanda topics (Avro + LZ4) for ClickHouse ingestion
           |
           v
+-----------------------+------------------------+
|                       |                        |
v                       v                        v  
[MinIO] ← [Nessie]    [Redpanda] ----------> [ClickHouse]
Iceberg Lakehouse       |                      Kafka Engine
(RAW events)            +-> metrics topics     Materialized Views
```

## Services

| Service | Image | Ports | RAM |
|---------|-------|-------|-----|
| `minio` | `minio/minio:latest` | 9000, 9001 | 512MB |
| `nessie` | `ghcr.io/projectnessie/nessie:latest` | 19120 | 256MB |
| `redpanda` | `redpandadata/redpanda:latest` | 9092, 8081 | 1GB |
| `clickhouse` | `clickhouse/clickhouse-server:latest` | 8123, 9000 | 1GB |
| `flink-jobmanager` | `flink:1.18-scala-2.12-java11` | 8081 | 1GB |
| `flink-taskmanager` | `flink:1.18-scala-2.12-java11` | 6123 | 1GB |
| `firehose-consumer` | Custom Python image | — | 256MB |

## Topics

| Topic | Key | Value Format | Compression |
|-------|-----|-------------|-------------|
| `raw-events` | `did` | Avro | LZ4 |
| `keyword-counts` | `window_start` | Avro | LZ4 |
| `trending-topics` | `window_start` | Avro | LZ4 |

## Schema Design (Avro & Iceberg)

Added `obtained_at` (when consumer received the event) for time-lag monitoring compared to processing time.

### analytics.raw_events (Iceberg)
```sql
CREATE TABLE analytics.raw_events (
  did           STRING,
  cid           STRING,
  text          STRING,
  hashtags      ARRAY<STRING>,
  created_at    TIMESTAMP,  -- Post creation time
  obtained_at   TIMESTAMP   -- Ingestion time
)
PARTITIONED BY (days(obtained_at));
```

### analytics.post_keywords (ClickHouse / Iceberg)
```sql
CREATE TABLE analytics.post_keywords (
  word          STRING,
  count         BIGINT,
  window_start  TIMESTAMP,
  obtained_at   TIMESTAMP,  -- Copied from raw event (e.g., MIN(obtained_at) for the window)
  created_at    TIMESTAMP   -- Flink processing time
)
PARTITIONED BY (days(window_start));
```

### analytics.trending_topics
```sql
CREATE TABLE analytics.trending_topics (
  hashtag       STRING,
  count         BIGINT,
  window_start  TIMESTAMP,
  obtained_at   TIMESTAMP,  -- Copied from raw event
  created_at    TIMESTAMP   -- Flink processing time
)
PARTITIONED BY (days(window_start));
```

## Flink Job Design (SQL)

All business logic in SQL submitted via `sql-client.sh`. Flink natively integrates with Confluent Schema Registry (compatible with Redpanda on port 8081).

### 00-init-catalogs.sql
- Creates `nessie_catalog` (Iceberg REST catalog → MinIO S3).
- Creates `raw_events` table (Kafka source, Avro format, LZ4).
- Creates Iceberg tables.
- Creates Kafka sink tables (Avro format, LZ4) for ClickHouse ingestion.

### 01-raw-ingestion.sql
1. Consume `raw_events` topic.
2. `INSERT INTO` the Iceberg `analytics.raw_events` table (pass-through).

### 02-post-keywords.sql & 03-trending-topics.sql
1. Consume `raw_events`.
2. 5-min tumbling window.
3. Keep track of `MIN(obtained_at)` for the window to track ingestion lag.
4. `INSERT INTO` Kafka topic (ClickHouse picks this up). Optionally dual-write to Iceberg aggregates.

## ClickHouse Integration

ClickHouse ingests the aggregated metrics directly from Redpanda via the Kafka Engine. This provides lightning-fast analytics decoupled from the main storage/compute.
1. **Kafka Table:** `keyword_counts_queue` (consumes `keyword-counts` topic).
2. **MergeTree Table:** `keyword_counts_mv_target` (stores the data locally for the dashboards).
3. **Materialized View:** Transfers data from the queue to the target table automatically.

## Storage
- **MinIO:** Stores Iceberg Parquet data (the lakehouse).
- **Redpanda:** Tiered storage can be optionally backed by MinIO if retention grows.
- **ClickHouse:** Local disk optimized for fast analytical dashboard access.

## Orchestration & Kubernetes Strategy

**Recommendation:**
- **Local Development:** Use `kind` (Kubernetes in Docker). It's lightweight enough for a laptop to run this stack and mirrors a real Kubernetes API perfectly for testing.
- **NAS Deployment:** Use `k3s`. It is a production-ready, highly efficient, single-binary Kubernetes distribution designed specifically for resource-constrained environments like edge devices or a home NAS. It heavily outperforms full K8s in CPU/RAM overhead while remaining fully CNF certified.

**Migration Path (Docker Compose → Kustomize/Helm):**
| Component | K8s Resource |
|-----------|--------------|
| `minio` / `clickhouse` | `StatefulSet` + `PVC` |
| `redpanda` | Redpanda Operator or Helm Chart (StatefulSet) |
| `nessie` / `jobmanager` | `Deployment` |
| `taskmanager` | Flink Kubernetes Operator |
| `firehose-consumer` | `Deployment` |
