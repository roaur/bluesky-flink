# Bluesky Firehose Analytics Pipeline

A fully-containerized real-time analytics pipeline that streams data from the **Bluesky Jetstream** firehose, buffers it into **Redpanda** using Avro Schema Registry, processes it via **Apache Flink SQL**, and persists the data to two specialized destinations: an **Iceberg** Data Lake (via Nessie and MinIO) for deep historic storage, and **ClickHouse** for sub-second dashboarding.

---

## 🏗️ How It Works

1. **Ingestion**: A custom Python lightweight WebSocket consumer (`firehose-consumer`) connects to the Bluesky Jetstream firehose (`wss://jetstream1.us-east.bsky.network/subscribe`). It filters for post creation events, extracts hashtags and terms, attaches an `obtained_at` timestamp for latency tracking, and serializes the payload using **Avro** (automatically registering the schema with Redpanda).
2. **Buffering**: The consumer publishes these Avro events to a **Redpanda** topic (`raw-events`) compressed with LZ4.
3. **Stream Processing (Flink)**: 
   - A **Raw Ingestion Job** simply copies all raw Avro events straight from Redpanda into the Iceberg Lakehouse.
   - **Aggregation Jobs** window the events into 5-minute tumbling windows to calculate keyword frequency and trending hashtags. These 5-minute aggregates are streamed *back* into Redpanda.
4. **Data Sinks**:
   - **Iceberg (MinIO + Nessie)**: Acts as the historic source of truth, storing Parquet blobs on local MinIO storage.
   - **ClickHouse**: Natively connects to the downstream Redpanda topics using its Kafka Engine to pull in the 5-minute aggregations via Materialized Views, exposing them for blazingly fast Grafana dashboards.

---

## 📂 Repository Structure

This monorepo is completely infrastructure-agnostic, meaning the application code is separated from deployment strategies.

```text
/bluesky-flink
├── firehose-consumer/      # Python source code, Dockerfile, and requirements
├── flink-jobs/             # Flink SQL scripts (business logic)
├── k8s/                    # Kubernetes manifests, configurations, and deep-dive documentation
├── compose/                # Legacy Docker Compose orchestrator and local configs
├── PLAN.md                 # Detailed architectural blueprint and schemas
├── bootstrap.sh            # One-click startup script for the Kubernetes architecture
└── README.md
```

Detailed definitions of the Kubernetes objects used in this project can be found in `k8s/README.md`.

---

## ⚙️ Prerequisites & Requirements

To run this pipeline locally, you must have the following tools installed on your host machine:

### Core Infrastructure Tools
* **Docker Engine** (or Docker Desktop)
* **[kind](https://kind.sigs.k8s.io/)** (Kubernetes IN Docker)  
* **kubectl** (Kubernetes CLI)

### Python Development Tools
* **Python 3.13+** (Required for the `firehose-consumer`)
* **[uv](https://github.com/astral-sh/uv)** (The hyper-fast Python package installer/resolver written in Rust)

### 📚 Tech Stack Versions
The pipeline specifically relies on the following engine versions:
* **Apache Flink:** `1.18` (Scala 2.12 / Java 11)
* **Apache Iceberg:** `1.5.2` (AWS Bundle & Flink Runtime)
* **Redpanda:** `latest` (With built-in Schema Registry on port `8081`)
* **ClickHouse:** `latest`
* **Nessie:** `latest`
* **MinIO:** `latest`
* **Confluent Kafka Python:** `2.3.0` (with `fastavro` dependencies)

---

## 🚀 Quickstart: Running the Pipeline

This repository is infrastructure-agnostic. You can run the entire pipeline locally using either a full Kubernetes simulation (`kind`) or the legacy `docker-compose` orchestrator.

### Option 1: Kubernetes via `kind` (Recommended)
We use `kind` to perfectly simulate a production-grade orchestration cluster locally.

1. Ensure the Docker daemon is running.
2. Make the bootstrap script executable:
   ```bash
   chmod +x ./bootstrap.sh
   ```
3. Run the bootstrap script:
   ```bash
   ./bootstrap.sh
   ```

**What the bootstrap does automatically:**
- Spins up a local `kind` Kubernetes cluster named `bluesky-cluster`.
- Uses your local Docker daemon to build the `firehose-consumer` Python image.
- Injects that freshly built image directly into the Kubernetes cluster cache.
- Sequentially applies all Namespaces, ConfigMaps, StatefulSets (Databases), and Deployments via `kubectl apply`.

### Option 2: Docker Compose
If you prefer a simpler local environment without Kubernetes, the legacy Docker Compose manifests are fully supported.

1. Navigate to the compose directory:
   ```bash
   cd compose/
   ```
2. Spin up the entire stack in detached mode:
   ```bash
   docker compose up -d --build
   ```
3. To view the logs of the Python consumer or Flink jobs:
   ```bash
   docker compose logs -f firehose-consumer
   ```
4. To shut the entire pipeline down:
   ```bash
   docker compose down -v
   ```
