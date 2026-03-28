#!/usr/bin/env python3
import os
import json
import re
import time
import signal
from datetime import datetime, timezone
import websocket
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
JETSTREAM_URL = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
TOPIC = "raw-events"

running = True

# Flink/Iceberg expects timestamps in string or proper millis format. 
# Avro timestamp-millis is standard.
SCHEMA_STR = """
{
  "namespace": "analytics",
  "name": "raw_events",
  "type": "record",
  "fields": [
    {"name": "did", "type": "string"},
    {"name": "cid", "type": "string"},
    {"name": "text", "type": "string"},
    {"name": "hashtags", "type": {"type": "array", "items": "string"}},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "obtained_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

def extract_hashtags(text):
    return [tag.lower() for tag in re.findall(r"#\w+", text)]

def parse_iso8601(timestamp_str):
    try:
        # Some are full ISO, some might be lacking Z or have different microsec formatting
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)

def delivery_report(err, msg):
    if err is not None:
        print(f"[WARN] Delivery failed: {err}")

def setup_kafka():
    print(f"[INFO] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Connecting to Schema Registry at {SCHEMA_REGISTRY_URL}")
    
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    
    # We use confluent_kafka's AvroSerializer which prepends the 5-byte magic + schema ID
    avro_serializer = AvroSerializer(schema_registry_client, SCHEMA_STR)

    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'compression.type': 'lz4',
        'linger.ms': 5,
        'batch.size': 32768,
        'acks': 'all'
    }
    producer = Producer(producer_conf)
    return producer, avro_serializer

def signal_handler(signum, frame):
    global running
    running = False

def main():
    global running
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    producer, avro_serializer = None, None
    delay = 1

    while running:
        try:
            if producer is None:
                producer, avro_serializer = setup_kafka()

            print(f"[INFO] Connecting to Jetstream at {JETSTREAM_URL}")
            ws = websocket.WebSocket()
            ws.connect(JETSTREAM_URL)
            delay = 1 # reset delay on successful connect

            print("[INFO] Connection established, receiving messages...")
            while running:
                message = ws.recv()
                if not message:
                    continue

                obtained_at = int(time.time() * 1000)
                data = json.loads(message)

                if data.get("kind") != "commit":
                    continue
                
                commit = data.get("commit", {})
                if commit.get("operation") != "create":
                    continue
                if commit.get("collection") != "app.bsky.feed.post":
                    continue

                record = commit.get("record", {})
                text = record.get("text", "")
                if not text:
                    continue

                created_at_str = record.get("createdAt", "")
                created_at_ms = parse_iso8601(created_at_str)

                event = {
                    "did": data.get("did", ""),
                    "cid": commit.get("cid", ""),
                    "text": text,
                    "hashtags": extract_hashtags(text),
                    "created_at": created_at_ms,
                    "obtained_at": obtained_at
                }

                # Serialize and send
                val = avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
                producer.produce(
                    topic=TOPIC,
                    key=event["did"].encode('utf-8'),
                    value=val,
                    on_delivery=delivery_report
                )
                producer.poll(0)

        except Exception as e:
            if not running:
                break
            print(f"[ERROR] {e}. Retrying in {delay}s...", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60)
            producer = None

    if producer:
        producer.flush()
        print("[INFO] Producer closed.")

if __name__ == "__main__":
    main()
