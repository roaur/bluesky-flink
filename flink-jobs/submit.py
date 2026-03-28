#!/usr/bin/env python3
"""
Submit Flink SQL jobs to the JobManager via REST API.

Usage:
    python submit.py [sql_file ...]

Reads the job manager address from JOB_MANAGER_RPC_ADDRESS (default: flink-jobmanager)
and FLINK_REST_PORT (default: 8081).
"""

import os
import sys
import time
import urllib.request
import urllib.error
import json
from pathlib import Path


JOB_MANAGER = os.environ.get("JOB_MANAGER_RPC_ADDRESS", "flink-jobmanager")
REST_PORT = os.environ.get("FLINK_REST_PORT", "8081")
BASE_URL = f"http://{JOB_MANAGER}:{REST_PORT}"


def wait_for_jobmanager(timeout: int = 60) -> bool:
    """Wait for JobManager REST API to be healthy."""
    print(f"[INFO] Waiting for JobManager at {BASE_URL}...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{BASE_URL}/overview")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    print("[INFO] JobManager is ready", flush=True)
                    return True
        except urllib.error.URLError:
            pass
        time.sleep(2)
    print("[ERROR] JobManager not available after timeout", flush=True)
    return False


def submit_sql_file(sql_content: str, job_name: str) -> dict | None:
    """Submit a SQL file as a Flink job via the REST API."""
    payload = {
        "type": "sql",
        "sql": sql_content,
    }
    body = json.dumps(payload).encode("utf-8")
    url = f"{BASE_URL}/jobs"
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            print(
                f"[INFO] Submitted '{job_name}': job_id={result.get('id')}", flush=True
            )
            return result
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"[ERROR] Failed to submit '{job_name}': {e.code} {body}", flush=True)
        return None


def main():
    sql_files = sys.argv[1:]
    if not sql_files:
        print("[ERROR] No SQL files provided", flush=True)
        sys.exit(1)

    if not wait_for_jobmanager():
        sys.exit(1)

    for sql_file in sql_files:
        path = Path(sql_file)
        if not path.exists():
            print(f"[WARN] File not found: {sql_file}", flush=True)
            continue

        print(f"[INFO] Submitting {sql_file}...", flush=True)
        content = path.read_text()
        result = submit_sql_file(content, path.name)
        if result is None:
            print(f"[ERROR] Failed to submit {sql_file}", flush=True)
        else:
            print(f"[OK] {sql_file} submitted as job {result.get('id')}", flush=True)

        time.sleep(2)

    print("[INFO] All SQL jobs submitted", flush=True)


if __name__ == "__main__":
    main()
