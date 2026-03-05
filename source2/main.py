"""
Source 2 — Mockoon Fetcher + FastAPI Kafka Producer
----------------------------------------------------
Fetches real transaction data from your running Mockoon instance
(http://host.docker.internal:3000/api/transaction) using the
requests library, then publishes each response to Kafka.

Mockoon must be running on your host machine on port 3000 before
starting this container. The endpoint /api/transaction should
return a single transaction JSON object per call.
"""

import asyncio
import json
import os
import time

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "transactions")
SOURCE_NAME    = os.getenv("SOURCE_NAME", "source2")
INTERVAL       = float(os.getenv("PRODUCE_INTERVAL_SEC", "2"))

# host.docker.internal resolves to the host machine from inside Docker.
# Override via env var if needed (e.g. for Linux: set to your host LAN IP).
MOCKOON_BASE   = os.getenv("MOCKOON_BASE_URL", "http://host.docker.internal:3000")
MOCKOON_PATH   = os.getenv("MOCKOON_ENDPOINT", "/api/transaction")
MOCKOON_URL    = f"{MOCKOON_BASE}{MOCKOON_PATH}"

app = FastAPI(title="Source 2 – Mockoon Fetcher + Kafka Producer")

# ── Kafka producer (with retry) ───────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    for attempt in range(1, 20):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[source2] Kafka producer connected on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            print(f"[source2] Waiting for Kafka... attempt {attempt}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries")

producer: KafkaProducer | None = None

# ── Fetch one transaction from Mockoon ───────────────────────────────────────
def fetch_from_mockoon() -> dict:
    """
    GETs a transaction from Mockoon and injects source metadata.
    Raises requests.RequestException if Mockoon is unreachable.
    """
    response = requests.get(MOCKOON_URL, timeout=5)
    response.raise_for_status()
    tx = response.json()

    # Stamp the source so the consumer/dashboard can distinguish it
    tx["source"] = SOURCE_NAME
    return tx

# ── FastAPI endpoints ─────────────────────────────────────────────────────────
@app.get("/fetch")
def fetch():
    """Manually fetch one transaction from Mockoon (without publishing)."""
    try:
        return fetch_from_mockoon()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Mockoon unreachable: {e}")

@app.post("/produce")
def produce():
    """Fetch from Mockoon and publish one transaction to Kafka."""
    try:
        tx = fetch_from_mockoon()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Mockoon unreachable: {e}")

    producer.send(KAFKA_TOPIC, value=tx)
    producer.flush()
    print(f"[source2] → Kafka | {tx.get('transaction_id','?')} | "
          f"{tx.get('merchant','?')} | {tx.get('amount','?')} {tx.get('currency','?')}")
    return {"status": "sent", "transaction_id": tx.get("transaction_id")}

@app.get("/health")
def health():
    mockoon_ok = True
    try:
        requests.get(MOCKOON_URL, timeout=2).raise_for_status()
    except Exception:
        mockoon_ok = False
    return {
        "status": "ok",
        "source": SOURCE_NAME,
        "mockoon_reachable": mockoon_ok,
        "mockoon_url": MOCKOON_URL,
    }

# ── Background producer loop ──────────────────────────────────────────────────
async def auto_produce():
    global producer
    await asyncio.sleep(5)          # let Kafka settle first
    producer = create_producer()

    print(f"[source2] Starting auto-fetch loop from {MOCKOON_URL} every {INTERVAL}s")

    while True:
        try:
            tx = fetch_from_mockoon()
            producer.send(KAFKA_TOPIC, value=tx)
            producer.flush()
            print(f"[source2] → Kafka | {tx.get('transaction_id','?')} | "
                  f"{tx.get('merchant','?')} | {tx.get('amount','?')}")
        except requests.RequestException as e:
            print(f"[source2] Mockoon fetch failed (is it running on port 3000?): {e}")
        except Exception as e:
            print(f"[source2] Produce error: {e}")

        await asyncio.sleep(INTERVAL)

@app.on_event("startup")
async def startup():
    asyncio.create_task(auto_produce())

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)