"""
Source 2 — Mockoon-style Mock API + FastAPI Kafka Producer
----------------------------------------------------------
Simulates the Mockoon API block from the diagram.
A built-in mock endpoint generates realistic bank-like transaction
JSON, and a background loop continuously POSTs to the FastAPI
endpoint which then publishes the event to Kafka.
"""

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "transactions")
SOURCE_NAME   = os.getenv("SOURCE_NAME", "source2")
INTERVAL      = float(os.getenv("PRODUCE_INTERVAL_SEC", "2"))

MERCHANTS  = ["Amazon", "Netflix", "Uber", "Starbucks", "Apple", "Walmart"]
CATEGORIES = ["e-commerce", "streaming", "transport", "food", "tech", "retail"]
CURRENCIES = ["USD", "EUR", "GBP", "CAD"]

app = FastAPI(title="Source 2 – Mock API Producer")

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
            import time; time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries")

producer: KafkaProducer | None = None

# ── Mock data generator (mimics Mockoon response) ────────────────────────────
def generate_mock_transaction() -> dict:
    merchant = random.choice(MERCHANTS)
    return {
        "transaction_id": str(uuid.uuid4()),
        "source":         SOURCE_NAME,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "amount":         round(random.uniform(1.0, 500.0), 2),
        "currency":       random.choice(CURRENCIES),
        "merchant":       merchant,
        "category":       CATEGORIES[MERCHANTS.index(merchant)],
        "status":         random.choices(["success", "pending", "failed"], weights=[80, 15, 5])[0],
        "user_id":        f"user_{random.randint(1000, 9999)}",
        "card_last4":     str(random.randint(1000, 9999)),
    }

# ── FastAPI endpoints ─────────────────────────────────────────────────────────
@app.get("/mock-api")
def mock_api():
    """Mimics the Mockoon API — returns a fresh transaction JSON."""
    return generate_mock_transaction()

@app.post("/produce")
def produce():
    """FastAPI endpoint that publishes one transaction to Kafka."""
    tx = generate_mock_transaction()
    producer.send(KAFKA_TOPIC, value=tx)
    producer.flush()
    print(f"[source2] → Kafka | {tx['transaction_id']} | {tx['merchant']} | {tx['amount']} {tx['currency']}")
    return {"status": "sent", "transaction_id": tx["transaction_id"]}

@app.get("/health")
def health():
    return {"status": "ok", "source": SOURCE_NAME}

# ── Background producer loop ──────────────────────────────────────────────────
async def auto_produce():
    global producer
    await asyncio.sleep(5)           # let Kafka settle
    producer = create_producer()
    while True:
        try:
            tx = generate_mock_transaction()
            producer.send(KAFKA_TOPIC, value=tx)
            producer.flush()
            print(f"[source2] → Kafka | {tx['transaction_id']} | {tx['merchant']} | {tx['amount']}")
        except Exception as e:
            print(f"[source2] Produce error: {e}")
        await asyncio.sleep(INTERVAL)

@app.on_event("startup")
async def startup():
    asyncio.create_task(auto_produce())

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)