"""
Source 1 — Faker Generator + FastAPI Kafka Producer
----------------------------------------------------
Simulates the Faker block from the diagram.
Uses the Faker library to generate realistic mocked JSON data,
exposes a FastAPI endpoint, and continuously pushes events to Kafka.
"""

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone

import uvicorn
from faker import Faker
from fastapi import FastAPI
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "transactions")
SOURCE_NAME   = os.getenv("SOURCE_NAME", "source1")
INTERVAL      = float(os.getenv("PRODUCE_INTERVAL_SEC", "3"))

fake = Faker()
app  = FastAPI(title="Source 1 – Faker Producer")

# ── Kafka producer (with retry) ───────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    for attempt in range(1, 20):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[source1] Kafka producer connected on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            print(f"[source1] Waiting for Kafka... attempt {attempt}")
            import time; time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after retries")

producer: KafkaProducer | None = None

# ── Faker-based transaction generator ────────────────────────────────────────
def generate_faker_transaction() -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "source":         SOURCE_NAME,
        "timestamp":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z",
        "amount":         round(random.uniform(5.0, 2000.0), 2),
        "currency":       random.choice(["USD", "EUR", "GBP", "JPY", "AUD"]),
        "merchant":       fake.company(),
        "category":       random.choice(["retail", "food", "travel", "health", "entertainment"]),
        "status":         random.choices(["success", "pending", "failed"], weights=[75, 15, 10])[0],
        "user_id":        fake.uuid4(),
        "user_name":      fake.name(),
        "user_email":     fake.email(),
        "user_country":   fake.country_code(),
        "card_last4":     fake.credit_card_number()[-4:],
        "ip_address":     fake.ipv4(),
    }

# ── FastAPI endpoints ─────────────────────────────────────────────────────────
@app.get("/generate")
def generate():
    """Returns a single Faker-generated transaction JSON."""
    return generate_faker_transaction()

@app.post("/produce")
def produce():
    """FastAPI endpoint — generates + publishes one transaction to Kafka."""
    tx = generate_faker_transaction()
    producer.send(KAFKA_TOPIC, value=tx)
    producer.flush()
    print(f"[source1] → Kafka | {tx['transaction_id']} | {tx['merchant']} | {tx['amount']}")
    return {"status": "sent", "transaction_id": tx["transaction_id"]}

@app.get("/health")
def health():
    return {"status": "ok", "source": SOURCE_NAME}

# ── Background producer loop ──────────────────────────────────────────────────
async def auto_produce():
    global producer
    await asyncio.sleep(7)           # slightly offset from source2
    producer = create_producer()
    while True:
        try:
            tx = generate_faker_transaction()
            producer.send(KAFKA_TOPIC, value=tx)
            producer.flush()
            print(f"[source1] → Kafka | {tx['transaction_id']} | {tx['merchant']} | {tx['amount']}")
        except Exception as e:
            print(f"[source1] Produce error: {e}")
        await asyncio.sleep(INTERVAL)

@app.on_event("startup")
async def startup():
    asyncio.create_task(auto_produce())

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)