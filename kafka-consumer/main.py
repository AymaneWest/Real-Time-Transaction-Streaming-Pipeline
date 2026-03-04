"""
Kafka Consumer → MongoDB Writer
--------------------------------
Subscribes to the 'transactions' topic, consumes every message
from both Source 1 and Source 2, and persists them into MongoDB
for the dashboard to query.

This is the "Kafka Consumer" block on the right side of the diagram.
"""

import json
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, errors as mongo_errors

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID   = os.getenv("KAFKA_GROUP_ID", "mongo-writer-group")
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB", "transactions_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "transactions")

# ── MongoDB ───────────────────────────────────────────────────────────────────
def connect_mongo():
    for attempt in range(1, 15):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            print(f"[consumer] MongoDB connected on attempt {attempt}")
            return client[MONGO_DB][MONGO_COLLECTION]
        except Exception as e:
            print(f"[consumer] Waiting for MongoDB... attempt {attempt}: {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to MongoDB")

# ── Kafka Consumer ────────────────────────────────────────────────────────────
def create_consumer() -> KafkaConsumer:
    for attempt in range(1, 20):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            print(f"[consumer] Kafka consumer connected on attempt {attempt}")
            return consumer
        except NoBrokersAvailable:
            print(f"[consumer] Waiting for Kafka... attempt {attempt}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka")

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("[consumer] Starting up...")
    collection = connect_mongo()
    consumer   = create_consumer()

    print(f"[consumer] Listening on topic '{KAFKA_TOPIC}' → MongoDB '{MONGO_DB}.{MONGO_COLLECTION}'")

    total = 0
    for msg in consumer:
        tx = msg.value
        try:
            collection.update_one(
                {"transaction_id": tx["transaction_id"]},
                {"$set": tx},
                upsert=True,
            )
            total += 1
            print(
                f"[consumer] ✓ Saved | src={tx.get('source','?')} | "
                f"id={tx['transaction_id'][:8]}… | "
                f"amount={tx.get('amount')} {tx.get('currency','?')} | "
                f"status={tx.get('status')} | total={total}"
            )
        except mongo_errors.DuplicateKeyError:
            pass          # idempotent — already saved
        except Exception as e:
            print(f"[consumer] MongoDB write error: {e}")

if __name__ == "__main__":
    main()