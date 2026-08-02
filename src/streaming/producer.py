"""Dummy real-time transaction generator, publishing to a Kafka topic.

Stands in for a production event source (e.g. a CDC stream off the OLTP
database, or an application emitting order events directly). Each message
carries its own event_time so the consumer can perform event-time windowing
rather than trusting Kafka's ingestion time.
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "transactions.raw"
NUM_CUSTOMERS = 1_200


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def generate_transaction() -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST{random.randint(1, NUM_CUSTOMERS):06d}",
        "amount": round(random.uniform(10_000, 5_000_000), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def run(producer: KafkaProducer, min_interval_seconds: float, max_interval_seconds: float) -> None:
    try:
        while True:
            transaction = generate_transaction()
            producer.send(TOPIC, key=transaction["transaction_id"], value=transaction)
            print(f"produced {transaction['transaction_id']} at {transaction['event_time']}")
            time.sleep(random.uniform(min_interval_seconds, max_interval_seconds))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy real-time transaction generator")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--min-interval", type=float, default=0.1, help="seconds between events, lower bound")
    parser.add_argument("--max-interval", type=float, default=1.0, help="seconds between events, upper bound")
    args = parser.parse_args()

    producer = build_producer(args.bootstrap_servers)
    run(producer, args.min_interval, args.max_interval)


if __name__ == "__main__":
    main()
