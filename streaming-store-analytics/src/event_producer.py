"""Generate and publish synthetic store events to Kafka."""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

EVENT_TYPES = ("view", "add_to_cart", "purchase")
REGIONS = ("north", "south", "east", "west")
PRODUCT_IDS = ("P-100", "P-200", "P-300", "P-400", "P-500")


def build_event(rng: random.Random | None = None) -> dict[str, Any]:
    """Create one realistic event, with an optional random source for tests."""
    random_source = rng or random
    event_type = random_source.choice(EVENT_TYPES)
    amount = round(random_source.uniform(5, 250), 2) if event_type == "purchase" else 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        ),
        "user_id": f"U-{random_source.randint(1, 1000):04d}",
        "event_type": event_type,
        "product_id": random_source.choice(PRODUCT_IDS),
        "amount": amount,
        "region": random_source.choice(REGIONS),
    }


def publish_events(
    bootstrap_servers: str,
    topic: str,
    event_count: int,
    interval_seconds: float,
    seed: int | None = None,
) -> None:
    if event_count < 1:
        raise ValueError("event_count must be at least 1")
    if interval_seconds < 0:
        raise ValueError("interval_seconds cannot be negative")

    from kafka import KafkaProducer

    rng = random.Random(seed)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
    )
    try:
        for index in range(event_count):
            event = build_event(rng)
            producer.send(topic, value=event)
            if (index + 1) % 10 == 0 or index + 1 == event_count:
                print(f"Published {index + 1}/{event_count} events", flush=True)
            if interval_seconds:
                time.sleep(interval_seconds)
        producer.flush()
    finally:
        producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=200, help="Number of events to publish")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.2,
        help="Delay between events; use 0 for a fast batch",
    )
    parser.add_argument("--seed", type=int, help="Optional random seed for repeatable event values")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "store-events"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    publish_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        event_count=args.events,
        interval_seconds=args.interval_seconds,
        seed=args.seed,
    )
