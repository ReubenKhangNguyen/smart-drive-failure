"""Publish a small Backblaze CSV sample as JSON SMART events to Kafka."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer


EVENT_COLUMNS = (
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "smart_5_raw",
    "smart_187_raw",
    "smart_197_raw",
    "smart_198_raw",
    "smart_199_raw",
)
INTEGER_COLUMNS = set(EVENT_COLUMNS) - {"date", "serial_number", "model"}


def normalise(row: dict[str, str]) -> dict[str, object]:
    """Keep a stable, compact event schema and convert numeric SMART fields."""
    event: dict[str, object] = {}
    for column in EVENT_COLUMNS:
        value = row.get(column, "").strip()
        if column in INTEGER_COLUMNS:
            try:
                event[column] = int(value) if value else None
            except ValueError:
                event[column] = None
        else:
            event[column] = value or None
    return event


def run(input_file: Path, bootstrap_servers: str, topic: str, limit: int, delay_seconds: float) -> int:
    """Read at most ``limit`` rows and send one key-partitioned JSON event per row."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
    )
    sent = 0
    with input_file.open("r", encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            event = normalise(row)
            if not event["serial_number"]:
                continue
            producer.send(topic, key=str(event["serial_number"]), value=event)
            sent += 1
            if delay_seconds:
                time.sleep(delay_seconds)
            if sent >= limit:
                break
    producer.flush()
    producer.close()
    print(f"KAFKA_PRODUCER_RESULT topic={topic} sent={sent} input={input_file}", flush=True)
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish Backblaze SMART sample events to Kafka.")
    parser.add_argument("--input", type=Path, default=Path("/data/smoke/2026-01-01.csv"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "smart-drive-events"))
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--delay-seconds", type=float, default=0.01)
    args = parser.parse_args()
    run(args.input, args.bootstrap_servers, args.topic, args.limit, args.delay_seconds)
