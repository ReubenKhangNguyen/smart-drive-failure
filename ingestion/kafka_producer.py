from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Callable, Dict, Iterator


def read_csv_rows(path: Path) -> Iterator[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def row_to_message(row: Dict[str, str]) -> bytes:
    """One Backblaze drive-day row -> JSON message, used as the Kafka producer value."""
    return json.dumps(row).encode("utf-8")


def stream_file(
    path: Path,
    send: Callable[[bytes], None],
    delay_seconds: float = 0.05,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Send every row in `path` as one message via `send`, pausing `delay_seconds`
    between messages to simulate a real daily feed arriving gradually, not a full-file
    dump (this is the streaming DEMO layer — Phase 2's batch Bronze load is the real
    source of truth for analytics/training, per docs/ROADMAP.md Phase 2.b)."""
    count = 0
    for row in read_csv_rows(path):
        send(row_to_message(row))
        count += 1
        sleep(delay_seconds)
    return count


def main() -> int:
    import argparse

    from kafka import KafkaProducer

    parser = argparse.ArgumentParser(description="Phat tung dong CSV Bronze vao Kafka de mo phong luong SMART hang ngay")
    parser.add_argument("--host-source-dir", required=True, help="Thu muc chua CSV, vd C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026")
    parser.add_argument("--dates", nargs="+", required=True, help="Vd 2026-01-01 2026-01-02")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="smart-events")
    parser.add_argument("--delay-seconds", type=float, default=0.05)
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)
    total = 0
    for date_str in args.dates:
        path = Path(args.host_source_dir) / "{}.csv".format(date_str)
        sent = stream_file(
            path,
            lambda msg: producer.send(args.topic, msg),
            args.delay_seconds,
        )
        print("Sent {} messages from {}".format(sent, path))
        total += sent

    producer.flush()
    print("Total sent:", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
