"""Phase 2.b producer: replay Bronze CSV rows into Kafka, paced in batches, as the 13-column JSON the streaming
consumer reads. Standard library plus `kafka-python` (imported only in main, so everything else is testable without it).

Why not one message per row at 0.2 s: one day has ~338,760 rows, which would take ~19 hours. Why 13 columns and not all
197: a full-row JSON message is ~4,885 bytes (1.65 GB per day) against ~314 bytes (106 MB) for the columns the consumer
parses. This is the streaming DEMO layer; the batch Bronze load stays the source for analytics and training.
"""
from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

# The columns the streaming consumer parses. Duplicated from processing.spark_jobs.smart_etl.REQUIRED_RAW_COLUMNS
# because the producer runs on the host, where pyspark is not installed; a test keeps the two lists equal.
REQUIRED_COLUMNS = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "smart_5_raw",
    "smart_9_raw",
    "smart_187_raw",
    "smart_188_raw",
    "smart_194_raw",
    "smart_197_raw",
    "smart_198_raw",
    "smart_199_raw",
]


def read_csv_rows(path: Path) -> Iterator[Dict[str, str]]:
    with open(str(path), "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def row_to_message(row: Dict[str, str], columns: Optional[Sequence[str]] = REQUIRED_COLUMNS) -> bytes:
    """One drive-day row -> JSON bytes. `columns=None` keeps every column; otherwise only those, a column the CSV
    lacks (schema drift) becomes null, as the Spark side already tolerates."""
    data = row if columns is None else {c: row.get(c) for c in columns}
    return json.dumps(data).encode("utf-8")


def stream_file(
    path: Path,
    send: Callable[[bytes], None],
    batch_size: int = 2000,
    batch_delay: float = 0.2,
    max_messages: Optional[int] = None,
    columns: Optional[Sequence[str]] = REQUIRED_COLUMNS,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Send the rows of `path` through `send`, sleeping `batch_delay` after every `batch_size` messages so the feed
    arrives gradually (micro-batches, not one burst). Stops after `max_messages` when given. Returns the count."""
    count = 0
    for row in read_csv_rows(path):
        if max_messages is not None and count >= max_messages:
            break
        send(row_to_message(row, columns))
        count += 1
        if batch_size > 0 and count % batch_size == 0:
            sleep(batch_delay)
    return count


def produce_dates(
    send: Callable[[bytes], None],
    source_dir: Path,
    dates: Sequence[str],
    batch_size: int = 2000,
    batch_delay: float = 0.2,
    max_messages: Optional[int] = None,
    columns: Optional[Sequence[str]] = REQUIRED_COLUMNS,
    sleep: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    """Stream one CSV per date (`<date>.csv`). `max_messages` caps the TOTAL across dates."""
    per_date = {}  # type: Dict[str, int]
    total = 0
    for date in dates:
        remaining = None if max_messages is None else max_messages - total
        if remaining is not None and remaining <= 0:
            break
        sent = stream_file(Path(source_dir) / "{}.csv".format(date), send, batch_size, batch_delay, remaining, columns, sleep)
        per_date[date] = sent
        total += sent
    return {"per_date": per_date, "total": total}


class DeliveryCounter:
    """Counts messages handed to the producer and the broker acknowledgements (or errors) that come back."""

    def __init__(self) -> None:
        self.sent = 0
        self.delivered = 0
        self.failed = 0

    def on_success(self, _metadata: Any = None) -> None:
        self.delivered += 1

    def on_error(self, _exc: Any = None) -> None:
        self.failed += 1

    def wrap(self, producer: Any, topic: str) -> Callable[[bytes], None]:
        def send(message: bytes) -> None:
            self.sent += 1
            future = producer.send(topic, message)
            future.add_callback(self.on_success)
            future.add_errback(self.on_error)

        return send


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Phat tung dong CSV Bronze vao Kafka (13 cot, phat theo lo) de mo phong luong SMART")
    parser.add_argument("--host-source-dir", required=True, help="Thu muc chua CSV, vd C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026")
    parser.add_argument("--dates", nargs="+", required=True, help="Vd 2026-01-01")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="smart-events")
    parser.add_argument("--batch-size", type=int, default=2000, help="so message moi lo")
    parser.add_argument("--batch-delay", type=float, default=0.2, help="giay nghi sau moi lo")
    parser.add_argument("--max-messages", type=int, default=None, help="gioi han tong so message (mac dinh: gui het)")
    parser.add_argument("--all-columns", action="store_true", help="gui ca 197 cot (rat nang: ~4.9 KB/message)")
    args = parser.parse_args(argv)

    from kafka import KafkaProducer  # kafka-python==3.0.11 (requirements-streaming.txt); imported here on purpose

    started = time.time()
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, linger_ms=20)
    counter = DeliveryCounter()
    result = produce_dates(
        counter.wrap(producer, args.topic), Path(args.host_source_dir), args.dates, args.batch_size, args.batch_delay,
        args.max_messages, None if args.all_columns else REQUIRED_COLUMNS,
    )
    producer.flush()
    summary = {
        "topic": args.topic, "dates": args.dates, "per_date": result["per_date"], "sent": counter.sent,
        "delivered": counter.delivered, "failed": counter.failed,
        "columns": "all" if args.all_columns else len(REQUIRED_COLUMNS), "batch_size": args.batch_size,
        "batch_delay": args.batch_delay, "seconds": round(time.time() - started, 1),
    }
    print("PRODUCER_SUMMARY " + json.dumps(summary))
    return 0 if counter.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
