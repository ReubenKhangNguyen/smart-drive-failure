from __future__ import annotations

import argparse
import subprocess
import threading
import time

from ingestion.kafka_producer import stream_file
from pathlib import Path


def _run_consumer(timeout_seconds: int) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "/opt/smart-drive/pipeline/streaming_consumer.py",
            "--timeout-seconds",
            str(timeout_seconds),
        ],
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Demo Phase 2.b: bat producer + consumer Kafka cung luc trong vai phut (buoc 8 bat buoc theo yeu cau giang vien)"
    )
    parser.add_argument("--host-source-dir", required=True, help="Thu muc chua CSV, vd C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026")
    parser.add_argument("--dates", nargs="+", default=["2026-01-01"], help="1-3 ngay mau de demo, vd 2026-01-01 2026-01-02")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="smart-events")
    parser.add_argument("--delay-seconds", type=float, default=0.2)
    parser.add_argument("--consumer-timeout-seconds", type=int, default=90)
    args = parser.parse_args()

    consumer_thread = threading.Thread(target=_run_consumer, args=(args.consumer_timeout_seconds,))
    consumer_thread.start()
    time.sleep(10)  # give Spark Structured Streaming time to subscribe before producing

    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)
    total = 0
    for date_str in args.dates:
        path = Path(args.host_source_dir) / "{}.csv".format(date_str)
        sent = stream_file(path, lambda msg: producer.send(args.topic, msg), args.delay_seconds)
        print("Sent {} messages from {}".format(sent, path))
        total += sent
    producer.flush()
    print("Total sent:", total)

    consumer_thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
