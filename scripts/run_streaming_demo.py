"""Phase 2.b demo, step 8 of the lecturer's pipeline: Bronze CSV rows -> Kafka -> Spark Structured Streaming ->
/smart-drive/streaming_output/<run_id>, with evidence in artifacts/reports/streaming_demo.md.

Runs on the HOST (standard library; the producer also needs kafka-python, see requirements-streaming.txt, in a venv):

    python scripts/run_streaming_demo.py --host-source-dir "C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026" \
        --dates 2026-01-01 --start-kafka --stop-kafka

Nothing is deleted: --stop-kafka only stops the zookeeper and kafka containers (never HDFS or Spark); streaming_output/streaming_checkpoint
for the run stay on HDFS until someone decides to clean them.
"""
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # `python scripts/...` puts scripts/ first, not the repo root

from ingestion.kafka_producer import REQUIRED_COLUMNS, DeliveryCounter, produce_dates  # noqa: E402
from pipeline.cluster_guard import ClusterBusyError, fetch_master_state, wait_until_idle  # noqa: E402
from pipeline.streaming_demo import (  # noqa: E402
    MemorySampler,
    consumer_command,
    create_topic_command,
    docker_memory_sample,
    list_topics_command,
    parse_summary_line,
    producer_container_command,
    start_streaming_command,
    stop_streaming_command,
)
from pipeline.streaming_report import format_report, memory_summary, total_mib  # noqa: E402

REPORT = ROOT / "artifacts" / "reports" / "streaming_demo.md"


def _run(command: List[str], timeout: Optional[float] = None) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(command, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True,
                          encoding="utf-8", errors="replace", timeout=timeout)


def kafka_ready() -> bool:
    try:
        return _run(list_topics_command(), timeout=60).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def start_kafka(wait_seconds: float = 180.0) -> None:
    print("Starting zookeeper and kafka (profile streaming) ...", flush=True)
    done = _run(start_streaming_command())
    if done.returncode != 0:
        raise RuntimeError("docker compose up failed:\n" + done.stdout[-1500:])
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        if kafka_ready():
            return
        time.sleep(5)
    raise RuntimeError("Kafka did not answer within {:.0f} s".format(wait_seconds))


def probe_host_producer(bootstrap: str) -> None:
    """Send ONE message from the host with kafka-python and wait for the broker's acknowledgement."""
    from kafka import KafkaProducer  # noqa: WPS433 - only needed in host mode

    producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=5)
    metadata = producer.send("smart-probe", b'{"probe": true}').get(timeout=20)
    producer.close()
    print("PROBE OK: topic={} partition={} offset={}".format(metadata.topic, metadata.partition, metadata.offset), flush=True)


def run_consumer(args: argparse.Namespace, topic: str, state: Dict[str, Any]) -> None:
    command = consumer_command(args.run_id, topic, args.dates[0] if len(args.dates) == 1 else None,
                               args.max_offsets_per_trigger, args.idle_seconds, args.timeout_seconds)
    process = subprocess.Popen(command, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True,
                               encoding="utf-8", errors="replace", bufsize=1)
    lines = []  # type: List[str]
    for line in process.stdout:  # type: ignore[union-attr]
        lines.append(line.rstrip("\n"))
        if line.startswith("STREAM_STARTED"):
            state["started"].set()
    process.wait()
    state["returncode"] = process.returncode
    state["text"] = "\n".join(lines)
    state["done"].set()


def run_producer(args: argparse.Namespace, topic: str) -> Dict[str, Any]:
    started = time.time()
    if args.producer_mode == "container":
        done = _run(producer_container_command(args.host_source_dir, str(ROOT), args.dates, topic, args.batch_size,
                                               args.batch_delay, args.max_messages))
        summary = parse_summary_line(done.stdout, "PRODUCER_SUMMARY")
        if summary is None:
            raise RuntimeError("container producer printed no summary:\n" + done.stdout[-1500:])
        return summary
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, linger_ms=20)
    counter = DeliveryCounter()
    result = produce_dates(counter.wrap(producer, topic), Path(args.host_source_dir), args.dates, args.batch_size,
                           args.batch_delay, args.max_messages, REQUIRED_COLUMNS)
    producer.flush()
    return {"topic": topic, "dates": args.dates, "per_date": result["per_date"], "sent": counter.sent,
            "delivered": counter.delivered, "failed": counter.failed, "columns": len(REQUIRED_COLUMNS),
            "batch_size": args.batch_size, "batch_delay": args.batch_delay, "seconds": round(time.time() - started, 1)}


def checks_pass(producer: Dict[str, Any], consumer: Dict[str, Any]) -> bool:
    ok = producer["failed"] == 0 and producer["sent"] == producer["delivered"]
    ok = ok and consumer["streamed_input_rows"] == producer["sent"] and consumer["output_rows"] == producer["sent"]
    comparison = consumer.get("silver_comparison")
    return ok and (comparison is None or bool(comparison["match"]))


def main() -> int:
    parser = argparse.ArgumentParser(description="Demo Kafka + Spark Structured Streaming (buoc 8), 1 ngay du lieu, co bao cao bang chung")
    parser.add_argument("--host-source-dir", required=True, help="Thu muc chua CSV, vd C:/dataset_smart_drive_failure/data_Q1_2026/data_Q1_2026")
    parser.add_argument("--dates", nargs="+", default=["2026-01-01"])
    parser.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S"))
    parser.add_argument("--topic", help="mac dinh: smart-events-<run-id> (moi lan chay mot topic moi)")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")  # IPv4 literal: see docker-compose.yml
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--batch-delay", type=float, default=0.2)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50000)
    parser.add_argument("--idle-seconds", type=float, default=30.0)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--producer-mode", choices=["host", "container"], default="host")
    parser.add_argument("--master-http", default="http://localhost:8080")
    parser.add_argument("--start-kafka", action="store_true", help="docker compose --profile streaming up -d zookeeper kafka")
    parser.add_argument("--stop-kafka", action="store_true", help="docker compose --profile streaming stop (khong xoa gi)")
    parser.add_argument("--probe-only", action="store_true", help="chi gui 1 message tu host de kiem tra kafka-python, roi thoat")
    parser.add_argument("--report", default=str(REPORT))
    args = parser.parse_args()
    topic = args.topic or "smart-events-{}".format(args.run_id)

    try:
        wait_until_idle(lambda: fetch_master_state(args.master_http))
    except ClusterBusyError as exc:
        print("ABORT:", exc, file=sys.stderr)
        return 2

    memory = {"docker_limit_mib": 0.0}  # type: Dict[str, Any]
    limit = _run(["docker", "info", "--format", "{{.MemTotal}}"]).stdout.strip()
    memory["docker_limit_mib"] = float(limit) / (1024 * 1024) if limit.isdigit() else 0.0
    memory["before_kafka_mib"] = total_mib(docker_memory_sample(str(ROOT)))

    try:
        if args.start_kafka:
            start_kafka()
        elif not kafka_ready():
            print("ABORT: Kafka is not running; start it with --start-kafka or `docker compose --profile streaming up -d zookeeper kafka`", file=sys.stderr)
            return 3
        time.sleep(10)
        memory["after_kafka_mib"] = total_mib(docker_memory_sample(str(ROOT)))

        if args.probe_only:
            probe_host_producer(args.bootstrap_servers)
            return 0
        if args.producer_mode == "host":
            probe_host_producer(args.bootstrap_servers)  # fail early, before Spark starts
        done = _run(create_topic_command(topic))
        if done.returncode != 0:
            raise RuntimeError("create topic failed:\n" + done.stdout[-800:])
        print("Topic {} created".format(topic), flush=True)

        sampler = MemorySampler(interval=5.0, cwd=str(ROOT))
        sampler.start()
        state = {"started": threading.Event(), "done": threading.Event(), "returncode": None, "text": ""}  # type: Dict[str, Any]
        consumer = threading.Thread(target=run_consumer, args=(args, topic, state), daemon=True)
        consumer.start()
        waited = 0.0
        while not state["started"].is_set() and not state["done"].is_set() and waited < 300:
            time.sleep(1)
            waited += 1
        if not state["started"].is_set():
            sampler.stop()
            raise RuntimeError("the consumer never started streaming (exit {}):\n{}".format(state["returncode"], state["text"][-2000:]))
        print("Consumer is streaming; producing {} ...".format(", ".join(args.dates)), flush=True)
        producer = run_producer(args, topic)
        print("Producer: sent={sent} delivered={delivered} failed={failed} in {seconds}s".format(**producer), flush=True)
        consumer.join()
        sampler.stop()
    finally:
        if args.stop_kafka:
            print("Stopping streaming profile (nothing is removed) ...", flush=True)
            _run(stop_streaming_command())

    consumer_summary = parse_summary_line(state["text"], "CONSUMER_SUMMARY")
    if consumer_summary is None:
        print("ABORT: the consumer printed no summary (exit {}):\n{}".format(state["returncode"], state["text"][-2000:]), file=sys.stderr)
        return 4
    memory.update(memory_summary(sampler.samples))
    report = format_report({"run_id": args.run_id, "producer": producer, "consumer": consumer_summary, "memory": memory})
    Path(args.report).write_text(report, encoding="utf-8")
    print("Report:", args.report)
    ok = checks_pass(producer, consumer_summary)
    print("RESULT:", "ALL CHECKS PASS" if ok else "CHECKS FAILED (see report)")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
