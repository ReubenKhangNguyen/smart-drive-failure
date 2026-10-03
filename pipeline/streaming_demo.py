"""Command builders and helpers for scripts/run_streaming_demo.py (host side, standard library only, no pyspark/yaml)."""
from __future__ import annotations

import json
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

from pipeline.streaming_report import parse_docker_stats

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"  # Scala 2.12, Spark 3.5.1 (verified to resolve)
# /home/spark does not exist in the Spark image, so Ivy's default ~/.ivy2 cannot be created (FileNotFoundException).
IVY_DIR = "/tmp/.ivy2"
KAFKA_PY_VERSION = "kafka-python==3.0.11"  # 2.0.2 does not even import on Python 3.14 (requirements-streaming.txt)
COMPOSE_NETWORK = "smart-drive-failure_smart-drive-net"
KAFKA_INTERNAL = "kafka:29092"


def create_topic_command(topic: str) -> List[str]:
    return ["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", KAFKA_INTERNAL,
            "--create", "--if-not-exists", "--topic", topic, "--partitions", "1", "--replication-factor", "1"]


def list_topics_command() -> List[str]:
    return ["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", KAFKA_INTERNAL, "--list"]


def consumer_command(
    run_id: str, topic: str, compare_date: Optional[str], max_offsets_per_trigger: int, idle_seconds: float, timeout_seconds: float
) -> List[str]:
    """spark-submit of pipeline/streaming_consumer.py inside spark-master. No executor/driver size flags: the default
    1g executors are the configuration every other job ran with."""
    command = [
        "docker", "compose", "exec", "-T", "-e", "PYTHONPATH=/opt/smart-drive", "spark-master", "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077", "--conf", "spark.jars.ivy={}".format(IVY_DIR), "--packages", SPARK_KAFKA_PACKAGE,
        "/opt/smart-drive/pipeline/streaming_consumer.py", "--run-id", run_id, "--topic", topic,
        "--max-offsets-per-trigger", str(max_offsets_per_trigger), "--idle-seconds", str(idle_seconds),
        "--timeout-seconds", str(timeout_seconds),
    ]
    if compare_date:
        command += ["--compare-date", compare_date]
    return command


def producer_container_command(
    host_source_dir: str, repo_dir: str, dates: List[str], topic: str, batch_size: int, batch_delay: float,
    max_messages: Optional[int],
) -> List[str]:
    """Fallback when the host Python cannot run kafka-python: a throwaway python:3.11 container on the compose network
    (CSV and repo mounted read-only, nothing is built or installed on the host)."""
    inner = ("pip install -q {pkg} && python /app/ingestion/kafka_producer.py --host-source-dir /data --dates {dates} "
             "--bootstrap-servers {kafka} --topic {topic} --batch-size {bs} --batch-delay {bd}").format(
        pkg=KAFKA_PY_VERSION, dates=" ".join(dates), kafka=KAFKA_INTERNAL, topic=topic, bs=batch_size, bd=batch_delay)
    if max_messages is not None:
        inner += " --max-messages {}".format(max_messages)
    return ["docker", "run", "--rm", "--network", COMPOSE_NETWORK, "-v", "{}:/data:ro".format(host_source_dir),
            "-v", "{}:/app:ro".format(repo_dir), "python:3.11-slim", "sh", "-c", inner]


def parse_summary_line(text: str, prefix: str) -> Optional[Dict[str, Any]]:
    """The JSON after `<prefix> ` on the last line that starts with it (PRODUCER_SUMMARY / CONSUMER_SUMMARY)."""
    found = None  # type: Optional[Dict[str, Any]]
    for line in text.splitlines():
        if line.startswith(prefix + " "):
            found = json.loads(line[len(prefix) + 1:])
    return found


def docker_memory_sample(cwd: Optional[str] = None) -> Dict[str, float]:
    done = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"], cwd=cwd,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True, timeout=60)
    return parse_docker_stats(done.stdout)


class MemorySampler(threading.Thread):
    """Samples `docker stats` every `interval` seconds until stop() is called."""

    def __init__(self, interval: float = 5.0, cwd: Optional[str] = None) -> None:
        super().__init__(daemon=True)
        self.interval = interval
        self.cwd = cwd
        self.samples = []  # type: List[Dict[str, float]]
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                sample = docker_memory_sample(self.cwd)
                if sample:
                    self.samples.append(sample)
            except (OSError, subprocess.TimeoutExpired):
                pass
            self._stop_event.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()
        self.join(timeout=30)
