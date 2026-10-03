from __future__ import annotations

from pathlib import Path

import yaml

from config.settings import hdfs_uri, load_config
from processing.spark_jobs.schema_profile import build_parser

ROOT = Path(__file__).resolve().parent.parent


def _services():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))["services"]


def test_kafka_and_zookeeper_only_start_under_the_streaming_profile():
    services = _services()

    for name in ("zookeeper", "kafka"):
        assert services[name]["profiles"] == ["streaming"], name
        assert services[name]["restart"] == "no"
    assert not [n for n, s in services.items() if "profiles" not in s and n in ("zookeeper", "kafka")]


def test_heap_options_use_the_variable_the_images_actually_read():
    services = _services()

    # verified in the cp-* 7.6.1 launch scripts: both read KAFKA_HEAP_OPTS and only default it when empty
    assert services["zookeeper"]["environment"]["KAFKA_HEAP_OPTS"] == "-Xmx256m -Xms128m"
    assert services["kafka"]["environment"]["KAFKA_HEAP_OPTS"] == "-Xmx512m -Xms256m"


def test_the_broker_advertises_the_ipv4_loopback_not_localhost():
    # measured on this Windows host: localhost -> ::1 first, the port is published on 127.0.0.1 only, so ::1 was refused
    advertised = _services()["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"]

    assert "PLAINTEXT_HOST://127.0.0.1:9092" in advertised and "localhost" not in advertised
    assert "PLAINTEXT://kafka:29092" in advertised  # containers keep using the internal listener


def test_host_side_defaults_use_the_ipv4_loopback():
    for path in ("ingestion/kafka_producer.py", "scripts/run_streaming_demo.py"):
        text = (ROOT / path).read_text(encoding="utf-8")
        assert 'default="127.0.0.1:9092"' in text and 'default="localhost:9092"' not in text, path


def test_kafka_port_is_published_on_localhost_only_and_images_are_pinned():
    services = _services()

    assert services["kafka"]["ports"] == ["127.0.0.1:${KAFKA_PORT:-9092}:9092"]
    assert services["zookeeper"]["image"] == "confluentinc/cp-zookeeper:7.6.1"
    assert services["kafka"]["image"] == "confluentinc/cp-kafka:7.6.1"
    assert "docker.sock" not in (ROOT / "docker-compose.yml").read_text(encoding="utf-8")


def test_the_airflow_block_survived_the_merge_with_the_kafka_block():
    services = _services()

    for name in ("airflow-init", "airflow-webserver", "airflow-scheduler"):
        assert services[name]["profiles"] == ["orchestration"], name
    for name in ("namenode", "spark-master", "spark-worker1", "spark-worker2", "ui-dashboard"):
        assert "profiles" not in services[name], name  # the default stack is unchanged


def test_kafka_python_is_pinned_only_where_the_producer_runs():
    assert "kafka" not in (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()  # not in the dashboard image
    streaming = (ROOT / "requirements-streaming.txt").read_text(encoding="utf-8")

    assert "kafka-python==3.0.11" in streaming.splitlines()  # 2.0.2 does not import on Python 3.12+


def test_env_example_and_gitignore_cover_the_kafka_demo():
    assert "KAFKA_PORT=9092" in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    assert ".venv-kafka/" in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()


def test_quick_start_documents_the_demo_honestly():
    text = (ROOT / "docs" / "quick-start.md").read_text(encoding="utf-8")
    section = text[text.index("## 7. Demo streaming Kafka"):text.index("## Dừng hệ thống")]

    assert "stop zookeeper kafka" in section and "không kèm tên sẽ dừng cả HDFS và Spark" in section
    for must in ("spark.jars.ivy=/tmp/.ivy2", "--probe-only", "--producer-mode container", "requirements-streaming.txt",
                 "không tự xóa", "338,760", "không thay batch", "KAFKA_HEAP_OPTS", ".venv-kafka"):
        assert must in section, must


def test_schema_profile_cli_defaults_to_the_bronze_path_from_the_config():
    parser = build_parser()

    assert parser.parse_args([]).dir == hdfs_uri(load_config(), "bronze")
    assert parser.parse_args(["--dir", "file:///tmp/x"]).dir == "file:///tmp/x"
