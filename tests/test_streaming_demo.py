from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from pipeline.streaming_demo import (
    IVY_DIR,
    KAFKA_PY_VERSION,
    SPARK_KAFKA_PACKAGE,
    consumer_command,
    create_topic_command,
    parse_summary_line,
    producer_container_command,
    start_streaming_command,
    stop_streaming_command,
)
from pipeline.streaming_report import format_report, memory_summary, parse_docker_stats, total_mib

ROOT = Path(__file__).resolve().parent.parent


def test_consumer_command_fixes_ivy_and_uses_the_matching_kafka_package():
    command = consumer_command("20261003120000", "smart-events-20261003120000", "2026-01-01", 50000, 30.0, 900.0)
    text = " ".join(command)

    assert "--conf spark.jars.ivy={}".format(IVY_DIR) in text and IVY_DIR == "/tmp/.ivy2"  # /home/spark does not exist
    assert "--packages {}".format(SPARK_KAFKA_PACKAGE) in text
    assert SPARK_KAFKA_PACKAGE == "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"  # Scala 2.12, Spark 3.5.1
    assert "-e PYTHONPATH=/opt/smart-drive" in text and "spark://spark-master:7077" in text
    assert "--run-id 20261003120000" in text and "--topic smart-events-20261003120000" in text
    assert "--compare-date 2026-01-01" in text and command[-1] == "2026-01-01"


def test_consumer_command_omits_the_comparison_when_no_single_day_is_given_and_never_resizes_executors():
    command = consumer_command("r", "t", None, 50000, 30.0, 900.0)

    assert "--compare-date" not in command
    for flag in ("--executor-memory", "--driver-memory", "--total-executor-cores", "--executor-cores", "spark.executor.memory"):
        assert flag not in " ".join(command)  # the default 1g executors are the configuration every other job ran with


def test_create_topic_command_is_idempotent_and_single_partition():
    command = create_topic_command("smart-events-x")

    assert "--if-not-exists" in command and command[command.index("--topic") + 1] == "smart-events-x"
    assert command[command.index("--partitions") + 1] == "1" and "kafka:29092" in command


def test_producer_container_command_is_a_throwaway_readonly_container_on_the_compose_network():
    command = producer_container_command("C:/data", "D:/repo", ["2026-01-01"], "t", 2000, 0.2, None)
    text = " ".join(command)

    assert command[:4] == ["docker", "run", "--rm", "--network"] and "smart-drive-failure_smart-drive-net" in text
    assert "C:/data:/data:ro" in text and "D:/repo:/app:ro" in text and "python:3.11-slim" in text
    assert KAFKA_PY_VERSION == "kafka-python==3.0.11" and KAFKA_PY_VERSION in command[-1]
    assert "--bootstrap-servers kafka:29092" in command[-1] and "--max-messages" not in command[-1]
    assert "--max-messages 500" in producer_container_command("C:/data", "D:/repo", ["2026-01-01"], "t", 2000, 0.2, 500)[-1]


def test_parse_summary_line_takes_the_last_matching_line():
    text = 'noise\nCONSUMER_SUMMARY {"a": 1}\nmore noise\nCONSUMER_SUMMARY {"a": 2, "b": [1]}\nPRODUCER_SUMMARY {"x": 9}'

    assert parse_summary_line(text, "CONSUMER_SUMMARY") == {"a": 2, "b": [1]}
    assert parse_summary_line(text, "PRODUCER_SUMMARY") == {"x": 9}
    assert parse_summary_line("nothing here", "CONSUMER_SUMMARY") is None


def test_parse_docker_stats_converts_units_to_mib_and_memory_summary_finds_the_peak():
    sample = parse_docker_stats("smart-drive-kafka 612.5MiB\nsmart-drive-spark-worker1 1.5GiB\nsmall 2048KiB\ngarbage line\n")

    assert sample == {"smart-drive-kafka": 612.5, "smart-drive-spark-worker1": 1536.0, "small": 2.0}
    assert total_mib(sample) == 612.5 + 1536.0 + 2.0
    summary = memory_summary([{"a": 100.0, "b": 50.0}, {"a": 90.0, "b": 400.0}, {"a": 120.0, "b": 10.0}])
    assert summary["samples"] == 3 and summary["peak_total_mib"] == 490.0
    assert list(summary["peak_by_container"].items()) == [("b", 400.0), ("a", 120.0)]
    assert memory_summary([])["peak_total_mib"] is None


def _run(match: bool = True, delivered: int = 338760, output_rows: int = 338760):
    comparison = {"date": "2026-01-01", "match": match,
                  "streamed": {"rows": output_rows, "serials": 338000, "failures": 11},
                  "silver": {"rows": 338760, "serials": 338000, "failures": 11}}
    return {
        "run_id": "20261003120000",
        "producer": {"topic": "t", "dates": ["2026-01-01"], "sent": 338760, "delivered": delivered, "failed": 338760 - delivered,
                     "batch_size": 2000, "batch_delay": 0.2, "seconds": 61.5},
        "consumer": {"topic": "t", "streamed_input_rows": 338760, "output_rows": output_rows, "stopped_because": "idle",
                     "max_offsets_per_trigger": 50000, "seconds": 140.2, "silver_comparison": comparison,
                     "micro_batches": [{"batchId": 1, "numInputRows": 50000, "triggerExecutionMs": 4100, "processedRowsPerSecond": 12195.1}]},
        "memory": {"before_kafka_mib": 2300.0, "after_kafka_mib": 3300.0, "peak_total_mib": 6400.0, "samples": 40,
                   "docker_limit_mib": 7532.0, "peak_by_container": {"smart-drive-spark-worker1": 1873.0}},
    }


def test_report_shows_every_evidence_item_and_passes_when_everything_matches():
    report = format_report(_run())

    assert "338,760 / 338,760 / 0" in report and "KHÔNG KHỚP" not in report and report.count("ĐẠT") >= 5
    assert "streaming_output/20261003120000" in report and "So với Silver 2026-01-01" in report
    assert "| 1 | 50,000 | 4100 | 12,195 |" in report
    assert "6,400 MiB (6.25 GiB)" in report and "7,532 MiB" in report and "2,300 MiB" in report
    assert "batch pipeline (Bronze → Silver → Gold) vẫn là nguồn chính" in report and "1 ngày dữ liệu" in report


def test_report_flags_a_mismatch_instead_of_hiding_it():
    report = format_report(_run(match=False, delivered=338000, output_rows=338000))

    assert report.count("KHÔNG KHỚP") >= 3


def _load_demo_script():
    spec = importlib.util.spec_from_file_location("run_streaming_demo", str(ROOT / "scripts" / "run_streaming_demo.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_checks_pass_requires_acknowledged_complete_and_silver_matching_data():
    demo = _load_demo_script()
    producer = {"sent": 10, "delivered": 10, "failed": 0}
    consumer = {"streamed_input_rows": 10, "output_rows": 10, "silver_comparison": {"match": True}}

    assert demo.checks_pass(producer, consumer) is True
    assert demo.checks_pass({"sent": 10, "delivered": 9, "failed": 1}, consumer) is False
    assert demo.checks_pass(producer, dict(consumer, output_rows=9)) is False
    assert demo.checks_pass(producer, dict(consumer, silver_comparison={"match": False})) is False
    assert demo.checks_pass(producer, {"streamed_input_rows": 10, "output_rows": 10}) is True  # no comparison requested


def test_no_demo_file_hard_codes_the_namenode_uri():
    for path in ("pipeline/streaming_consumer.py", "pipeline/streaming_demo.py", "pipeline/streaming_report.py",
                 "scripts/run_streaming_demo.py", "ingestion/kafka_producer.py", "processing/spark_jobs/schema_profile.py"):
        assert "hdfs://namenode:9000" not in (ROOT / path).read_text(encoding="utf-8"), path


def test_host_side_files_need_neither_pyspark_nor_yaml_nor_the_config_package():
    # the host runs Python 3.14 without pyspark: these files must import cleanly there
    for path in ("scripts/run_streaming_demo.py", "pipeline/streaming_demo.py", "pipeline/streaming_report.py",
                 "ingestion/kafka_producer.py"):
        source = (ROOT / path).read_text(encoding="utf-8")
        imports = re.findall(r"^\s*(?:import|from)\s+([\w.]+)", source, flags=re.MULTILINE)
        assert not [m for m in imports if m.split(".")[0] in ("pyspark", "yaml", "config", "numpy", "pandas")], (path, imports)


def test_stop_command_names_only_zookeeper_and_kafka_so_hdfs_and_spark_keep_running():
    # regression: `docker compose --profile streaming stop` with no service names stopped the WHOLE project
    command = stop_streaming_command()

    assert command == ["docker", "compose", "--profile", "streaming", "stop", "zookeeper", "kafka"]
    assert command[command.index("stop") + 1:] == ["zookeeper", "kafka"]  # a service list must follow `stop`
    assert start_streaming_command()[-2:] == ["zookeeper", "kafka"]


def test_the_demo_script_stops_the_cluster_nowhere_else():
    source = (ROOT / "scripts" / "run_streaming_demo.py").read_text(encoding="utf-8")

    assert '"stop"]' not in source  # no inline `... stop` list; only stop_streaming_command() may stop anything
    assert "stop_streaming_command()" in source and "start_streaming_command()" in source

