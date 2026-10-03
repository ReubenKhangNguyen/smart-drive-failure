from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List

from pipeline.streaming_consumer import compare_with_silver, parse_kafka_messages, summarize_progress, wait_until_idle


def test_parse_kafka_messages_reuses_phase3_cleaning(spark):
    row = {
        "date": "2026-01-01",
        "serial_number": "SN001",
        "model": "ST4000DM000",
        "capacity_bytes": "-1",
        "failure": "0",
        "smart_5_raw": "0",
        "smart_9_raw": "100",
        "smart_187_raw": "0",
        "smart_188_raw": "0",
        "smart_194_raw": "30",
        "smart_197_raw": "0",
        "smart_198_raw": "0",
        "smart_199_raw": "0",
    }
    kafka_df = spark.createDataFrame([(json.dumps(row).encode("utf-8"),)], ["value"])

    result = parse_kafka_messages(kafka_df).collect()[0]

    assert result["serial_number"] == "SN001"
    assert result["manufacturer"] == "Seagate"  # same inference as smart_etl.py
    assert result["capacity_bytes"] is None  # negative capacity nulled out, same as batch


def test_parse_kafka_messages_ignores_extra_keys_and_tolerates_missing_ones(spark):
    message = {"date": "2026-01-01", "serial_number": "SN9", "model": "WDC X", "failure": "1", "unexpected": "ignored"}
    kafka_df = spark.createDataFrame([(json.dumps(message).encode("utf-8"),)], ["value"])

    result = parse_kafka_messages(kafka_df).collect()[0]

    assert result["serial_number"] == "SN9" and result["manufacturer"] == "Western Digital"
    assert result["smart_187_raw"] is None  # absent from the message: null, not an error


def _event(batch_id: int, rows: int, ms: int = 1000) -> Dict[str, Any]:
    return {"batchId": batch_id, "numInputRows": rows, "durationMs": {"triggerExecution": ms}, "processedRowsPerSecond": rows / 2.0}


def test_summarize_progress_keeps_only_batches_that_read_data_once_each():
    events = [_event(0, 0), _event(1, 50000), _event(1, 50000), _event(2, 12345), _event(3, 0)]

    summary = summarize_progress(events)

    assert [b["batchId"] for b in summary["batches"]] == [1, 2]
    assert summary["total_input_rows"] == 62345
    assert summary["batches"][0]["triggerExecutionMs"] == 1000 and summary["batches"][0]["processedRowsPerSecond"] == 25000.0


class _Clock:
    def __init__(self):
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_wait_until_idle_returns_after_data_then_a_quiet_period():
    clock = _Clock()

    def progress() -> List[Dict[str, Any]]:
        # a batch of 100 rows arrives at t=2, 4 and 6; nothing after that
        return [_event(i, 100) for i, t in enumerate((2, 4, 6)) if clock.now >= t]

    reason = wait_until_idle(progress, idle_seconds=10, timeout_seconds=300, poll_seconds=1, clock=clock.clock, sleep=clock.sleep)

    assert reason == "idle" and 16 <= clock.now <= 17  # last growth at t=6, plus 10 quiet seconds


def test_wait_until_idle_does_not_stop_while_no_data_has_arrived_yet():
    clock = _Clock()

    reason = wait_until_idle(lambda: [], idle_seconds=5, timeout_seconds=40, poll_seconds=2, clock=clock.clock, sleep=clock.sleep)

    assert reason == "timeout" and clock.now >= 40  # an empty topic is not "idle", only the timeout ends it


def test_wait_until_idle_notices_a_query_that_stopped_by_itself():
    clock = _Clock()

    reason = wait_until_idle(lambda: [], idle_seconds=5, timeout_seconds=60, is_active=lambda: False, clock=clock.clock, sleep=clock.sleep)

    assert reason == "stopped"


def _silver(spark, path: str, rows):
    schema = "date date, serial_number string, failure int"
    spark.createDataFrame([(dt.date.fromisoformat(d), s, f) for d, s, f in rows], schema).write.partitionBy("date").parquet(path)


def test_compare_with_silver_matches_rows_serials_and_failures_of_one_day(spark, tmp_path):
    day = [("2026-01-01", "A", 0), ("2026-01-01", "B", 1), ("2026-01-01", "C", 0)]
    _silver(spark, (tmp_path / "silver").as_uri(), day + [("2026-01-02", "A", 1)])  # another day must not leak in
    streamed = spark.createDataFrame([(d, s, float(f)) for d, s, f in day], "date string, serial_number string, failure double")
    streamed.write.parquet((tmp_path / "streamed").as_uri())

    result = compare_with_silver(spark, (tmp_path / "streamed").as_uri(), (tmp_path / "silver").as_uri(), "2026-01-01")

    assert result["match"] is True
    assert result["streamed"] == result["silver"] == {"rows": 3, "serials": 3, "failures": 1}


def test_compare_with_silver_reports_a_difference(spark, tmp_path):
    _silver(spark, (tmp_path / "silver").as_uri(), [("2026-01-01", "A", 0), ("2026-01-01", "B", 1)])
    spark.createDataFrame([("2026-01-01", "A", 0.0)], "date string, serial_number string, failure double").write.parquet(
        (tmp_path / "streamed").as_uri())

    result = compare_with_silver(spark, (tmp_path / "streamed").as_uri(), (tmp_path / "silver").as_uri(), "2026-01-01")

    assert result["match"] is False and result["streamed"]["rows"] == 1 and result["silver"]["rows"] == 2
