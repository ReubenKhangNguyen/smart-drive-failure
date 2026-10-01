from __future__ import annotations

import json

from pipeline.streaming_consumer import parse_kafka_messages


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
