from __future__ import annotations

from processing.spark_jobs.smart_cleaning import (
    duplicate_key_count,
    implausible_value_count,
    missing_dates_per_serial,
    serials_reappearing_after_failure,
)

COLUMNS = ["date", "serial_number", "failure", "smart_5_raw"]


def test_duplicate_key_count(spark):
    rows = [
        ("2026-01-01", "SN001", 0, 0.0),
        ("2026-01-01", "SN001", 0, 0.0),
        ("2026-01-01", "SN002", 0, 0.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    assert duplicate_key_count(df) == 1


def test_serials_reappearing_after_failure(spark):
    rows = [
        ("2026-01-01", "SN001", 0, 0.0),
        ("2026-01-02", "SN001", 1, 5.0),
        ("2026-01-03", "SN001", 0, 5.0),  # reappears after its own failure day
        ("2026-01-01", "SN002", 0, 0.0),
        ("2026-01-02", "SN002", 0, 0.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    assert serials_reappearing_after_failure(df) == 1


def test_implausible_value_count(spark):
    rows = [
        ("2026-01-01", "SN001", 0, -1.0),
        ("2026-01-01", "SN002", 0, 5.0),
        ("2026-01-01", "SN003", 0, None),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    assert implausible_value_count(df, "smart_5_raw") == 1


def test_missing_dates_per_serial(spark):
    rows = [
        ("2026-01-01", "SN001", 0, 0.0),
        ("2026-01-02", "SN001", 0, 0.0),
        ("2026-01-01", "SN002", 0, 0.0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    assert missing_dates_per_serial(df, expected_day_count=2) == 1
