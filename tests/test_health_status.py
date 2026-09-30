from __future__ import annotations

from pyspark.sql import functions as F

from analytics.health_status import baseline_failure_rate_by_level, classify_health

COLUMNS = [
    "date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure",
    "smart_5_raw", "smart_9_raw", "smart_187_raw", "smart_188_raw",
    "smart_194_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw",
]


def _row(serial, date, smart5=0.0, smart187=0.0, smart197=0.0, smart198=0.0, failure=0):
    return (
        date, serial, "ModelA", "Seagate", 1000, failure,
        float(smart5), 100.0, float(smart187), 0.0,
        30.0, float(smart197), float(smart198), 0.0,
    )


def _build_fixture(spark):
    rows = [
        _row("SN_HEALTHY", "2026-01-01"),
        _row("SN_WATCH", "2026-01-01", smart5=3.0),
        _row("SN_CRITICAL_COMBO", "2026-01-01", smart5=3.0, smart197=2.0),
        _row("SN_CRITICAL_SEVERE", "2026-01-01", smart5=150.0),
    ]
    return spark.createDataFrame(rows, COLUMNS)


def test_classify_health_one_serial_per_level(spark):
    df = _build_fixture(spark)
    result = {r["serial_number"]: r for r in classify_health(df).collect()}

    assert result["SN_HEALTHY"]["health_level"] == "HEALTHY"
    assert result["SN_HEALTHY"]["reasons"] == []

    assert result["SN_WATCH"]["health_level"] == "WATCH"
    assert len(result["SN_WATCH"]["reasons"]) == 1

    assert result["SN_CRITICAL_COMBO"]["health_level"] == "CRITICAL"
    assert len(result["SN_CRITICAL_COMBO"]["reasons"]) == 2

    assert result["SN_CRITICAL_SEVERE"]["health_level"] == "CRITICAL"
    assert "smart_5_raw >= 102" in result["SN_CRITICAL_SEVERE"]["reasons"][0]


def test_classify_health_sets_rules_version(spark):
    df = _build_fixture(spark)
    result = classify_health(df).collect()
    assert all(r["rules_version"] == "rules_v1" for r in result)


def test_baseline_failure_rate_by_level(spark):
    rows = [
        _row("SN001", "2026-01-01"),  # healthy, fails 3 days later
        _row("SN001", "2026-01-04", failure=1),
        _row("SN002", "2026-01-01"),  # healthy, never fails
        _row("SN002", "2026-01-02"),
        _row("SN003", "2026-02-01"),  # pushes max_date far enough that 2026-01-01 isn't censored
    ]
    df = spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))
    health_df = classify_health(df)

    baseline = {r["health_level"]: r for r in baseline_failure_rate_by_level(df, health_df, days=7).collect()}

    # SN001's 2026-01-01 row is HEALTHY and fails within 7 days -> counted
    assert baseline["HEALTHY"]["total_rows"] >= 1
    assert baseline["HEALTHY"]["failed_within_window"] >= 1
