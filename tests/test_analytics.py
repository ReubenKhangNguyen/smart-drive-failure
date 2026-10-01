from __future__ import annotations

from analytics.drive_model_analysis import model_population_summary
from analytics.failure_analysis import afr_by_group, overall_afr
from analytics.smart_analysis import (
    failure_dates,
    healthy_vs_failed_snapshot,
    pre_failure_signal_rate,
)

COLUMNS = ["date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure", "smart_5_raw"]


def _row(date, serial, model, manufacturer, capacity, failure, smart5):
    return (date, serial, model, manufacturer, capacity, failure, float(smart5))


def test_overall_afr(spark):
    rows = [
        _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 0),
        _row("2026-01-02", "SN001", "M1", "A", 1000, 1, 5),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    # 1 failure over 2 drive-days -> afr = 0.5 * 365
    assert overall_afr(df) == 0.5 * 365


def test_afr_by_group(spark):
    rows = [
        _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 0),
        _row("2026-01-02", "SN001", "M1", "A", 1000, 1, 5),
        _row("2026-01-01", "SN002", "M2", "B", 1000, 0, 0),
        _row("2026-01-02", "SN002", "M2", "B", 1000, 0, 0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = {r["model"]: r["afr"] for r in afr_by_group(df, ["model"]).collect()}

    assert result["M1"] == 0.5 * 365
    assert result["M2"] == 0.0


def test_model_population_summary(spark):
    rows = [
        _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 0),
        _row("2026-01-01", "SN002", "M1", "A", 2000, 1, 0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = {r["model"]: r for r in model_population_summary(df).collect()}

    assert result["M1"]["drive_count"] == 2
    assert result["M1"]["failures"] == 1
    assert result["M1"]["avg_capacity_bytes"] == 1500.0


def test_failure_dates(spark):
    rows = [
        _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 0),
        _row("2026-01-02", "SN001", "M1", "A", 1000, 1, 5),
        _row("2026-01-01", "SN002", "M1", "A", 1000, 0, 0),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = {r["serial_number"]: r["failure_date"] for r in failure_dates(df).collect()}
    assert set(result.keys()) == {"SN001"}


def test_pre_failure_signal_rate_detects_signal_before_failure(spark):
    rows = [
        _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 0),   # healthy, 6 days before failure
        _row("2026-01-05", "SN001", "M1", "A", 1000, 0, 3),   # signal appears 2 days before failure
        _row("2026-01-07", "SN001", "M1", "A", 1000, 1, 5),   # failure day itself
        _row("2026-01-01", "SN002", "M1", "A", 1000, 0, 0),   # never fails, no signal
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    rate = pre_failure_signal_rate(df, "smart_5_raw", days_before=7)

    assert rate == 1.0  # the only failed serial (SN001) had a signal in its pre-failure window


def test_healthy_vs_failed_snapshot(spark):
    rows = [
        _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 0),
        _row("2026-01-02", "SN001", "M1", "A", 1000, 1, 10),  # failure day
        _row("2026-01-01", "SN002", "M1", "A", 1000, 0, 0),   # healthy, last day value 0
    ]
    # SN001's pre-failure day (2026-01-01) has smart_5_raw = 0; adjust to show a clear signal
    rows[0] = _row("2026-01-01", "SN001", "M1", "A", 1000, 0, 8)
    df = spark.createDataFrame(rows, COLUMNS)

    result = healthy_vs_failed_snapshot(df, "smart_5_raw")

    assert result["healthy"]["mean"] == 0.0
    assert result["pre_failure"]["mean"] == 8.0
