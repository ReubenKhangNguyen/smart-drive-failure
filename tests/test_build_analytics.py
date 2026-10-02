from __future__ import annotations

import datetime as dt

import pytest

from analytics.build_analytics import build_all, run

COLUMNS = ["date", "serial_number", "model", "manufacturer", "failure", "smart_5_raw", "smart_197_raw"]
SMART = ["smart_5_raw", "smart_197_raw"]


def _day(n):
    return dt.date(2026, 1, n)


def _silver(spark):
    rows = []
    # F1: model A, fails on day 10; smart_5 turns positive (3) from day 5 on.
    for d in range(1, 11):
        rows.append((_day(d), "F1", "A", "X", 1 if d == 10 else 0, 3 if d >= 5 else 0, 0))
    # H1: model A, healthy, all zeros, observed days 1-10.
    for d in range(1, 11):
        rows.append((_day(d), "H1", "A", "X", 0, 0, 0))
    # H2: model B, healthy, observed days 1-5, smart_5 = 7 on its last day.
    for d in range(1, 6):
        rows.append((_day(d), "H2", "B", "Y", 0, 7 if d == 5 else 0, 0))
    return spark.createDataFrame(rows, COLUMNS)


def _by_key(df, *keys):
    return {tuple(r[k] for k in keys): r.asDict() for r in df.collect()}


def test_afr_is_failures_over_drive_years_not_percent(spark):
    tables = build_all(_silver(spark), SMART)
    by_model = _by_key(tables["afr_by_model"], "model")

    assert tables["afr_by_model"].columns == ["model", "drive_days", "failures", "afr"]
    assert by_model[("A",)]["drive_days"] == 20 and by_model[("A",)]["failures"] == 1
    assert by_model[("A",)]["afr"] == pytest.approx(1 / (20 / 365.0))
    assert by_model[("B",)]["afr"] == 0.0
    assert _by_key(tables["afr_by_manufacturer"], "manufacturer")[("X",)]["drive_days"] == 20


def test_smart_distribution_uses_last_day_of_healthy_and_day_before_failure(spark):
    tables = build_all(_silver(spark), SMART)
    dist = _by_key(tables["smart_distribution"], "smart_attribute", "cohort")

    assert tables["smart_distribution"].columns == ["smart_attribute", "cohort", "n", "mean", "p50", "p90", "p99"]
    healthy = dist[("smart_5_raw", "healthy")]  # H1 last day = 0, H2 last day = 7
    assert healthy["n"] == 2 and healthy["mean"] == pytest.approx(3.5)
    pre_failure = dist[("smart_5_raw", "pre_failure")]  # F1 on day 9 = 3
    assert pre_failure["n"] == 1 and pre_failure["mean"] == 3.0 and pre_failure["p50"] == 3.0
    assert dist[("smart_197_raw", "pre_failure")]["mean"] == 0.0


def test_pre_failure_signal_counts_failed_serials_with_value_above_zero(spark):
    tables = build_all(_silver(spark), SMART)
    signal = _by_key(tables["pre_failure_signal"], "smart_attribute", "window_days")

    assert tables["pre_failure_signal"].columns == [
        "smart_attribute", "window_days", "failed_serials", "signalled_serials", "signal_rate"
    ]
    # failure on day 10: window 7 = days 3..9 includes the positive smart_5 values
    assert signal[("smart_5_raw", 7)]["failed_serials"] == 1
    assert signal[("smart_5_raw", 7)]["signalled_serials"] == 1
    assert signal[("smart_5_raw", 7)]["signal_rate"] == 1.0
    assert signal[("smart_5_raw", 30)]["signalled_serials"] == 1
    assert signal[("smart_197_raw", 7)]["signalled_serials"] == 0
    assert signal[("smart_197_raw", 7)]["signal_rate"] == 0.0


def test_run_writes_four_gold_tables_and_exports_each_for_the_dashboard(spark, tmp_path):
    silver_path = (tmp_path / "silver").as_uri()
    _silver(spark).write.parquet(silver_path)
    analytics_path = (tmp_path / "analytics").as_uri()
    export_dir = tmp_path / "dashboard"

    stats = run(spark, silver_path, analytics_path, SMART, export_dir=export_dir)

    names = {"afr_by_model", "afr_by_manufacturer", "smart_distribution", "pre_failure_signal"}
    assert set(stats["tables"]) == names
    for name in names:
        assert spark.read.parquet("{}/{}".format(analytics_path, name)).count() == stats["tables"][name]
        assert spark.read.parquet((export_dir / (name + ".parquet")).as_uri()).count() == stats["tables"][name]
    assert stats["tables"]["afr_by_model"] == 2
    assert stats["tables"]["smart_distribution"] == len(SMART) * 2  # attributes x cohorts
    assert stats["tables"]["pre_failure_signal"] == len(SMART) * 2  # attributes x windows (7, 30)
