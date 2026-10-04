from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import functions as F

from config.settings import quarters_from_data
from features.oot import build_quarter_features
from pipeline.batch_pipeline import build_quarter_steps

SMART = ["smart_5_raw", "smart_9_raw", "smart_187_raw", "smart_188_raw", "smart_194_raw", "smart_197_raw", "smart_198_raw", "smart_199_raw"]
SILVER_COLUMNS = "date string, serial_number string, model string, manufacturer string, capacity_bytes bigint, failure int, " + ", ".join(
    "{} double".format(c) for c in SMART)

# Tiny calendar: analysis quarter = 01-01..01-20, quarter A = 01-21..01-31 (split oot), quarter B = 02-01..02-10 (oot2)
DATA = {
    "start_date": "2026-01-01", "end_date": "2026-01-20", "warmup_end_date": "2026-01-02",
    "train_end_date": "2026-01-10", "val_end_date": "2026-01-14",
    "quarters": [
        {"id": "2026-Q2", "split": "oot", "start_date": "2026-01-21", "end_date": "2026-01-31"},
        {"id": "2026-Q3", "split": "oot2", "start_date": "2026-02-01", "end_date": "2026-02-10"},
    ],
}
QA, QB = quarters_from_data(DATA)
HORIZON = 7
FIRST, LAST = dt.date(2026, 1, 1), dt.date(2026, 2, 10)


def _day(offset):
    return (FIRST + dt.timedelta(days=offset)).isoformat()


def _silver_rows(last=LAST):
    rows = []

    def add(serial, model, first_offset, last_offset, fail_offset=None, ramp=False):
        for n in range(first_offset, last_offset + 1):
            if (FIRST + dt.timedelta(days=n)) > last:
                break
            smart_5 = float(n) if ramp else 0.0  # rises every single day: every day is an increase
            rows.append((_day(n), serial, model, "Seagate", 1000, 1 if n == fail_offset else 0, smart_5, 0.0, None, None, 30.0, 0.0, 0.0, 0.0))

    add("SN_OK", "ModelA", 0, 40, ramp=True)
    add("SN_B", "ModelB", 0, 36)
    add("SN_FAIL_A", "ModelC", 20, 24, fail_offset=24)  # quarter A: fails on 01-25
    add("SN_FAIL_B", "ModelD", 31, 36, fail_offset=36)  # quarter B: fails on 02-06
    return rows


def _silver(spark, last=LAST):
    return spark.createDataFrame(_silver_rows(last), SILVER_COLUMNS).withColumn("date", F.to_date("date"))


def _rows_of(df):
    return sorted(((r["serial_number"], r["date"].isoformat(), tuple(r)) for r in df.collect()), key=lambda x: x[:2])


def test_quarter_features_return_only_that_quarters_rows_with_its_own_split(spark):
    a = build_quarter_features(_silver(spark), DATA, QA, HORIZON)
    b = build_quarter_features(_silver(spark), DATA, QB, HORIZON)

    rows_a, rows_b = a.collect(), b.collect()
    assert rows_a and rows_b
    assert {r["split"] for r in rows_a} == {"oot"} and {r["split"] for r in rows_b} == {"oot2"}
    assert min(r["date"].isoformat() for r in rows_a) >= "2026-01-21" and max(r["date"].isoformat() for r in rows_a) <= "2026-01-31"
    assert min(r["date"].isoformat() for r in rows_b) >= "2026-02-01" and max(r["date"].isoformat() for r in rows_b) <= "2026-02-10"


def test_a_quarter_does_not_change_when_a_later_quarter_is_added(spark):
    """The silver may hold later quarters, yet quarter A's rows must be identical: its labels and features only use
    data up to its own last day (so already published results cannot move)."""
    only_a = build_quarter_features(_silver(spark, last=dt.date(2026, 1, 31)), DATA, QA, HORIZON)
    with_b = build_quarter_features(_silver(spark), DATA, QA, HORIZON)

    assert only_a.count() > 0
    assert only_a.subtract(with_b).count() == 0 and with_b.subtract(only_a).count() == 0


def test_window_features_of_a_later_quarter_use_the_previous_quarters_history(spark):
    b = build_quarter_features(_silver(spark), DATA, QB, HORIZON)

    first = b.where((F.col("serial_number") == "SN_OK") & (F.col("date") == F.lit("2026-02-01").cast("date"))).collect()[0]
    # smart_5 rose every day, including 01-26..01-31 of the previous quarter: 7 increases in the 7-row window
    assert first["smart_5_raw_increasing_days_7d"] == 7.0


def test_labels_of_a_later_quarter_use_its_own_censoring_end(spark):
    b = build_quarter_features(_silver(spark), DATA, QB, HORIZON)

    fail_b = {r["date"].isoformat(): r["fail_within_7_days"] for r in b.where(F.col("serial_number") == "SN_FAIL_B").collect()}
    assert fail_b["2026-02-01"] == 1  # window (02-01, 02-08] holds the failure on 02-06
    ok_dates = {r["date"].isoformat() for r in b.where(F.col("serial_number") == "SN_OK").collect()}
    assert "2026-02-03" in ok_dates and "2026-02-04" not in ok_dates  # last 7 days (02-04..02-10) are censored


# ---------------------------------------------------------------- the pipeline step and its guards
def _config(base):
    return {
        "hdfs": {"namenode_url": base, "silver": "/silver", "features": "/features"},
        "project": {"horizon_days": HORIZON},
        "data": DATA,
    }


def _prepare(spark, tmp_path, silver_last=LAST):
    base = tmp_path.as_uri()
    _silver(spark, silver_last).write.parquet(base + "/silver")
    a = build_quarter_features(_silver(spark), DATA, QA, HORIZON)
    a.write.mode("overwrite").partitionBy("date").parquet(base + "/features")
    return _config(base), base


def _run(spark, config, dry_run=False, quarter="2026-Q3"):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        _, step = build_quarter_steps(spark, config, quarter, dry_run)[0]
        return step()
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")


def test_step_writes_only_the_new_quarter_and_leaves_earlier_partitions_alone(spark, tmp_path):
    config, base = _prepare(spark, tmp_path)
    before = spark.read.parquet(base + "/features").where(F.col("split") == "oot")
    before_rows = _rows_of(before)

    result = _run(spark, config)

    after = spark.read.parquet(base + "/features")
    assert result["quarter"] == "2026-Q3" and result["split"] == "oot2" and result["rows_out"] > 0
    assert {r["split"] for r in after.select("split").distinct().collect()} == {"oot", "oot2"}
    assert _rows_of(after.where(F.col("split") == "oot")) == before_rows  # quarter A byte-for-byte unchanged


def test_dry_run_checks_and_reports_but_writes_nothing(spark, tmp_path):
    config, base = _prepare(spark, tmp_path)
    before = spark.read.parquet(base + "/features").count()

    plan = _run(spark, config, dry_run=True)

    assert plan["dry_run"] is True and plan["silver_days"] == 10 and plan["existing_feature_days"] == 0
    assert spark.read.parquet(base + "/features").count() == before


def test_step_refuses_a_quarter_that_is_not_fully_in_silver(spark, tmp_path):
    config, _ = _prepare(spark, tmp_path, silver_last=dt.date(2026, 2, 8))

    with pytest.raises(ValueError, match="of the 10 days"):
        _run(spark, config)


def test_step_refuses_to_overwrite_rows_of_another_split(spark, tmp_path):
    config, base = _prepare(spark, tmp_path)
    existing = spark.read.parquet(base + "/features")
    foreign = existing.limit(1).withColumn("date", F.to_date(F.lit("2026-02-05"))).withColumn("split", F.lit("test"))
    foreign.write.mode("append").partitionBy("date").parquet(base + "/features")

    with pytest.raises(ValueError, match="another split"):
        _run(spark, config)


def test_unknown_quarter_is_rejected_before_any_work(spark, tmp_path):
    config, _ = _prepare(spark, tmp_path)

    with pytest.raises(ValueError, match="unknown quarter"):
        build_quarter_steps(spark, config, "2030-Q1")


def test_fingerprint_is_equal_for_the_same_rows_and_changes_when_a_value_changes(spark):
    from features.oot import quarter_fingerprint

    a = build_quarter_features(_silver(spark), DATA, QA, HORIZON)
    again = build_quarter_features(_silver(spark), DATA, QA, HORIZON)
    changed = a.withColumn("smart_5_raw", F.when(F.col("serial_number") == "SN_OK", F.col("smart_5_raw") + 1).otherwise(F.col("smart_5_raw")))

    assert quarter_fingerprint(a) == quarter_fingerprint(again)
    assert quarter_fingerprint(a)["rows"] == quarter_fingerprint(changed)["rows"]
    assert quarter_fingerprint(a)["checksum"] != quarter_fingerprint(changed)["checksum"]
