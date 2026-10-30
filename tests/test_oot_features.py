from __future__ import annotations

from pyspark.sql import functions as F

from features.label import assign_split, build_labeled_dataset
from features.oot import build_oot_features

COLUMNS = ["date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure", "smart_5_raw"]
SMART = ["smart_5_raw"]
HORIZON = 7

# Tiny calendar: "Q1" = 2026-01-01..01-20, "Q2" (out-of-time) = 2026-01-21..01-31.
CFG = {
    "end_date": "2026-01-20",
    "warmup_end_date": "2026-01-02",
    "train_end_date": "2026-01-10",
    "val_end_date": "2026-01-14",
    "oot_start_date": "2026-01-21",
    "oot_end_date": "2026-01-31",
}


def _day(n):
    return "2026-01-{:02d}".format(n)


def _fixture(spark, late_smart_5=0.0):
    rows = []
    # Healthy serial, whole calendar, smart_5 rises from day 15 on.
    for d in range(1, 32):
        value = 0.0 if d < 15 else float(d - 14)
        if d == 28:
            value = late_smart_5
        rows.append((_day(d), "SN_OK", "ModelA", "Seagate", 1000, 0, value))
    # Serial of a model that only exists in the out-of-time quarter; fails on day 25.
    for d in range(21, 26):
        rows.append((_day(d), "SN_NEW", "ModelOnlyQ2", "Toshiba", 1000, 1 if d == 25 else 0, 1.0))
    # Second model in Q1 only, so the indexer has more than one label to order.
    for d in range(1, 31):
        rows.append((_day(d), "SN_B", "ModelB", "HGST", 1000, 0, 0.0))
    return spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))


def test_assign_split_marks_oot_and_keeps_test_for_the_old_quarter(spark):
    labeled = build_labeled_dataset(_fixture(spark), CFG["oot_end_date"], HORIZON)
    split_df = assign_split(
        labeled, CFG["warmup_end_date"], CFG["train_end_date"], CFG["val_end_date"], oot_start_date=CFG["oot_start_date"]
    )
    by_date = {r["date"].isoformat(): r["split"] for r in split_df.select("date", "split").distinct().collect()}
    assert by_date["2026-01-10"] == "train"
    assert by_date["2026-01-14"] == "val"
    assert by_date["2026-01-20"] == "test"
    assert by_date["2026-01-21"] == "oot"
    assert by_date["2026-01-24"] == "oot"


def test_assign_split_without_oot_never_returns_oot(spark):
    labeled = build_labeled_dataset(_fixture(spark), CFG["oot_end_date"], HORIZON)
    split_df = assign_split(labeled, CFG["warmup_end_date"], CFG["train_end_date"], CFG["val_end_date"])
    assert split_df.where(F.col("split") == "oot").count() == 0


def test_oot_features_return_only_oot_rows_with_labels(spark):
    out = build_oot_features(_fixture(spark), CFG, HORIZON, columns=SMART)
    rows = out.collect()
    assert rows and {r["split"] for r in rows} == {"oot"}
    assert min(r["date"].isoformat() for r in rows) >= "2026-01-21"

    new = {r["date"].isoformat(): r["fail_within_7_days"] for r in rows if r["serial_number"] == "SN_NEW"}
    assert new["2026-01-21"] == 1  # window (01-21, 01-28] holds the failure on 01-25
    assert "2026-01-26" not in new  # nothing after its own failure

    ok_dates = {r["date"].isoformat() for r in rows if r["serial_number"] == "SN_OK"}
    assert "2026-01-24" in ok_dates and "2026-01-25" not in ok_dates  # last 7 days censored (end 01-31)


def test_oot_features_use_q1_history_across_the_quarter_boundary(spark):
    out = build_oot_features(_fixture(spark), CFG, HORIZON, columns=SMART)
    first = out.where((F.col("serial_number") == "SN_OK") & (F.col("date") == F.lit("2026-01-21").cast("date"))).collect()[0]
    # smart_5 on 01-21 is 7; the 7-day window reaches back into the old quarter (01-15..01-21).
    assert first["smart_5_raw_max_7d"] == 7.0
    # every day 01-15..01-21 rose versus the day before (01-15 vs 01-14 included) -> 7 increases
    assert first["smart_5_raw_increasing_days_7d"] == 7.0


def test_oot_index_is_fit_on_q1_train_only(spark):
    out = build_oot_features(_fixture(spark), CFG, HORIZON, columns=SMART)
    idx = {r["model"]: r["model_index"] for r in out.select("model", "model_index").distinct().collect()}
    # Train rows (01-03..01-10) only know ModelA and ModelB (ModelB has no failures, equal counts -> any order),
    # a model first seen in the out-of-time quarter falls in the 'keep' bucket = number of known labels.
    assert idx["ModelOnlyQ2"] == 2.0
    assert idx["ModelA"] in (0.0, 1.0)


def test_oot_features_of_a_day_do_not_depend_on_later_days(spark):
    day = F.lit("2026-01-22").cast("date")
    a = build_oot_features(_fixture(spark, late_smart_5=0.0), CFG, HORIZON, columns=SMART)
    b = build_oot_features(_fixture(spark, late_smart_5=999.0), CFG, HORIZON, columns=SMART)
    cols = [c for c in a.columns if c != "fail_within_7_days"]
    ra = a.where((F.col("serial_number") == "SN_OK") & (F.col("date") == day)).select(*cols).collect()[0]
    rb = b.where((F.col("serial_number") == "SN_OK") & (F.col("date") == day)).select(*cols).collect()[0]
    assert ra == rb  # changing 01-28 must not change the features of 01-22
