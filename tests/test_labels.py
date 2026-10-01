from __future__ import annotations

from pyspark.sql import functions as F

from features.label import assign_split, build_labeled_dataset, label_drive_days

COLUMNS = ["date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure", "smart_5_raw"]

DATASET_END = "2026-01-20"
HORIZON = 7


def _row(serial, date, failure=0):
    return (date, serial, "ModelA", "Seagate", 1000, failure, 0.0)


def _build_fixture(spark):
    rows = []
    # Fixture 1: healthy serial observed through the end of the dataset.
    for day in range(1, 21):
        rows.append(_row("SN_HEALTHY", "2026-01-{:02d}".format(day)))
    # Fixture 2: serial that fails on 2026-01-10.
    for day in range(1, 12):
        rows.append(_row("SN_FAILED", "2026-01-{:02d}".format(day), failure=1 if day == 10 else 0))
    # Fixture 3: serial that disappears on 2026-01-03 without ever failing.
    for day in range(1, 4):
        rows.append(_row("SN_GONE", "2026-01-{:02d}".format(day)))

    df = spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))
    return df


def test_failed_serial_labeled_1_within_window_and_0_before(spark):
    df = _build_fixture(spark)
    labeled = label_drive_days(df, DATASET_END, HORIZON)
    rows = {r["date"].isoformat(): r for r in labeled.where(F.col("serial_number") == "SN_FAILED").collect()}

    # 2026-01-05: window (01-05, 01-12] includes failure date 01-10 -> label 1
    assert rows["2026-01-05"]["label_status"] == "LABELED"
    assert rows["2026-01-05"]["fail_within_7_days"] == 1

    # 2026-01-01: window (01-01, 01-08] does not include 01-10 -> label 0
    assert rows["2026-01-01"]["label_status"] == "LABELED"
    assert rows["2026-01-01"]["fail_within_7_days"] == 0

    # 2026-01-10 itself: still labeled (fails within its own window trivially), not post-failure
    assert rows["2026-01-10"]["label_status"] == "LABELED"
    assert rows["2026-01-10"]["fail_within_7_days"] == 1

    # 2026-01-11: after the failure date -> POST_FAILURE, dropped from the labeled set
    assert rows["2026-01-11"]["label_status"] == "POST_FAILURE"


def test_healthy_serial_labeled_0_away_from_censoring(spark):
    df = _build_fixture(spark)
    labeled = label_drive_days(df, DATASET_END, HORIZON)
    rows = {r["date"].isoformat(): r for r in labeled.where(F.col("serial_number") == "SN_HEALTHY").collect()}

    assert rows["2026-01-01"]["label_status"] == "LABELED"
    assert rows["2026-01-01"]["fail_within_7_days"] == 0


def test_rows_near_dataset_end_are_censored(spark):
    df = _build_fixture(spark)
    labeled = label_drive_days(df, DATASET_END, HORIZON)
    rows = {r["date"].isoformat(): r for r in labeled.where(F.col("serial_number") == "SN_HEALTHY").collect()}

    # 2026-01-18: window (01-18, 01-25] extends past dataset end (01-20) -> CENSORED
    assert rows["2026-01-18"]["label_status"] == "CENSORED"


def test_censoring_boundary_is_exact(spark):
    """date == end_date - horizon_days must NOT be censored (its window ends exactly
    on the last observed day); one day later must be censored."""
    df = _build_fixture(spark)
    labeled = label_drive_days(df, DATASET_END, HORIZON)
    rows = {r["date"].isoformat(): r for r in labeled.where(F.col("serial_number") == "SN_HEALTHY").collect()}

    assert rows["2026-01-13"]["label_status"] == "LABELED"  # window (01-13, 01-20] fits exactly
    assert rows["2026-01-14"]["label_status"] == "CENSORED"  # window (01-14, 01-21] overshoots


def test_disappeared_serial_is_flagged_not_labeled_healthy(spark):
    df = _build_fixture(spark)
    labeled = label_drive_days(df, DATASET_END, HORIZON)
    rows = {r["date"].isoformat(): r for r in labeled.where(F.col("serial_number") == "SN_GONE").collect()}

    for date_str in ["2026-01-01", "2026-01-02", "2026-01-03"]:
        assert rows[date_str]["label_status"] == "DISAPPEARED"


def test_build_labeled_dataset_drops_unresolved_rows(spark):
    df = _build_fixture(spark)
    result = build_labeled_dataset(df, DATASET_END, HORIZON)
    statuses = {r["serial_number"] for r in result.collect()}

    assert "SN_GONE" not in statuses  # all its rows are DISAPPEARED
    assert "SN_HEALTHY" in statuses  # some of its rows survive (the non-censored ones)
    assert "SN_FAILED" in statuses


def test_assign_split_drops_warmup_and_orders_chronologically(spark):
    df = _build_fixture(spark)
    labeled = build_labeled_dataset(df, DATASET_END, HORIZON)
    split_df = assign_split(labeled, warmup_end_date="2026-01-02", train_end_date="2026-01-06", val_end_date="2026-01-09")

    dates_present = {r["date"].isoformat() for r in split_df.collect()}
    assert "2026-01-01" not in dates_present  # warmup dropped
    assert "2026-01-02" not in dates_present  # warmup boundary inclusive-dropped

    by_date = {r["date"].isoformat(): r["split"] for r in split_df.select("date", "split").distinct().collect()}
    assert by_date["2026-01-03"] == "train"
    assert by_date["2026-01-06"] == "train"
    assert by_date["2026-01-07"] == "val"
    assert by_date["2026-01-10"] == "test"
