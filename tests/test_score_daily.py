from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from ml.score_daily import build_daily_scores, scorable_date_ranges

COLUMNS = ["date", "serial_number", "model", "f1", "f2", "fail_within_7_days", "split"]

CFG = {
    "warmup_end_date": "2026-01-30",
    "end_date": "2026-03-31",
    "oot_start_date": "2026-04-01",
    "oot_end_date": "2026-06-30",
}


def _row(date, serial, f1, label=0, split="val"):
    return (date, serial, "ModelA", float(f1), 0.0, label, split)


def _model(spark):
    train = spark.createDataFrame(
        [_row("2026-01-31", "T1", 10, 1, "train"), _row("2026-01-31", "T2", 0, 0, "train"),
         _row("2026-02-01", "T3", 8, 1, "train"), _row("2026-02-01", "T4", 0, 0, "train")],
        COLUMNS,
    )
    stages = [
        VectorAssembler(inputCols=["f1", "f2"], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=30),
    ]
    return Pipeline(stages=stages).fit(train)


def _features(spark):
    rows = []
    for date, split in (("2026-02-10", "train"), ("2026-03-10", "val"), ("2026-03-28", "test")):
        # SN_HI is the riskiest and does fail; SN_MID looks risky but does not; SN_LO is quiet.
        rows += [_row(date, "SN_HI", 9, 1, split), _row(date, "SN_MID", 5, 0, split), _row(date, "SN_LO", 0, 0, split)]
    return spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))


def test_scorable_ranges_drop_the_censored_last_days_of_each_quarter():
    assert scorable_date_ranges(CFG, 7) == [("2026-01-31", "2026-03-24"), ("2026-04-01", "2026-06-23")]
    # without an out-of-time quarter configured only the first range exists
    only_q1 = {k: v for k, v in CFG.items() if not k.startswith("oot")}
    assert scorable_date_ranges(only_q1, 7) == [("2026-01-31", "2026-03-24")]


def test_topk_per_day_is_ranked_and_carries_the_known_outcome(spark):
    topk, days, base = build_daily_scores(
        _model(spark), _features(spark), k=2, model_version="vtest", ranges=[("2026-01-31", "2026-03-24")]
    )
    try:
        rows = topk.collect()
        assert {r["date"].isoformat() for r in rows} == {"2026-02-10", "2026-03-10"}  # 03-28 is outside the range
        by_day = {}
        for r in rows:
            by_day.setdefault(r["date"].isoformat(), []).append(r)
        for day_rows in by_day.values():
            day_rows.sort(key=lambda r: r["risk_rank"])
            assert [r["serial_number"] for r in day_rows] == ["SN_HI", "SN_MID"]  # K=2, riskiest first
            assert [r["risk_rank"] for r in day_rows] == [1, 2]
            assert [r["failed_within_7d"] for r in day_rows] == [1, 0]
            assert {r["model_version"] for r in day_rows} == {"vtest"}
    finally:
        base.unpersist()


def test_day_table_counts_rows_positives_hits_and_split(spark):
    topk, days, base = build_daily_scores(
        _model(spark), _features(spark), k=2, model_version="vtest", ranges=[("2026-01-31", "2026-03-24")]
    )
    try:
        by_day = {r["date"].isoformat(): r for r in days.collect()}
        assert set(by_day) == {"2026-02-10", "2026-03-10"}
        assert by_day["2026-02-10"]["split"] == "train" and by_day["2026-03-10"]["split"] == "val"
        for row in by_day.values():
            assert (row["scored_rows"], row["positives"], row["topk_hits"]) == (3, 1, 1)
    finally:
        base.unpersist()


def test_two_ranges_are_both_scored(spark):
    ranges = [("2026-02-10", "2026-02-10"), ("2026-03-28", "2026-03-28")]
    topk, days, base = build_daily_scores(_model(spark), _features(spark), k=1, model_version="v", ranges=ranges)
    try:
        assert sorted(r["date"].isoformat() for r in days.collect()) == ["2026-02-10", "2026-03-28"]
        assert topk.count() == 2
    finally:
        base.unpersist()


def test_scorable_ranges_has_one_range_per_configured_quarter():
    data = dict(CFG)
    data.pop("oot_start_date"), data.pop("oot_end_date")
    data["quarters"] = [
        {"id": "2026-Q2", "split": "oot", "start_date": "2026-04-01", "end_date": "2026-06-30"},
        {"id": "2026-Q3", "split": "oot2", "start_date": "2026-07-01", "end_date": "2026-09-30"},
    ]

    assert scorable_date_ranges(data, 7) == [("2026-01-31", "2026-03-24"), ("2026-04-01", "2026-06-23"), ("2026-07-01", "2026-09-23")]
