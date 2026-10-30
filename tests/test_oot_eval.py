from __future__ import annotations

from datetime import date

import pytest
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from ml.oot_eval import evaluate_frozen_on_oot

SMART = ["smart_5_raw", "smart_187_raw", "smart_197_raw", "smart_198_raw"]
COLUMNS = ["date", "serial_number", "model"] + SMART + ["fail_within_7_days"]


def _row(day, serial, smart_197, label):
    return ("2026-06-{:02d}".format(day), serial, "ModelA", 0.0, 0.0, float(smart_197), 0.0, label)


def _model(spark):
    train = spark.createDataFrame(
        [
            ("2026-02-01", "T1", "ModelA", 0.0, 0.0, 30.0, 0.0, 1),
            ("2026-02-01", "T2", "ModelA", 0.0, 0.0, 0.0, 0.0, 0),
            ("2026-02-02", "T3", "ModelA", 0.0, 0.0, 25.0, 0.0, 1),
            ("2026-02-02", "T4", "ModelA", 0.0, 0.0, 0.0, 0.0, 0),
        ],
        COLUMNS,
    )
    stages = [
        VectorAssembler(inputCols=SMART, outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="fail_within_7_days", maxIter=30),
    ]
    return Pipeline(stages=stages).fit(train)


def _oot(spark):
    rows = []
    # Normal days 06-20, 06-21: the positive drive carries the top signal -> top-1 hits it.
    for day in (20, 21):
        rows += [_row(day, "SN_HI", 30, 1), _row(day, "SN_LO", 0, 0), _row(day, "SN_MID", 1, 0)]
    # 06-22: the top signal drive is healthy and the real failure looks quiet -> top-1 misses.
    rows += [_row(22, "SN_HI", 30, 0), _row(22, "SN_LO", 0, 1), _row(22, "SN_MID", 1, 0)]
    # Tail 06-23, 06-24: only drives known to fail remain, every row positive.
    # One such drive per day, fewer than K, exactly like the real tail (positives per day < K=100).
    rows += [_row(23, "SN_X", 0, 1), _row(24, "SN_Y", 2, 1)]
    return spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))


@pytest.fixture(scope="module")
def result(spark):
    return evaluate_frozen_on_oot(_model(spark), _oot(spark), k=1, normal_end_date="2026-06-22")


def test_segments_split_at_normal_end_date(result):
    seg = result["segments"]
    assert (seg["normal"]["rows"], seg["tail"]["rows"], seg["full"]["rows"]) == (9, 2, 11)
    assert (seg["normal"]["positives"], seg["tail"]["positives"], seg["full"]["positives"]) == (3, 2, 5)


def test_normal_headline_numbers_for_model_and_rules(result):
    normal = result["segments"]["normal"]
    # days 20, 21 hit, day 22 misses -> macro recall 2/3 for both the model and the rules baseline
    assert normal["logistic_regression"]["recall_at_k"] == pytest.approx(2 / 3)
    assert normal["baseline_rules_v1"]["recall_at_k"] == pytest.approx(2 / 3)
    assert normal["logistic_regression"]["precision_at_k"] == pytest.approx(2 / 3)
    assert normal["logistic_regression"]["pr_auc"] is not None


def test_tail_is_trivially_perfect_and_auc_is_undefined(result):
    tail = result["segments"]["tail"]
    assert tail["positive_rate"] == 1.0
    assert tail["logistic_regression"]["recall_at_k"] == 1.0
    assert tail["baseline_rules_v1"]["precision_at_k"] == 1.0
    assert tail["logistic_regression"]["pr_auc"] is None and tail["logistic_regression"]["roc_auc"] is None


def test_monthly_breakdown_covers_the_normal_segment_only(result):
    months = result["monthly_normal"]
    assert [m["month"] for m in months] == ["2026-06"]
    assert months[0]["days"] == 3 and months[0]["positives"] == 3
    assert months[0]["logistic_regression"]["recall_at_k"] == pytest.approx(2 / 3)
    assert date(2026, 6, 23).strftime("%Y-%m") == months[0]["month"]  # tail days never enter the monthly numbers
