from __future__ import annotations

import random

from pyspark.sql import functions as F

from ml.evaluate import extract_risk_score, macro_average_at_k, pr_auc, recall_precision_at_k_by_day, roc_auc
from ml.train import add_class_weight, train_model

FEATURE_COLS = ["f1", "f2"]
COLUMNS = ["date", "serial_number", "f1", "f2", "fail_within_7_days"]


def _build_fixture(spark, n_per_day=20, n_days=3, seed=42):
    rnd = random.Random(seed)
    rows = []
    for day in range(1, n_days + 1):
        for i in range(n_per_day):
            is_positive = i == 0  # exactly one positive per day, ranked highest by f1
            f1 = 10.0 if is_positive else rnd.uniform(0, 1)
            f2 = rnd.uniform(0, 1)
            rows.append(("2026-01-{:02d}".format(day), "SN{:03d}_{}".format(i, day), f1, f2, 1 if is_positive else 0))
    return spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))


def test_add_class_weight_upweights_minority_class(spark):
    df = _build_fixture(spark)
    weighted = add_class_weight(df)
    rows = {r["fail_within_7_days"]: r["class_weight"] for r in weighted.select("fail_within_7_days", "class_weight").distinct().collect()}

    assert rows[1] > rows[0]  # rare positive class gets a higher weight


def test_train_logistic_regression_and_evaluate(spark):
    df = _build_fixture(spark)
    model = train_model(df, FEATURE_COLS, "logistic_regression")
    predictions = model.transform(df)

    assert 0.0 <= pr_auc(predictions) <= 1.0
    assert 0.0 <= roc_auc(predictions) <= 1.0


def test_train_random_forest_and_evaluate(spark):
    df = _build_fixture(spark)
    model = train_model(df, FEATURE_COLS, "random_forest")
    predictions = model.transform(df)

    assert 0.0 <= pr_auc(predictions) <= 1.0


def test_recall_precision_at_k_perfect_ranking(spark):
    df = _build_fixture(spark)
    model = train_model(df, FEATURE_COLS, "logistic_regression")
    predictions = extract_risk_score(model.transform(df))

    daily = recall_precision_at_k_by_day(predictions, k=1)
    macro = macro_average_at_k(daily)

    # f1=10.0 for the single positive per day makes it trivially rank #1 -> perfect recall@1
    assert macro["recall_at_k"] == 1.0
