from __future__ import annotations

from pyspark.sql import functions as F

from features.build_features import add_window_features, build_features, feature_columns, index_categorical
from features.label import assign_split, build_labeled_dataset

COLUMNS = ["date", "serial_number", "model", "manufacturer", "capacity_bytes", "failure", "smart_5_raw"]


def _row(serial, date, smart5, failure=0):
    return (date, serial, "ModelA", "Seagate", 1000, failure, float(smart5))


def _build_series(spark, values):
    rows = [_row("SN001", "2026-01-{:02d}".format(i + 1), v) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))


def test_feature_at_date_t_unchanged_when_future_data_changes(spark):
    baseline_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    df_a = _build_series(spark, baseline_values)

    changed_future_values = list(baseline_values)
    changed_future_values[8] = 999.0  # change day 9 (index 8), which is AFTER day 5
    df_b = _build_series(spark, changed_future_values)

    result_a = add_window_features(df_a, columns=["smart_5_raw"]).where(F.col("date") == "2026-01-05").collect()[0]
    result_b = add_window_features(df_b, columns=["smart_5_raw"]).where(F.col("date") == "2026-01-05").collect()[0]

    for field in result_a.asDict():
        if field in ("date", "serial_number"):
            continue
        assert result_a[field] == result_b[field], "field {} changed when future data changed".format(field)


def test_feature_at_date_t_changes_when_past_data_changes(spark):
    """Sanity check that the test above isn't vacuous: changing PAST data must be visible."""
    baseline_values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    df_a = _build_series(spark, baseline_values)

    changed_past_values = list(baseline_values)
    changed_past_values[1] = 999.0  # change day 2 (index 1), which is BEFORE day 5
    df_b = _build_series(spark, changed_past_values)

    result_a = add_window_features(df_a, columns=["smart_5_raw"]).where(F.col("date") == "2026-01-05").collect()[0]
    result_b = add_window_features(df_b, columns=["smart_5_raw"]).where(F.col("date") == "2026-01-05").collect()[0]

    assert result_a["smart_5_raw_max_7d"] != result_b["smart_5_raw_max_7d"]


def test_no_forbidden_columns_in_feature_list(spark):
    df = _build_series(spark, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    labeled = build_labeled_dataset(df, dataset_end_date="2026-03-01", horizon_days=7)
    train_df = labeled  # small fixture: reuse as its own train split for the indexer
    features_df = build_features(labeled, train_df, columns=["smart_5_raw"])

    columns = feature_columns(features_df)

    assert "serial_number" not in columns
    assert "failure" not in columns
    assert "fail_within_7_days" not in columns
    assert "date" not in columns
    assert "split" not in columns


def test_categorical_indexer_fit_on_train_only_ignores_test_only_category(spark):
    """StringIndexer must be fit on train_df alone: a model name that only appears in
    the test split must not get its own learned index (handleInvalid='keep' maps it
    to the reserved unseen-category bucket instead)."""
    train_rows = [
        ("2026-01-01", "SN001", "ModelA", "Seagate", 1000, 0, 0.0),
        ("2026-01-02", "SN001", "ModelA", "Seagate", 1000, 0, 0.0),
    ]
    test_rows = [
        ("2026-01-03", "SN002", "ModelB_never_in_train", "Seagate", 1000, 0, 0.0),
    ]
    train_df = spark.createDataFrame(train_rows, COLUMNS).withColumn("date", F.to_date("date"))
    test_df = spark.createDataFrame(test_rows, COLUMNS).withColumn("date", F.to_date("date"))
    full_df = train_df.union(test_df)

    result = index_categorical(full_df, train_df, columns=("model",))
    indices = {r["model"]: r["model_index"] for r in result.collect()}

    # The unseen category must land on the reserved "unseen" bucket (numLabels, i.e. 1
    # here since train only had one distinct model), not get its own learned slot.
    assert indices["ModelB_never_in_train"] == 1.0
    assert indices["ModelA"] == 0.0


def test_split_date_ranges_do_not_overlap(spark):
    rows = [
        _row("SN001", "2026-01-{:02d}".format(day) if day <= 31 else "2026-02-{:02d}".format(day - 31), 0)
        for day in range(1, 61)
    ]
    df = spark.createDataFrame(rows, COLUMNS).withColumn("date", F.to_date("date"))
    labeled = build_labeled_dataset(df, dataset_end_date="2026-03-15", horizon_days=7)

    split_df = assign_split(labeled, warmup_end_date="2026-01-05", train_end_date="2026-01-20", val_end_date="2026-01-31")

    max_train = split_df.where(F.col("split") == "train").agg(F.max("date")).collect()[0][0]
    min_val = split_df.where(F.col("split") == "val").agg(F.min("date")).collect()[0][0]
    max_val = split_df.where(F.col("split") == "val").agg(F.max("date")).collect()[0][0]
    min_test = split_df.where(F.col("split") == "test").agg(F.min("date")).collect()[0][0]

    assert max_train < min_val
    assert max_val < min_test
